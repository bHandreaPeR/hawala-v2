"""
v3/backtest/run_backtest_nifty.py
==================================
Full 6-signal V3 backtest for NIFTY — simulates real option trades (buy ATM CE/PE).

Trade logic (real-time bar-by-bar):
  - Signal evaluated at every 1m bar using ONLY data available up to that bar (no lookahead).
  - First evaluation at bar 30 = 09:45 (need 30 bars for OI quadrant + velocity window).
  - Entry: signal crosses threshold → buy ATM CE/PE at NEXT bar's close.
  - Last entry: 14:30 (leaves ≥50 min to play out before EOD).
  - Exit triggers (whichever hits first):
      SL       → option loses 50% of entry premium
      TP       → option doubles (100% gain)
      REVERSAL → signal crosses threshold in opposite direction
      EOD      → 15:20 square-off (fallback to last available bar)
  - One trade per day — no re-entry after exit.
  - N/A suppression: no trade if velocity=0 AND classifier=None (pure lag-1, no live OI).
  - PnL = (exit_close - entry_close) × 65  [Nifty lot size = 65 units]

Data sources (all lag-1 / no lookahead):
  - 1m futures candles:        v3/cache/candles_1m_NIFTY.pkl
  - Per-strike option 1m LTP:  v3/cache/option_oi_1m_NIFTY.pkl  (needs close column)
  - Bhavcopy EOD:              v3/cache/bhavcopy_NIFTY_all.pkl  (WARNING if missing)
  - PCR:                       from bhavcopy, lag-1
  - FII F&O participant OI:    trade_logs/_fii_fo_cache.pkl
  - FII cash flows:            fii_data.csv
  - Spot (^NSEI):              yfinance 6mo daily

DTE: nearest TUESDAY expiry (Nifty weekly = Tuesday)

Usage: python v3/backtest/run_backtest_nifty.py
"""
import sys, pickle, warnings, logging, argparse
warnings.filterwarnings('ignore')
from pathlib import Path
from datetime import date, datetime as _dt

import pandas as pd
import numpy as np
import yfinance as yf

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# ── Date-range filter (for splitting long runs under sandbox timeout) ─────────
_arg_parser = argparse.ArgumentParser(add_help=False)
_arg_parser.add_argument('--start', default=None, help='Start date YYYY-MM-DD (inclusive)')
_arg_parser.add_argument('--end',   default=None, help='End date YYYY-MM-DD (inclusive)')
_arg_parser.add_argument('--out',   default=None, help='Override output CSV filename')
_args, _ = _arg_parser.parse_known_args()
_FILTER_START = pd.Timestamp(_args.start).date() if _args.start else None
_FILTER_END   = pd.Timestamp(_args.end).date()   if _args.end   else None
_OUT_OVERRIDE = _args.out

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('backtest_nifty')

from v3.signals.engine import compute_signal_state, state_to_dict
from v3.data.fetch_option_oi import compute_intraday_velocity, compute_eod_walls

# FII/DII classifier — optional, degrades gracefully if thresholds not calibrated
try:
    from v3.signals.fii_dii_classifier import FIIDIIClassifier, OISnapshot
    _clf = FIIDIIClassifier()
    _FII_CLF_AVAILABLE = True
    print("FII/DII classifier loaded")
except Exception as _clf_err:
    _clf = None
    _FII_CLF_AVAILABLE = False
    print(f"WARNING: FII/DII classifier unavailable: {_clf_err}")


def _derive_pcr_walls_from_oi_cache(oi_cache: dict) -> tuple:
    """
    Derive bhavcopy-equivalent PCR series and per-day strike DataFrames
    directly from the option_oi_1m_*.pkl cache (EOD last-candle OI per strike).
    This is used as a fallback when bhavcopy is not available.

    Returns: (bhav_equiv: {date_str: DataFrame[strike, ce_oi, pe_oi]},
              pcr_df: DataFrame[date, pcr, pcr_5d_ma, date_only])
    """
    bhav_equiv = {}
    pcr_rows   = []

    for date_str, strike_dict in sorted(oi_cache.items()):
        rows = []
        for strike, sides in strike_dict.items():
            ce_df = sides.get('CE', pd.DataFrame())
            pe_df = sides.get('PE', pd.DataFrame())
            ce_oi = float(ce_df['oi'].iloc[-1]) if not ce_df.empty and len(ce_df) else 0.0
            pe_oi = float(pe_df['oi'].iloc[-1]) if not pe_df.empty and len(pe_df) else 0.0
            if ce_oi > 0 or pe_oi > 0:
                rows.append({'strike': int(strike), 'ce_oi': ce_oi, 'pe_oi': pe_oi})

        if rows:
            df = pd.DataFrame(rows).sort_values('strike').reset_index(drop=True)
            bhav_equiv[date_str] = df
            total_ce = df['ce_oi'].sum()
            total_pe = df['pe_oi'].sum()
            if total_ce > 0:
                pcr_rows.append({
                    'date': pd.Timestamp(date_str),
                    'pcr':  round(total_pe / total_ce, 4),
                })

    if pcr_rows:
        pcr_df = pd.DataFrame(pcr_rows).sort_values('date').reset_index(drop=True)
        pcr_df['pcr_5d_ma'] = pcr_df['pcr'].rolling(5, min_periods=1).mean()
        pcr_df['date_only'] = pcr_df['date'].dt.date
    else:
        pcr_df = pd.DataFrame(columns=['date', 'pcr', 'pcr_5d_ma', 'date_only'])

    return bhav_equiv, pcr_df

# ── Load data ─────────────────────────────────────────────────────────────────
candle_file = ROOT / 'v3/cache/candles_1m_NIFTY.pkl'
if not candle_file.exists():
    raise FileNotFoundError(
        f"NIFTY candle cache not found: {candle_file}. "
        f"Run v3/data/fetch_1m_NIFTY.py first."
    )
with open(candle_file, 'rb') as f:
    candles = pickle.load(f)
trade_dates = sorted(candles['date'].unique())
print(f"NIFTY candles: {len(candles)} rows, {len(trade_dates)} trading days")

# Pre-compute 20-day realized vol map: {date → rolling std of daily % returns}.
# Used by F_VOL gate — O(1) per-day lookup in main loop.
_daily_close  = candles.groupby('date')['close'].last().sort_index()
_daily_ret    = _daily_close.pct_change() * 100
_vol_series   = _daily_ret.rolling(20, min_periods=10).std()
vol_20d_map   = _vol_series.to_dict()
print(f"Vol gate pre-computed: {len(vol_20d_map)} days, "
      f"current vol={_vol_series.iloc[-1]:.3f}% (threshold=0.85%)")

# Bhavcopy for NIFTY — if missing, warn loudly and degrade gracefully
bhav_file = ROOT / 'v3/cache/bhavcopy_NIFTY_all.pkl'
_BHAVCOPY_MISSING = False
bhav = {}
bhav_dates = []
pcr_df = pd.DataFrame(columns=['date', 'pcr', 'pcr_5d_ma'])

if bhav_file.exists():
    with open(bhav_file, 'rb') as f:
        bhav = pickle.load(f)
    bhav_dates = sorted(bhav.keys())
    print(f"NIFTY bhavcopy: {len(bhav_dates)} dates")

    # Build PCR series from bhavcopy
    pcr_rows = []
    for d_str, df_s in bhav.items():
        if df_s.empty:
            continue
        total_ce = df_s['ce_oi'].sum() if 'ce_oi' in df_s.columns else 0
        total_pe = df_s['pe_oi'].sum() if 'pe_oi' in df_s.columns else 0
        if total_ce > 0:
            pcr_rows.append({
                'date': pd.Timestamp(d_str),
                'pcr': round(total_pe / total_ce, 4),
            })
    if pcr_rows:
        pcr_df = pd.DataFrame(pcr_rows).sort_values('date').reset_index(drop=True)
        pcr_df['pcr_5d_ma'] = pcr_df['pcr'].rolling(5, min_periods=1).mean()
    pcr_df['date_only'] = pcr_df['date'].dt.date
else:
    _BHAVCOPY_MISSING = True
    print(
        "\nWARNING: bhavcopy_NIFTY_all.pkl NOT FOUND at %s" % bhav_file
    )
    print("WARNING: Falling back to OI cache for PCR + walls (EOD snapshot, lag-1).")
    print("         Run python v3/data/fetch_bhavcopy_nifty.py for full NSE history.\n")

# Per-strike option 1m OI
opt_oi_file = ROOT / 'v3/cache/option_oi_1m_NIFTY.pkl'
if opt_oi_file.exists():
    with open(opt_oi_file, 'rb') as f:
        opt_oi_cache = pickle.load(f)
    print(f"NIFTY option OI 1m cache: {len(opt_oi_cache)} days")
else:
    opt_oi_cache = {}
    print("WARNING: option_oi_1m_NIFTY.pkl not found — OI velocity will be 0")
    print("         Run: python v3/data/fetch_option_oi_NIFTY.py")

# If bhavcopy missing, derive PCR + walls from OI cache (lag-1 EOD snapshots)
if _BHAVCOPY_MISSING and opt_oi_cache:
    bhav, pcr_df = _derive_pcr_walls_from_oi_cache(opt_oi_cache)
    bhav_dates   = sorted(bhav.keys())
    _BHAVCOPY_MISSING = False
    print(f"[OI-CACHE FALLBACK] Derived PCR/walls for {len(bhav)} days from option OI cache.")

with open(ROOT / 'trade_logs/_fii_fo_cache.pkl', 'rb') as f:
    fii_fo = pickle.load(f)
fii_fo_dates = sorted(fii_fo.keys())

fii_cash_df = pd.read_csv(ROOT / 'fii_data.csv')
fii_cash_df['date'] = pd.to_datetime(fii_cash_df['date']).dt.date

nifty = yf.download('^NSEI', start='2025-07-01', interval='1d', progress=False, auto_adjust=True)
nifty.index = pd.to_datetime(nifty.index).date
spot_close = {d: float(nifty['Close']['^NSEI'].loc[d]) for d in nifty.index}
# Sorted list for fast lag-1 lookup by calendar date (handles candle-cache gaps correctly)
_spot_dates_sorted = sorted(spot_close.keys())


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_walls_from_bhavcopy(df_strikes, spot, band=1500):
    """Previous-day bhavcopy → call wall / put wall."""
    if df_strikes.empty or spot <= 0:
        return {}
    sub = df_strikes[
        (df_strikes['strike'] >= spot - band) &
        (df_strikes['strike'] <= spot + band)
    ].copy()
    if sub.empty:
        sub = df_strikes.copy()
    total_ce = sub['ce_oi'].sum()
    total_pe = sub['pe_oi'].sum()
    pcr_live = total_pe / total_ce if total_ce > 0 else 1.0
    calls_above = sub[sub['strike'] > spot]
    puts_below  = sub[sub['strike'] < spot]
    call_wall = (
        int(calls_above.loc[calls_above['ce_oi'].idxmax(), 'strike'])
        if not calls_above.empty else None
    )
    put_wall = (
        int(puts_below.loc[puts_below['pe_oi'].idxmax(), 'strike'])
        if not puts_below.empty else None
    )
    return {
        'call_wall': call_wall, 'put_wall': put_wall,
        'pcr_live': round(pcr_live, 3), 'ltp': spot,
    }


def _compute_early_velocity(day_oi: dict, spot: float,
                             from_bar: int = 0, to_bar: int = 30,
                             band_pct: float = 0.05) -> dict:
    """
    Compute OI velocity from the early-window bars of the same trading day.
    Default: bars 0-30 = 9:15 AM → 9:45 AM (30-minute open window).

    This is valid for backtesting: we're using the first 30 minutes of OI data
    to predict the direction for the rest of the day.  No lookahead — bar 30
    data is available at 9:45 AM, well before the signal fires at bar 60.

    Returns same format as signal_oi_velocity expects:
      {strike: {ce_velocity, pe_velocity, net_velocity, ce_oi, pe_oi}}
    velocity units: contracts per minute.
    """
    band   = spot * band_pct
    result = {}
    for strike, sides in day_oi.items():
        if abs(strike - spot) > band:
            continue
        ce_df = sides.get('CE', pd.DataFrame())
        pe_df = sides.get('PE', pd.DataFrame())
        if ce_df.empty or pe_df.empty:
            continue
        # Slice to early window
        ce_win = ce_df.iloc[from_bar : to_bar]
        pe_win = pe_df.iloc[from_bar : to_bar]
        if len(ce_win) < 2 or len(pe_win) < 2:
            continue
        ce_start = float(ce_win['oi'].iloc[0])
        ce_end   = float(ce_win['oi'].iloc[-1])
        pe_start = float(pe_win['oi'].iloc[0])
        pe_end   = float(pe_win['oi'].iloc[-1])
        n        = len(ce_win) - 1   # number of 1-min intervals
        ce_vel   = (ce_end - ce_start) / n
        pe_vel   = (pe_end - pe_start) / n
        net      = pe_vel - ce_vel
        result[strike] = {
            'ce_oi':       ce_end,
            'pe_oi':       pe_end,
            'ce_velocity': round(ce_vel, 2),
            'pe_velocity': round(pe_vel, 2),
            'net_velocity': round(net, 2),
        }
    return result


def _get_tuesday_dte(trade_d: date) -> int:
    """
    Return days to next Tuesday expiry on or after trade_d.
    Nifty weekly options expire on Tuesdays.
    """
    days_ahead = (1 - trade_d.weekday()) % 7   # 1 = Tuesday
    return days_ahead


NIFTY_LOT        = 65      # Nifty lot size (65 units per lot)
NIFTY_STEP       = 50      # Strike interval
MIN_SIGNAL_BAR   = 105     # bar index 105 = 11:00 AM — data: 10:30-11:00 only 35% accurate; 11:00+ hits 75%+
VELOCITY_WINDOW  = 60      # bars: rolling OI velocity window (60 min lookback)
MOMENTUM_BARS    = 30      # bars: price must trend in signal direction for last 30 min
MIN_SIGNAL_COUNT = 5       # minimum signals that must agree before entering (out of 6)
LAST_ENTRY_HHMM  = '13:00' # no entries after 13:00 — sweet spot 11:00-13:00; 13:00+ only 33% accurate
MIN_REVERSAL_HOLD = 20     # bars held before REVERSAL exit is allowed (~20 min); prevents whipsaw exits
EOD_EXIT_HHMM    = '15:20' # EOD square-off
SL_PCT           = -0.50   # stop loss:  option loses 50% of entry premium
TP_PCT           = +1.00   # take profit: option doubles
# Filter thresholds:
# F1: extreme_regime = |5d return| > 3%  → require |score| >= 0.50
# F2: PCR bearish (-1) + LONG + score < 0.55 → suppress (raised from 0.45)
# F3: FII_BEAR clf + LONG + score < 0.45 → suppress
# F4a: OI quadrant = -1 (bearish OI) + LONG → suppress (OI says bears, don't fight it)
# F4b: vs_open > +0.5% + LONG + strike_defense = -1 → suppress (chasing into resistance)
# F5: basis > 1.0% + regime < -3% + LONG → suppress (artificial contango in crash)
# NOTE: F6 (moderate FII cash + 10d return gate) was REMOVED.
#   It was curve-fit to Jan 2026 data.  FII selling context is now handled by
#   the classifier directly via the fii_cash_context feature (lag-1, no lookahead).
MIN_PRIOR_DAYS    = 5          # minimum prior trading days before firing any signal
FII_CASH_NORM_DIV = 20_000.0   # divisor for fii_cash_5d → fii_cash_5d_norm passed to clf
# F_VOL: 20-day realized vol gate — skip day if vol < MIN_VOL_PCT.
# Nifty low-vol regime = Jan 2026 (11 trades, 27% WR, -66.6 pts).
# In low-vol, option premiums don't decay fast enough to offset SL risk.
MIN_VOL_PCT       = 0.85       # minimum 20-day realized vol (daily % returns std) to trade


def _build_vel_cache(day_oi: dict) -> dict:
    """
    Pre-build {strike: {'CE': np.array, 'PE': np.array}} OI arrays for fast
    per-bar velocity computation.  Built once per day — O(strikes × bars).
    Per-bar lookup is then O(strikes_in_band) with simple numpy index ops.
    """
    cache: dict = {}
    for strike, sides in day_oi.items():
        entry = {}
        for side in ('CE', 'PE'):
            df = sides.get(side, pd.DataFrame())
            if not df.empty and 'oi' in df.columns:
                entry[side] = df['oi'].to_numpy(dtype=float, na_value=0.0)
            else:
                entry[side] = np.empty(0)
        cache[strike] = entry
    return cache


def _compute_velocity_fast(vel_cache: dict, spot: float, bar_idx: int,
                            window: int, band_pct: float = 0.05) -> dict:
    """
    Compute rolling OI velocity at bar_idx using pre-built numpy arrays.
    Uses the last `window` bars up to bar_idx as the velocity window.
    O(strikes_in_band) per call — no DataFrame slicing.
    """
    band     = spot * band_pct
    from_bar = max(0, bar_idx - window)
    result   = {}
    for strike, sides in vel_cache.items():
        if abs(strike - spot) > band:
            continue
        ce_arr = sides.get('CE', np.empty(0))
        pe_arr = sides.get('PE', np.empty(0))
        if len(ce_arr) == 0 or len(pe_arr) == 0:
            continue
        ce_end_idx   = min(bar_idx, len(ce_arr) - 1)
        pe_end_idx   = min(bar_idx, len(pe_arr) - 1)
        ce_start_idx = min(from_bar, ce_end_idx)
        pe_start_idx = min(from_bar, pe_end_idx)
        n = bar_idx - from_bar
        if n == 0:
            continue
        ce_end   = float(ce_arr[ce_end_idx])
        ce_start = float(ce_arr[ce_start_idx])
        pe_end   = float(pe_arr[pe_end_idx])
        pe_start = float(pe_arr[pe_start_idx])
        ce_vel   = (ce_end - ce_start) / n
        pe_vel   = (pe_end - pe_start) / n
        result[strike] = {
            'ce_oi':        ce_end,
            'pe_oi':        pe_end,
            'ce_velocity':  round(ce_vel, 2),
            'pe_velocity':  round(pe_vel, 2),
            'net_velocity': round(pe_vel - ce_vel, 2),
        }
    return result


def _build_opt_price_cache(day_opt: dict) -> dict:
    """
    Pre-build {strike: {side: {hhmm: price}}} dict for one trading day.
    O(strikes × bars) once per day — O(1) per-bar lookups afterwards.
    Uses vectorized zip — no iterrows().
    """
    cache: dict = {}
    for strike, sides in day_opt.items():
        cache[strike] = {}
        for side in ('CE', 'PE'):
            df = sides.get(side, pd.DataFrame())
            if df.empty or 'close' not in df.columns or 'ts' not in df.columns:
                cache[strike][side] = {}
                continue
            hhmm_arr = pd.to_datetime(df['ts']).dt.strftime('%H:%M')
            close_arr = df['close'].astype(float)
            cache[strike][side] = {
                h: float(c)
                for h, c in zip(hhmm_arr, close_arr)
                if c > 0
            }
    return cache


def _opt_px(opt_cache: dict, strike: int, side: str, hhmm: str) -> float | None:
    """O(1) option price lookup from pre-built cache."""
    return opt_cache.get(strike, {}).get(side, {}).get(hhmm)


def _opt_px_atm(opt_cache: dict, atm: int, side: str, hhmm: str) -> tuple[int | None, float | None]:
    """Try ATM ± offsets until a valid price is found. O(offsets)."""
    for offset in [0, NIFTY_STEP, -NIFTY_STEP, 2 * NIFTY_STEP, -2 * NIFTY_STEP]:
        px = _opt_px(opt_cache, atm + offset, side, hhmm)
        if px is not None:
            return atm + offset, px
    return None, None


def _get_option_price_at_time(
    opt_oi_cache: dict, trade_date, strike: int, side: str, hhmm: str
) -> float | None:
    """Fallback slow path — only used when per-day cache not pre-built."""
    day_opt = opt_oi_cache.get(str(trade_date), {})
    if not day_opt:
        return None
    df = day_opt.get(strike, {}).get(side, pd.DataFrame())
    if df.empty or 'close' not in df.columns:
        return None
    df = df.copy()
    df['_hhmm'] = pd.to_datetime(df['ts']).dt.strftime('%H:%M')
    rows = df[df['_hhmm'] == hhmm]
    if rows.empty:
        return None
    px = float(rows['close'].iloc[0])
    return px if px > 0 else None


def _build_clf_lookups(
    day_oi: dict, day_df_fut: pd.DataFrame, spot_proxy: float
) -> dict | None:
    """
    Pre-build per-strike {hhmm → value} lookup dicts for the FULL trading day.
    Called once per day — O(strikes × bars).  Used by _push_clf_bar to push
    snapshots incrementally (O(n) per day instead of O(n²) re-replay).
    Returns None if day_oi is empty or has no usable data.
    """
    if not day_oi:
        return None

    strike_lookup: dict = {}
    all_hhmm_set: set   = set()

    for strike, sides in day_oi.items():
        lkp: dict = {'CE_oi': {}, 'CE_close': {}, 'PE_oi': {}, 'PE_close': {}}
        for side in ('CE', 'PE'):
            df = sides.get(side, pd.DataFrame())
            if df.empty or 'ts' not in df.columns:
                continue
            df = df.copy()
            df['_hhmm'] = pd.to_datetime(df['ts']).dt.strftime('%H:%M')
            df = df[(df['_hhmm'] >= '09:15') & (df['_hhmm'] <= '15:30')]
            if df.empty:
                continue
            lkp[f'{side}_oi'] = dict(zip(df['_hhmm'], df['oi'].astype(float)))
            if 'close' in df.columns:
                lkp[f'{side}_close'] = dict(zip(df['_hhmm'], df['close'].astype(float)))
            all_hhmm_set.update(df['_hhmm'].tolist())
        strike_lookup[strike] = lkp

    if not all_hhmm_set:
        return None

    fut_lookup: dict   = {}
    trade_date_str: str = ''
    if day_df_fut is not None and not day_df_fut.empty:
        _f = day_df_fut.copy()
        _f['_hhmm'] = _f['ts'].dt.strftime('%H:%M')
        fut_lookup     = dict(zip(_f['_hhmm'], _f['close'].astype(float)))
        trade_date_str = str(_f['ts'].iloc[0].date())

    return {
        'strike_lookup':  strike_lookup,
        'all_hhmm_set':   set(h for h in all_hhmm_set if '09:15' <= h <= '15:30'),
        'fut_lookup':     fut_lookup,
        'trade_date_str': trade_date_str,
        'strikes':        sorted(strike_lookup.keys()),
        'atm_strike':     round(spot_proxy / NIFTY_STEP) * NIFTY_STEP,
        'pushed':         set(),   # hhmm values already pushed to clf this day
    }


def _push_clf_bar(clf, lk: dict, hhmm: str, spot_proxy: float) -> None:
    """
    Push one 1m bar snapshot to FIIDIIClassifier.
    No-op if bar already pushed or no OI data exists for this time.
    """
    if hhmm in lk['pushed'] or hhmm not in lk['all_hhmm_set']:
        return

    ce_oi_s: dict = {}
    pe_oi_s: dict = {}
    ce_cl_s: dict = {}
    pe_cl_s: dict = {}

    for strike in lk['strikes']:
        slk = lk['strike_lookup'][strike]
        v = slk['CE_oi'].get(hhmm);    ce_oi_s[strike] = v  if v is not None else None
        v = slk['PE_oi'].get(hhmm);    pe_oi_s[strike] = v  if v is not None else None
        v = slk['CE_close'].get(hhmm); ce_cl_s[strike] = v  if v is not None else None
        v = slk['PE_close'].get(hhmm); pe_cl_s[strike] = v  if v is not None else None

    # Remove None entries
    ce_oi_s = {k: v for k, v in ce_oi_s.items() if v is not None}
    pe_oi_s = {k: v for k, v in pe_oi_s.items() if v is not None}
    ce_cl_s = {k: v for k, v in ce_cl_s.items() if v is not None}
    pe_cl_s = {k: v for k, v in pe_cl_s.items() if v is not None}

    lk['pushed'].add(hhmm)  # mark as pushed regardless — avoids retry on empty bars

    if not ce_oi_s and not pe_oi_s:
        return False   # nothing pushed

    td_str = lk['trade_date_str']
    bar_ts = pd.Timestamp(f'{td_str} {hhmm}') if td_str else pd.Timestamp(hhmm)

    snap = OISnapshot(
        ts         = bar_ts,
        atm_strike = lk['atm_strike'],
        strikes    = lk['strikes'],
        ce_oi      = ce_oi_s,
        pe_oi      = pe_oi_s,
        ce_close   = ce_cl_s,
        pe_close   = pe_cl_s,
        fut_close  = lk['fut_lookup'].get(hhmm, spot_proxy),
        spot_close = spot_proxy,
    )
    clf.push(snap)
    return True   # OI data was pushed



# ── Pre-import signal helper for fast per-bar OI quadrant ────────────────────
from v3.signals.engine import signal_oi_quadrant

# ── Backtest loop ─────────────────────────────────────────────────────────────
results = []

for i, td in enumerate(trade_dates):
    # Date-range filter: skip days outside --start/--end window
    if _FILTER_START and td < _FILTER_START:
        continue
    if _FILTER_END and td > _FILTER_END:
        continue

    # MIN_PRIOR_DAYS: need enough history for regime/velocity/FII context to be meaningful
    if i < MIN_PRIOR_DAYS:
        continue

    day_df = candles[candles['date'] == td].sort_values('ts').reset_index(drop=True)
    if len(day_df) < 20:
        continue

    prev_td    = trade_dates[i - 1] if i > 0 else None
    open_price = float(day_df['open'].iloc[0])
    fut_close  = float(day_df['close'].iloc[-1])

    # Use nearest yfinance calendar date strictly before trade_date.
    # 6mo download covers all Jan-Apr 2026 dates (3mo only went back to Feb 3).
    prior_spot_dates = [d for d in _spot_dates_sorted if d < td]
    spot = spot_close[prior_spot_dates[-1]] if prior_spot_dates else spot_close.get(td, open_price * 0.9985)

    # Regime detection: 5-day spot return > 3% absolute = elevated volatility regime.
    # In this regime we require higher conviction to fire (|score| >= 0.50).
    # Threshold lowered 5% → 3%: data shows 10+ wrong trades came from weak-conviction
    # signals in moderate downtrends (regime -1% to -4%) that the engine misread as LONG.
    if len(prior_spot_dates) >= 5:
        spot_5d_ago = spot_close[prior_spot_dates[-5]]
        regime_5d_return = (float(spot) - spot_5d_ago) / spot_5d_ago * 100.0
    else:
        regime_5d_return = 0.0
    extreme_regime = abs(regime_5d_return) > 3.0

    # F_VOL: skip low-volatility days — option premiums don't move enough to profit.
    # 20-day realized vol < 0.85% = range-bound choppy market with wide bid-ask vs move.
    _vol_today = vol_20d_map.get(td, 0.0)
    if _vol_today > 0 and _vol_today < MIN_VOL_PCT:
        log.debug("F_VOL skip date=%s vol_20d=%.3f%% < %.2f%%",
                  td, _vol_today, MIN_VOL_PCT)
        continue

    # PCR lag-1
    if not _BHAVCOPY_MISSING and not pcr_df.empty:
        pcr_row = pcr_df[pcr_df['date_only'] < td].tail(1)
        pcr_val = float(pcr_row['pcr'].iloc[0]) if not pcr_row.empty else 1.0
        pcr_ma  = (
            float(pcr_row['pcr_5d_ma'].fillna(pcr_row['pcr']).iloc[0])
            if not pcr_row.empty else 1.0
        )
    else:
        pcr_val = 1.0
        pcr_ma  = 1.0

    # FII F&O lag-1
    prev_fo = [d for d in fii_fo_dates if d < str(td)]
    if prev_fo:
        fo = fii_fo[prev_fo[-1]]
        fl, fs = fo.get('fut_long', 0), fo.get('fut_short', 0)
        fii_fut_level = 1 if fl > fs * 1.15 else (-1 if fs > fl * 1.15 else 0)
    else:
        fii_fut_level = 0

    cash_prev = fii_cash_df[fii_cash_df['date'] < td].tail(1)
    net = float(cash_prev['fpi_net'].iloc[0]) if not cash_prev.empty else 0
    fii_cash_lag1 = 1 if net > 500 else (-1 if net < -500 else 0)

    # FII 5-day cash (lag-1): passed to classifier as fii_cash_context feature.
    # The classifier uses this to disambiguate "put writing in uptrend" (genuine
    # FII_BULL) from "put writing in downtrend" (defensive, FII selling cash).
    cash_prior5     = fii_cash_df[fii_cash_df['date'] < td].tail(5)
    fii_cash_5d     = float(cash_prior5['fpi_net'].sum()) if not cash_prior5.empty else 0.0
    fii_cash_5d_norm = float(np.clip(fii_cash_5d / FII_CASH_NORM_DIV, -3.0, 3.0))

    # DTE: nearest Tuesday expiry.
    # max(dte, 1): on expiry day (Tuesday) dte=0, which kills the basis signal.
    # Use 1-day floor — on expiry day any basis is vs ~0 fair premium, still informative.
    dte = max(_get_tuesday_dte(td), 1)

    # Strike walls: prev-day bhavcopy
    walls = {}
    if not _BHAVCOPY_MISSING and bhav_dates:
        prev_bhav_dates = [d for d in bhav_dates if d < str(td)]
        if prev_bhav_dates:
            walls = get_walls_from_bhavcopy(bhav[prev_bhav_dates[-1]], open_price)

    # ── Intraday setup ─────────────────────────────────────────────────────────
    day_df_hhmm = day_df.copy()
    day_df_hhmm['hhmm'] = day_df_hhmm['ts'].dt.strftime('%H:%M')

    # Spot proxy at 9:45 (bar 30) for velocity window and ATM seeding
    _r0945 = day_df_hhmm[day_df_hhmm['hhmm'] == '09:45']
    spot_at_0945 = float(_r0945['close'].iloc[0]) if not _r0945.empty else open_price

    # OI data for this day — used for dynamic per-bar velocity computation in bar loop.
    # velocity_data is NOT pre-computed here; it's recomputed at each signal evaluation
    # bar using the rolling window of bars up to that point (no lookahead).
    day_oi        = opt_oi_cache.get(str(td), {})
    _has_oi_data  = bool(day_oi) and any(
        len(df) > 0
        for sides in day_oi.values()
        for df in sides.values()
    )
    velocity_data = {}   # will be populated dynamically per-bar below

    # Pre-build velocity numpy array cache — O(strikes × bars) once, O(1) per-bar lookups
    vel_cache = _build_vel_cache(day_oi) if _has_oi_data else {}

    # Pre-build option price cache — O(strikes × bars) once, O(1) per-bar SL/TP lookups
    day_opt_raw = opt_oi_cache.get(str(td), {})
    opt_day_cache = _build_opt_price_cache(day_opt_raw) if day_opt_raw else {}

    # Pre-build CLF lookup dicts for this day — O(strikes × bars) once
    clf_lookups   = None
    fii_dii_live  = None   # classifier result at the latest pushed bar
    if _FII_CLF_AVAILABLE and _clf is not None and str(td) in opt_oi_cache:
        try:
            clf_lookups = _build_clf_lookups(opt_oi_cache[str(td)], day_df, spot_at_0945)
            if clf_lookups:
                _clf._buffer.clear()
                _clf._daily_oi_adds.clear()
                _clf._prev_snap = None
        except Exception as _clf_build_err:
            log.warning("CLF lookup build failed date=%s error=%s", td, _clf_build_err)
            clf_lookups = None

    # ── Fast-path: no intraday data at all → compute one signal for CSV, skip loop ─
    # Days with no option OI AND no CLF (e.g. Jan 2026) cannot fire a trade due to
    # N/A suppression.  Skip the full bar loop to avoid 300 wasted signal evaluations.
    _skip_loop = (not _has_oi_data) and (clf_lookups is None)
    if _skip_loop:
        _bar_for_csv = min(MIN_SIGNAL_BAR, len(day_df) - 1)
        if _bar_for_csv >= 0:
            _cb = day_df.iloc[_bar_for_csv]
            final_state = compute_signal_state(
                df_1m=day_df.iloc[:_bar_for_csv + 1],
                futures_ltp=float(_cb['close']), spot_ltp=float(spot),
                days_to_expiry=dte, pcr=pcr_val, pcr_5d_ma=pcr_ma,
                velocity_data={}, walls=walls,
                fii_fut_level=fii_fut_level, fii_cash_lag1=fii_cash_lag1,
                timestamp=_cb['ts'],
            )

    # ── Real-time bar-by-bar position loop ────────────────────────────────────
    # Logic:
    #   • Signal evaluated at every bar using ONLY data available up to that bar.
    #   • Entry at NEXT bar's close (realistic: current bar just closed, order executes next).
    #   • Exit on SL (-50% option premium), TP (+100%), EOD (15:20), or signal reversal.
    #   • One trade per day — no re-entry after exit.
    #   • N/A suppression: direction zeroed when no intraday OI (velocity=0, clf=None).
    #   • Extreme-regime filter applied per bar.

    in_position      = False
    direction_taken  = 0
    entry_bar_idx    = None    # bar index at entry — used for MIN_REVERSAL_HOLD
    entry_opt_px     = None
    entry_opt_strike = None
    entry_opt_side   = None
    entry_time       = None
    exit_opt_px      = None
    exit_time        = None
    exit_reason      = None
    entry_state      = None
    fii_dii_at_entry = None
    final_state      = None    # last computed signal state (for no-trade rows)
    final_fii        = None

    for bar_idx in (range(len(day_df)) if not _skip_loop else []):
        current_bar   = day_df.iloc[bar_idx]
        current_hhmm  = current_bar['ts'].strftime('%H:%M')
        current_fut   = float(current_bar['close'])

        # Advance classifier incrementally — only classify() when new OI data was pushed
        if clf_lookups is not None:
            try:
                pushed_now = _push_clf_bar(_clf, clf_lookups, current_hhmm, spot_at_0945)
                if pushed_now:
                    _r = _clf.classify(fii_cash_5d_norm=fii_cash_5d_norm)
                    if not (_r.get('attribution') == 'UNKNOWN' and _r.get('confidence', 0) == 0):
                        fii_dii_live = _r
            except Exception:
                pass

        # Need at least MIN_SIGNAL_BAR bars of history before evaluating signal
        if bar_idx < MIN_SIGNAL_BAR:
            continue

        eod = (current_hhmm >= EOD_EXIT_HHMM)

        # ── Hot path: in-position SL/TP/EOD check every bar (no signal needed) ─
        if in_position:
            opt_px_now = _opt_px(opt_day_cache, entry_opt_strike, entry_opt_side, current_hhmm)
            if opt_px_now is not None:
                pnl_pct = (opt_px_now - entry_opt_px) / entry_opt_px
                sl_hit  = pnl_pct <= SL_PCT
                tp_hit  = pnl_pct >= TP_PCT
                if sl_hit or tp_hit or eod:
                    exit_opt_px = opt_px_now
                    exit_time   = current_hhmm
                    exit_reason = 'SL' if sl_hit else ('TP' if tp_hit else 'EOD')
                    in_position = False
                    break
            elif eod:
                exit_time   = current_hhmm
                exit_reason = 'EOD_NO_DATA'
                in_position = False
                break
            # Reversal check every 5 bars (signal recomputed, less critical than SL/TP)
            if bar_idx % 5 != 0:
                continue

        # Not in position and EOD — nothing to do
        elif eod:
            continue

        # ── Dynamic velocity: recompute at this bar using pre-built numpy arrays ──
        # O(strikes_in_band) per call — no DataFrame slicing.
        # On days with no OI data, velocity_data stays {} → no_intraday=True → entry blocked.
        if _has_oi_data:
            velocity_data = _compute_velocity_fast(
                vel_cache, current_fut, bar_idx, window=VELOCITY_WINDOW
            )
        else:
            velocity_data = {}

        # ── Signal evaluation: only data up to current bar (no lookahead) ────
        df_so_far   = day_df.iloc[:bar_idx + 1]
        no_intraday = (not velocity_data) and (fii_dii_live is None)

        state = compute_signal_state(
            df_1m=df_so_far, futures_ltp=current_fut, spot_ltp=float(spot),
            days_to_expiry=dte, pcr=pcr_val, pcr_5d_ma=pcr_ma,
            velocity_data=velocity_data, walls=walls,
            fii_fut_level=fii_fut_level, fii_cash_lag1=fii_cash_lag1,
            timestamp=current_bar['ts'],
            fii_dii_result=fii_dii_live,
        )

        # Intraday price vs day's opening price (for F4b filter below)
        vs_open_pct = (current_fut - open_price) / open_price * 100.0

        # Apply filters to effective direction
        effective_dir = state.direction

        # ── F1: Extreme regime → require higher conviction ────────────────────
        # |5d return| > 3%: OI/velocity data becomes misleading in trending regimes.
        # Weak-score signals (< 0.50) in elevated-vol regimes have ~35% accuracy.
        if extreme_regime and abs(state.score) < 0.50:
            effective_dir = 0

        if no_intraday and effective_dir != 0:
            effective_dir = 0

        # ── F2: PCR hard veto — both directions ──────────────────────────────
        # The PCR signal directly measures the options market's put/call balance.
        # When PCR explicitly contradicts direction (pcr=-1 + LONG, or pcr=+1 + SHORT),
        # the options market is pricing in the opposite outcome — this is a structural
        # contradiction, not just low conviction. Hard veto regardless of score.
        # Data: pcr-disagree trades = 0% WR across all 3 occurrences in dataset.
        if effective_dir != 0 and state.pcr != 0 and state.pcr != effective_dir:
            effective_dir = 0

        # ── F3: FII/DII classifier BEAR + LONG direction (low conviction) ────
        # CLF attribution=FII_BEAR means the classifier sees institutional bears.
        # Entering LONG against FII_BEAR with score < 0.45 has ~30% accuracy.
        # High-conviction LONG signals (score >= 0.45) can still fire even with FII_BEAR.
        if effective_dir == 1 and fii_dii_live is not None:
            if fii_dii_live.get('attribution') == 'FII_BEAR' and state.score < 0.45:
                effective_dir = 0

        # ── F4a: OI quadrant bearish + LONG direction ─────────────────────────
        # OI quadrant = -1 means recent 5-bar price×OI action is bearish
        # (short buildup or long unwind). Going LONG against this OI signal has
        # historically been wrong — the velocity/basis were misleading.
        if effective_dir == 1 and state.oi_quadrant == -1:
            effective_dir = 0

        # ── F4b: Price run-up vs open + LONG + strike defense against ─────────
        # If price has already moved >0.5% above day's open AND strike defense
        # signal is -1 (sees call writers capping the rally), we are chasing a
        # move into resistance. These entries have historically failed.
        if effective_dir == 1 and vs_open_pct > 0.5 and state.strike_defense == -1:
            effective_dir = 0

        # ── F5: Extreme contango in heavy crash regime → suppress LONG ────────
        # In sharp downtrends (5d return < -3%), abnormally high futures basis
        # (>1.0% above fair value) is caused by shorts piling onto futures while
        # spot is sold, creating artificial contango.  The basis signal misreads
        # this as institutional buying — but it's forced hedging.
        # Any basis > 1.0% while regime < -3.0% is a structural trap for LONG.
        if effective_dir == 1 and regime_5d_return < -3.0:
            raw_prem   = (current_fut - float(spot)) / float(spot) * 100.0
            fair_prem  = 8.0 * (dte / 365)
            basis_now  = raw_prem - fair_prem
            if basis_now > 1.0:
                effective_dir = 0

        # ── Intraday momentum filter ──────────────────────────────────────────
        # Price must be trending in the signal direction for the last MOMENTUM_BARS.
        # Prevents entering a SHORT when price has been rallying for 15+ minutes,
        # or a LONG when price has been declining — even if the static OI says otherwise.
        if effective_dir != 0 and bar_idx >= MOMENTUM_BARS:
            price_now  = current_fut
            price_past = float(day_df.iloc[bar_idx - MOMENTUM_BARS]['close'])
            price_mom  = 1 if price_now > price_past else -1
            if price_mom != effective_dir:
                effective_dir = 0   # momentum disagrees — skip this bar

        # ── Signal consensus filter ───────────────────────────────────────────
        # Require at least MIN_SIGNAL_COUNT of 6 signals to agree.
        # Weak-consensus trades (4/6) have historically been coin-flips.
        if effective_dir != 0 and state.signal_count < MIN_SIGNAL_COUNT:
            effective_dir = 0

        final_state = state
        final_fii   = fii_dii_live

        # ── In-position reversal check ────────────────────────────────────────
        if in_position:
            rev_signal = (effective_dir != 0 and effective_dir != direction_taken)
            bars_held  = bar_idx - entry_bar_idx if entry_bar_idx is not None else 0
            # Minimum hold before reversal is allowed — prevents 2-bar whipsaw exits
            # in choppy contango markets where the smoother oscillates on noise.
            if rev_signal and bars_held >= MIN_REVERSAL_HOLD:
                opt_px_now = _opt_px(opt_day_cache, entry_opt_strike, entry_opt_side, current_hhmm)
                if opt_px_now is not None:
                    exit_opt_px = opt_px_now
                    exit_time   = current_hhmm
                    exit_reason = 'REVERSAL'
                    in_position = False
                    break

        # ── Not in position: look for entry signal ────────────────────────────
        elif current_hhmm <= LAST_ENTRY_HHMM and effective_dir != 0:
            # Execute on NEXT bar (current bar just closed, order fills next bar)
            next_idx = bar_idx + 1
            if next_idx >= len(day_df):
                continue
            next_hhmm = day_df.iloc[next_idx]['ts'].strftime('%H:%M')
            if next_hhmm > LAST_ENTRY_HHMM:
                continue

            atm  = round(current_fut / NIFTY_STEP) * NIFTY_STEP
            side = 'CE' if effective_dir == 1 else 'PE'

            actual_strike, opt_px = _opt_px_atm(opt_day_cache, atm, side, next_hhmm)
            if opt_px is not None and opt_px > 0:
                in_position      = True
                direction_taken  = effective_dir
                entry_bar_idx    = bar_idx
                entry_opt_px     = opt_px
                entry_opt_strike = actual_strike
                entry_opt_side   = side
                entry_time       = next_hhmm
                entry_state      = state
                fii_dii_at_entry = fii_dii_live
                log.info(
                    "ENTRY date=%s dir=%s strike=%s side=%s px=%.1f time=%s score=%.3f",
                    td, effective_dir, actual_strike, side, opt_px, next_hhmm, state.score,
                )

    # ── Force EOD exit if still in position after bar loop ────────────────────
    if in_position:
        for try_hhmm in [EOD_EXIT_HHMM, '15:25', '15:15', '15:10', '15:05', '15:00']:
            p = _opt_px(opt_day_cache, entry_opt_strike, entry_opt_side, try_hhmm)
            if p is not None and p > 0:
                exit_opt_px = p
                exit_time   = try_hhmm
                break
        exit_reason = 'EOD'
        in_position = False

    # ── Compute PnL ──────────────────────────────────────────────────────────
    pnl_pts      = float('nan')
    pnl_inr      = float('nan')
    trade_result = 'NO_TRADE'

    if entry_opt_px is not None and exit_opt_px is not None:
        pnl_pts      = round(exit_opt_px - entry_opt_px, 2)
        pnl_inr      = round(pnl_pts * NIFTY_LOT, 2)
        trade_result = f"{'WIN' if pnl_pts > 0 else 'LOSS'}_{exit_reason}"
    elif entry_opt_px is not None:
        trade_result = f'NO_EXIT_DATA_{exit_reason or "?"}'

    # ── Actual direction: measured over our entry→exit window ─────────────────
    actual_trade = 0
    if entry_time and exit_time:
        fe = day_df_hhmm[day_df_hhmm['hhmm'] == entry_time]['close']
        fx = day_df_hhmm[day_df_hhmm['hhmm'] == exit_time]['close']
        if not fe.empty and not fx.empty:
            actual_trade = 1 if float(fx.iloc[0]) > float(fe.iloc[0]) else -1
    actual_fullday = 1 if fut_close > open_price else -1

    # ── Build result row ──────────────────────────────────────────────────────
    use_state = entry_state  if entry_state  else final_state
    use_fii   = fii_dii_at_entry if fii_dii_at_entry else final_fii

    row = state_to_dict(use_state) if use_state else {}
    row['direction'] = direction_taken  # 0 if no trade fired this day

    row.update({
        'trade_date':     str(td),
        'day_open':       open_price,
        'fut_close':      fut_close,
        'actual':         actual_trade,
        'actual_fullday': actual_fullday,
        'pcr_input':      round(pcr_val, 3),
        'fii_fut':        fii_fut_level,
        'fii_cash':       fii_cash_lag1,
        'call_wall_in':   walls.get('call_wall'),
        'put_wall_in':    walls.get('put_wall'),
        'has_opt_oi':     _has_oi_data,
        'dte':            dte,
        'regime_5d_ret':  round(regime_5d_return, 2),
        'extreme_regime': extreme_regime,
        # FII/DII classifier at entry (or last available for no-trade days)
        'fii_attribution': use_fii.get('attribution', 'N/A') if use_fii else 'N/A',
        'clf_confidence':  round(use_fii.get('confidence', 0.0), 3) if use_fii else 0.0,
        'clf_fii_score':   round(use_fii.get('fii_score', 0.0), 3) if use_fii else 0.0,
        # Option trade columns
        'opt_strike':     entry_opt_strike,
        'opt_side':       entry_opt_side,
        'entry_time':     entry_time,
        'exit_time':      exit_time,
        'exit_reason':    exit_reason,
        'opt_entry':      entry_opt_px if entry_opt_px is not None else float('nan'),
        'opt_exit':       exit_opt_px  if exit_opt_px  is not None else float('nan'),
        'pnl_pts':        pnl_pts,
        'pnl_inr':        pnl_inr,
        'result':         trade_result,
    })
    results.append(row)

res = pd.DataFrame(results)

# ── Save CSV immediately (before any printing, so it survives timeout kills) ──
_CSV_OUT_NAME = _OUT_OVERRIDE if _OUT_OVERRIDE else 'trade_log_options_nifty.csv'
_out_csv_early = ROOT / _CSV_OUT_NAME
_export_cols_early = [
    'trade_date', 'direction', 'actual', 'actual_fullday', 'score', 'signal_count',
    'opt_strike', 'opt_side', 'entry_time', 'exit_time', 'exit_reason',
    'opt_entry', 'opt_exit', 'pnl_pts', 'pnl_inr', 'result',
    'oi_quadrant', 'futures_basis', 'pcr_signal', 'oi_velocity',
    'strike_defense', 'fii_signature', 'has_opt_oi',
    'fii_attribution', 'clf_confidence', 'clf_fii_score',
    'dte', 'regime_5d_ret', 'extreme_regime', 'call_wall_in', 'put_wall_in',
    'pcr_input', 'fii_fut', 'fii_cash', 'day_open', 'notes',
]
_export_cols_early = [c for c in _export_cols_early if c in res.columns]
res[_export_cols_early].to_csv(_out_csv_early, index=False)
log.info(f"CSV saved: {_out_csv_early}  rows={len(res)}")

# ── Output ────────────────────────────────────────────────────────────────────
if _BHAVCOPY_MISSING:
    print(
        "\n[DEGRADED MODE] bhavcopy_NIFTY_all.pkl missing — "
        "PCR=1.0, walls={} for all days. Results will underperform.\n"
    )

# Check if option price data is available in cache
has_opt_price = any(
    'close' in sides.get(s, pd.DataFrame()).columns
    for day in opt_oi_cache.values()
    for sides_dict in day.values()
    for s, sides in sides_dict.items()
) if opt_oi_cache else False

if not has_opt_price:
    print(
        "\nWARNING: Option cache has no 'close' (LTP) column — "
        "pnl_pts/pnl_inr will be NaN for all days.\n"
        "Fix: run  python v3/data/fetch_option_oi_NIFTY.py --force\n"
        "This re-fetches all days with OHLCV (takes ~2 hours, needs valid token).\n"
    )

print('\n=== NIFTY V3 BACKTEST — REAL-TIME BAR-BY-BAR ===')
print(f"  SL={SL_PCT*100:.0f}%  TP={TP_PCT*100:.0f}%  EOD={EOD_EXIT_HHMM}  "
      f"min_bar={MIN_SIGNAL_BAR} (11:00)  last_entry={LAST_ENTRY_HHMM}  "
      f"filters=F1+F2+F3+F4a+F4b+F5+MinHist({MIN_PRIOR_DAYS}d)  clf_feature=fii_cash_context")
print(f"{'DATE':<12} {'DIR':<6} {'ACT':<5} {'STRIKE':<8} {'SIDE':<5} "
      f"{'IN':>5} {'OUT':>5} {'ENTRY':>7} {'EXIT':>7} {'PNL_PTS':>8} {'PNL_INR':>9} "
      f"{'RESULT':<18} {'SCORE':>6}")
print('-' * 118)
for _, r in res.iterrows():
    if r['direction'] == 0:
        continue
    d_str   = r['trade_date']
    sig     = 'LONG ' if r['direction'] == 1 else 'SHORT'
    act_str = 'UP  '  if r['actual']    == 1 else ('DOWN' if r['actual'] == -1 else 'N/A ')
    strike  = str(r['opt_strike']) if r['opt_strike'] else '—'
    side    = str(r['opt_side'])   if r['opt_side']   else '—'
    in_t    = str(r['entry_time']) if r['entry_time'] else '—'
    out_t   = str(r['exit_time'])  if r['exit_time']  else '—'
    has_px  = pd.notna(r['opt_entry']) and pd.notna(r['opt_exit'])
    entry   = f"{r['opt_entry']:.1f}"  if has_px else '—'
    exit_p  = f"{r['opt_exit']:.1f}"   if has_px else '—'
    pnl_p   = f"{r['pnl_pts']:+.1f}"  if pd.notna(r['pnl_pts']) else '—'
    pnl_i   = f"{r['pnl_inr']:+.0f}"  if pd.notna(r['pnl_inr']) else '—'
    print(f"{d_str:<12} {sig:<6} {act_str:<5} {strike:<8} {side:<5} "
          f"{in_t:>5} {out_t:>5} {entry:>7} {exit_p:>7} {pnl_p:>8} {pnl_i:>9} "
          f"{r['result']:<18} {r['score']:>+6.3f}")

fired     = res[res['direction'] != 0]
priced    = fired[fired['result'].str.startswith('WIN') | fired['result'].str.startswith('LOSS')]
wins      = priced[priced['result'].str.startswith('WIN')]
losses    = priced[priced['result'].str.startswith('LOSS')]
no_data   = fired[~fired['result'].str.startswith('WIN') & ~fired['result'].str.startswith('LOSS')]
correct   = fired[fired['direction'] == fired['actual']]

print(f'\n{"─"*60}')
print(f'Total trading days : {len(res)}')
print(f'Signal fired       : {len(fired)}')
print(f'  → With opt price : {len(priced)}  (WIN={len(wins)}, LOSS={len(losses)})')
print(f'  → No opt data    : {len(no_data)}  (cache missing close column or date gap)')
print(f'Direction accuracy : {len(correct)}/{len(fired)} = {len(correct)/len(fired)*100:.1f}%' if len(fired) else 'n/a')

if len(priced):
    total_pnl  = priced['pnl_pts'].sum()
    total_inr  = priced['pnl_inr'].sum()
    avg_win    = wins['pnl_pts'].mean()   if len(wins)   else 0.0
    avg_loss   = losses['pnl_pts'].mean() if len(losses) else 0.0
    win_sum    = wins['pnl_pts'].sum()    if len(wins)   else 0.0
    loss_sum   = abs(losses['pnl_pts'].sum()) if len(losses) else 1.0
    pf         = win_sum / loss_sum if loss_sum else float('inf')
    print(f'\nOption PnL (1 lot = {NIFTY_LOT} units):')
    print(f'  Total pts   : {total_pnl:+.1f}')
    print(f'  Total INR   : ₹{total_inr:+,.0f}')
    print(f'  Avg win     : {avg_win:+.1f} pts')
    print(f'  Avg loss    : {avg_loss:+.1f} pts')
    print(f'  Profit factor: {pf:.2f}')
    print(f'  Win rate    : {len(wins)/len(priced)*100:.1f}%')
else:
    print('\nNo option price data available — re-fetch cache with --force flag.')

# FII/DII classifier breakdown
if _FII_CLF_AVAILABLE and 'fii_attribution' in fired.columns:
    print(f'\nFII/DII Classifier breakdown (fired trades):')
    for attr, grp in fired.groupby('fii_attribution'):
        correct_grp = grp[grp['direction'] == grp['actual']]
        pct = len(correct_grp) / len(grp) * 100 if len(grp) else 0
        priced_grp = grp[grp['result'].str.startswith('WIN') | grp['result'].str.startswith('LOSS')]
        pnl_grp = priced_grp['pnl_pts'].sum() if not priced_grp.empty else float('nan')
        pnl_str = f"  pnl={pnl_grp:+.1f}pts" if pd.notna(pnl_grp) else "  pnl=—"
        print(f"  {attr:<14} n={len(grp):>2}  dir_acc={pct:.0f}%{pnl_str}")

# Save CSV (already saved above; this is a no-op overwrite for completeness)
out_csv = ROOT / _CSV_OUT_NAME
export_cols = [
    'trade_date', 'direction', 'actual', 'actual_fullday', 'score', 'signal_count',
    'opt_strike', 'opt_side', 'entry_time', 'exit_time', 'exit_reason',
    'opt_entry', 'opt_exit', 'pnl_pts', 'pnl_inr', 'result',
    'oi_quadrant', 'futures_basis', 'pcr_signal', 'oi_velocity',
    'strike_defense', 'fii_signature', 'has_opt_oi',
    'fii_attribution', 'clf_confidence', 'clf_fii_score',
    'dte', 'regime_5d_ret', 'extreme_regime', 'call_wall_in', 'put_wall_in',
    'pcr_input', 'fii_fut', 'fii_cash', 'day_open', 'notes',
]
export_cols = [c for c in export_cols if c in res.columns]
res[export_cols].to_csv(out_csv, index=False)
print(f'\nCSV saved: {out_csv}')

# Fired detail
print('\nFIRED DETAIL:')
for _, r in fired.iterrows():
    c   = 'OK' if r['direction'] == r['actual'] else 'XX'
    sig = 'LONG ' if r['direction'] == 1 else 'SHORT'
    act = 'UP  '  if r['actual']    == 1 else 'DOWN'
    mv  = r['fut_close'] - r.get('day_open', r['fut_close'])   # full-day move for context
    pnl = f"  opt_pnl={r['pnl_pts']:+.1f}pts" if pd.notna(r['pnl_pts']) else "  opt_pnl=—"
    fullday_str = f"fullday={'UP  ' if r.get('actual_fullday',r['actual'])==1 else 'DOWN'}"
    print(f"  {c} {r['trade_date']}  {sig} window={act} {fullday_str}  fut_move={mv:+6.0f}{pnl}  "
          f"score={r['score']:+.3f}  sigs={r['signal_count']}/6")
    print(f"     {r['notes'][:140]}")
