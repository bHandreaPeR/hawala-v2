"""
v3/backtest/run_backtest_nifty_lowvol.py
=========================================
Low-volatility regime strategies for NIFTY.
Runs ONLY on days where 20d realized vol < MIN_VOL_PCT (range-bound market).
High-vol days (the main strategy's domain) are skipped here.

Two modes (--mode flag):

  orb  — ORB-triggered ATM directional buy.
          Entry: signal fires AND market has already moved ≥0.35% from open
          in the signal direction (momentum confirmed before we enter).
          Min entry bar: 135 (11:30 AM). Strike: ATM.
          SL=-50%, TP=+80% (tighter TP — less time in low-vol).
          Premise: even in a low-vol month, 3-5 days have real momentum.
          Wait for the market to show its hand before buying.

  otm  — Deep OTM event lottery.
          Entry: signal fires at 11:00 AM+ (same bar as high-vol strategy).
          Strike: 200pts OTM from ATM in signal direction.
          Premium ~15-30 pts. SL=-60%, TP=+200% (3x needed for OTM to pay).
          Premise: small defined risk, asymmetric payoff on tail moves/event days.

Usage:
    python v3/backtest/run_backtest_nifty_lowvol.py --mode orb
    python v3/backtest/run_backtest_nifty_lowvol.py --mode otm
"""
import sys, pickle, warnings, logging, argparse
warnings.filterwarnings('ignore')
from pathlib import Path
from datetime import date, datetime as _dt

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

# ── Arg parsing ───────────────────────────────────────────────────────────────
_ap = argparse.ArgumentParser()
_ap.add_argument('--mode', choices=['orb', 'otm'], required=True,
                 help='orb = ORB-triggered ATM buy | otm = deep OTM lottery')
_ap.add_argument('--start', default=None, help='Filter start date YYYY-MM-DD')
_ap.add_argument('--end',   default=None, help='Filter end date YYYY-MM-DD')
_ap.add_argument('--out',   default=None, help='Override output CSV name')
_args = _ap.parse_args()

MODE          = _args.mode
_FILTER_START = pd.Timestamp(_args.start).date() if _args.start else None
_FILTER_END   = pd.Timestamp(_args.end).date()   if _args.end   else None

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(f'backtest_nifty_lowvol_{MODE}')

from v3.signals.engine import compute_signal_state, state_to_dict
from v3.data.fetch_option_oi import compute_intraday_velocity

try:
    from v3.signals.fii_dii_classifier import FIIDIIClassifier, OISnapshot
    _clf = FIIDIIClassifier()
    _FII_CLF_AVAILABLE = True
    print("FII/DII classifier loaded")
except Exception as _clf_err:
    _clf = None
    _FII_CLF_AVAILABLE = False
    print(f"WARNING: FII/DII classifier unavailable: {_clf_err}")

# ── Mode-specific constants ────────────────────────────────────────────────────
MIN_VOL_PCT   = 0.85    # trade ONLY when vol < this (low-vol regime)
NIFTY_LOT     = 75
NIFTY_STEP    = 50
VELOCITY_WINDOW  = 60
MOMENTUM_BARS    = 30
MIN_SIGNAL_COUNT = 5
LAST_ENTRY_HHMM  = '13:00'
EOD_EXIT_HHMM    = '15:20'
MIN_PRIOR_DAYS   = 5
FII_CASH_NORM_DIV = 20_000.0

if MODE == 'orb':
    MIN_SIGNAL_BAR       = 135     # bar 135 = 11:30 AM (delayed for momentum confirmation)
    INTRADAY_MOVE_THRESH = 0.35    # market must have moved ≥0.35% from open in signal dir
    OTM_OFFSET           = 0      # ATM entry
    SL_PCT               = -0.50
    TP_PCT               = +0.80   # tighter TP: less time in low-vol day
    OUT_CSV              = ROOT / 'trade_log_nifty_lowvol_orb.csv'
    print(f"[ORB] ATM entry | min_bar=135 (11:30 AM) | move_thresh={INTRADAY_MOVE_THRESH}% | "
          f"SL={SL_PCT*100:.0f}% TP={TP_PCT*100:.0f}%")
else:  # otm
    MIN_SIGNAL_BAR       = 105    # bar 105 = 11:00 AM (same as high-vol strategy)
    INTRADAY_MOVE_THRESH = 0.0    # no momentum check — lottery, take the ticket
    OTM_OFFSET           = 200    # pts OTM from ATM in signal direction
    SL_PCT               = -0.60  # tighter % — premium is small, protect what's left
    TP_PCT               = +2.00  # 3x — OTM needs a real move, anything less doesn't pay
    OUT_CSV              = ROOT / 'trade_log_nifty_lowvol_otm.csv'
    print(f"[OTM] {OTM_OFFSET}pt OTM entry | min_bar=105 (11:00 AM) | "
          f"SL={SL_PCT*100:.0f}% TP={TP_PCT*100:.0f}%")

if _args.out:
    OUT_CSV = ROOT / _args.out

# ── Helper functions (same pattern as main backtest) ──────────────────────────

def _build_vel_cache(day_oi: dict) -> dict:
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
    cache: dict = {}
    for strike, sides in day_opt.items():
        cache[strike] = {}
        for side in ('CE', 'PE'):
            df = sides.get(side, pd.DataFrame())
            if df.empty or 'close' not in df.columns or 'ts' not in df.columns:
                cache[strike][side] = {}
                continue
            hhmm_arr  = pd.to_datetime(df['ts']).dt.strftime('%H:%M')
            close_arr = df['close'].astype(float)
            cache[strike][side] = {
                h: float(c) for h, c in zip(hhmm_arr, close_arr) if c > 0
            }
    return cache


def _opt_px(cache: dict, strike: int, side: str, hhmm: str) -> float | None:
    return cache.get(strike, {}).get(side, {}).get(hhmm)


def _opt_px_atm(cache: dict, atm: int, side: str, hhmm: str) -> tuple:
    """Try ATM ±offsets until valid price found."""
    for offset in [0, NIFTY_STEP, -NIFTY_STEP, 2*NIFTY_STEP, -2*NIFTY_STEP]:
        px = _opt_px(cache, atm + offset, side, hhmm)
        if px is not None:
            return atm + offset, px
    return None, None


def _opt_px_otm(cache: dict, otm_target: int, side: str, hhmm: str,
                min_premium: float = 5.0) -> tuple:
    """
    Find a valid OTM option price near otm_target.
    Falls back to adjacent strikes (±50, ±100, ±150) if exact not available.
    Requires premium >= min_premium (avoid zero-bid options).
    Returns (actual_strike, price) or (None, None) if nothing valid found.
    """
    for offset in [0, NIFTY_STEP, -NIFTY_STEP, 2*NIFTY_STEP, -2*NIFTY_STEP, 3*NIFTY_STEP, -3*NIFTY_STEP]:
        strike = otm_target + offset
        px = _opt_px(cache, strike, side, hhmm)
        if px is not None and px >= min_premium:
            return strike, px
    return None, None


# ── Classifier helpers (inlined — do NOT import from run_backtest_nifty.py,
#    that module executes its entire backtest at import time) ──────────────────

def _build_clf_lookups(
    day_oi: dict, day_df_fut, spot_proxy: float
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

    fut_lookup: dict    = {}
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


def _push_clf_bar(clf, lk: dict, hhmm: str, spot_proxy: float):
    """
    Push one 1m bar snapshot to FIIDIIClassifier.
    No-op if bar already pushed or no OI data exists for this time.
    Returns True if OI data was pushed, False/None otherwise.
    """
    from v3.signals.fii_dii_classifier import OISnapshot
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

    ce_oi_s = {k: v for k, v in ce_oi_s.items() if v is not None}
    pe_oi_s = {k: v for k, v in pe_oi_s.items() if v is not None}
    ce_cl_s = {k: v for k, v in ce_cl_s.items() if v is not None}
    pe_cl_s = {k: v for k, v in pe_cl_s.items() if v is not None}

    lk['pushed'].add(hhmm)

    if not ce_oi_s and not pe_oi_s:
        return False

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
    return True


def _get_tuesday_dte(trade_d: date) -> int:
    return (1 - trade_d.weekday()) % 7


def get_walls_from_bhavcopy(df_strikes, spot, band=1500):
    if df_strikes.empty or spot <= 0:
        return {}
    sub = df_strikes[
        (df_strikes['strike'] >= spot - band) &
        (df_strikes['strike'] <= spot + band)
    ].copy()
    if sub.empty:
        sub = df_strikes.copy()
    total_ce    = sub['ce_oi'].sum()
    total_pe    = sub['pe_oi'].sum()
    pcr_live    = total_pe / total_ce if total_ce > 0 else 1.0
    calls_above = sub[sub['strike'] > spot]
    puts_below  = sub[sub['strike'] < spot]
    call_wall   = int(calls_above.loc[calls_above['ce_oi'].idxmax(), 'strike']) if not calls_above.empty else None
    put_wall    = int(puts_below.loc[puts_below['pe_oi'].idxmax(), 'strike'])   if not puts_below.empty  else None
    return {'call_wall': call_wall, 'put_wall': put_wall, 'pcr_live': round(pcr_live, 3), 'ltp': spot}


# ── Data loading ──────────────────────────────────────────────────────────────

candle_file = ROOT / 'v3/cache/candles_1m_NIFTY.pkl'
if not candle_file.exists():
    raise FileNotFoundError(f"NIFTY candle cache not found: {candle_file}")
with open(candle_file, 'rb') as f:
    candles = pickle.load(f)
trade_dates = sorted(candles['date'].unique())
print(f"NIFTY candles: {len(candles)} rows, {len(trade_dates)} trading days")

# 20-day realized vol map — used to INCLUDE only low-vol days
_daily_close = candles.groupby('date')['close'].last().sort_index()
_daily_ret   = _daily_close.pct_change() * 100
_vol_series  = _daily_ret.rolling(20, min_periods=10).std()
vol_20d_map  = _vol_series.to_dict()
low_vol_days = [d for d, v in vol_20d_map.items() if 0.0 < v < MIN_VOL_PCT]
print(f"Low-vol days available (<{MIN_VOL_PCT}%): {len(low_vol_days)}")

# Bhavcopy
bhav_file = ROOT / 'v3/cache/bhavcopy_NIFTY_all.pkl'
bhav, bhav_dates = {}, []
pcr_df = pd.DataFrame(columns=['date', 'pcr', 'pcr_5d_ma', 'date_only'])
if bhav_file.exists():
    with open(bhav_file, 'rb') as f:
        bhav = pickle.load(f)
    bhav_dates = sorted(bhav.keys())
    pcr_rows = []
    for d_str, df_s in bhav.items():
        if df_s.empty:
            continue
        total_ce = df_s['ce_oi'].sum() if 'ce_oi' in df_s.columns else 0
        total_pe = df_s['pe_oi'].sum() if 'pe_oi' in df_s.columns else 0
        if total_ce > 0:
            pcr_rows.append({'date': pd.Timestamp(d_str), 'pcr': round(total_pe/total_ce, 4)})
    if pcr_rows:
        pcr_df = pd.DataFrame(pcr_rows).sort_values('date').reset_index(drop=True)
        pcr_df['pcr_5d_ma'] = pcr_df['pcr'].rolling(5, min_periods=1).mean()
        pcr_df['date_only'] = pcr_df['date'].dt.date
else:
    print("WARNING: bhavcopy_NIFTY_all.pkl not found — PCR will default to 1.0")

# Option OI 1m cache
opt_oi_file = ROOT / 'v3/cache/option_oi_1m_NIFTY.pkl'
if opt_oi_file.exists():
    with open(opt_oi_file, 'rb') as f:
        opt_oi_cache = pickle.load(f)
    print(f"NIFTY option OI 1m cache: {len(opt_oi_cache)} days")
else:
    opt_oi_cache = {}
    print("WARNING: option_oi_1m_NIFTY.pkl not found — OI velocity will be 0")

# FII F&O
with open(ROOT / 'trade_logs/_fii_fo_cache.pkl', 'rb') as f:
    fii_fo = pickle.load(f)
fii_fo_dates = sorted(fii_fo.keys())

# FII cash
fii_cash_df = pd.read_csv(ROOT / 'fii_data.csv')
fii_cash_df['date'] = pd.to_datetime(fii_cash_df['date']).dt.date

# Spot proxy from futures daily close (avoids yfinance network dependency).
# Nifty futures close ≈ spot within ~0.2%; sufficient for regime calc + velocity band.
spot_close = _daily_close.to_dict()   # date → last futures close
_spot_dates_sorted = sorted(spot_close.keys())

# ── Main day loop ─────────────────────────────────────────────────────────────
results = []

for i, td in enumerate(trade_dates):
    if _FILTER_START and td < _FILTER_START:
        continue
    if _FILTER_END and td > _FILTER_END:
        continue
    if i < MIN_PRIOR_DAYS:
        continue

    # LOW-VOL GATE: only trade days where vol < MIN_VOL_PCT
    # (opposite of main backtest — this file IS the low-vol strategy)
    _vol_today = vol_20d_map.get(td, 0.0)
    if _vol_today == 0.0 or _vol_today >= MIN_VOL_PCT:
        continue   # skip high-vol days — handled by main strategy

    day_df = candles[candles['date'] == td].sort_values('ts').reset_index(drop=True)
    if len(day_df) < 30:
        continue

    open_price = float(day_df['open'].iloc[0])
    fut_close  = float(day_df['close'].iloc[-1])

    prior_spot_dates = [d for d in _spot_dates_sorted if d < td]
    spot = spot_close[prior_spot_dates[-1]] if prior_spot_dates else open_price * 0.9985

    # Regime detection (same logic as main — for filters F1/F5)
    if len(prior_spot_dates) >= 5:
        spot_5d_ago = spot_close[prior_spot_dates[-5]]
        regime_5d_return = (float(spot) - spot_5d_ago) / spot_5d_ago * 100.0
    else:
        regime_5d_return = 0.0
    extreme_regime = abs(regime_5d_return) > 3.0

    # PCR lag-1
    if not pcr_df.empty and 'date_only' in pcr_df.columns:
        pcr_row = pcr_df[pcr_df['date_only'] < td].tail(1)
        pcr_val = float(pcr_row['pcr'].iloc[0])     if not pcr_row.empty else 1.0
        pcr_ma  = float(pcr_row['pcr_5d_ma'].fillna(pcr_row['pcr']).iloc[0]) if not pcr_row.empty else 1.0
    else:
        pcr_val, pcr_ma = 1.0, 1.0

    # FII F&O lag-1
    prev_fo = [d for d in fii_fo_dates if d < str(td)]
    if prev_fo:
        fo = fii_fo[prev_fo[-1]]
        fl, fs = fo.get('fut_long', 0), fo.get('fut_short', 0)
        fii_fut_level = 1 if fl > fs * 1.15 else (-1 if fs > fl * 1.15 else 0)
    else:
        fii_fut_level = 0

    cash_prev = fii_cash_df[fii_cash_df['date'] < td].tail(1)
    fii_cash_lag1 = 1 if not cash_prev.empty and float(cash_prev['fpi_net'].iloc[0]) > 500 else (
                   -1 if not cash_prev.empty and float(cash_prev['fpi_net'].iloc[0]) < -500 else 0)

    cash_prior5     = fii_cash_df[fii_cash_df['date'] < td].tail(5)
    fii_cash_5d     = float(cash_prior5['fpi_net'].sum()) if not cash_prior5.empty else 0.0
    fii_cash_5d_norm = float(np.clip(fii_cash_5d / FII_CASH_NORM_DIV, -3.0, 3.0))

    dte   = max(_get_tuesday_dte(td), 1)
    walls = {}
    if bhav_dates:
        prev_bhav = [d for d in bhav_dates if d < str(td)]
        if prev_bhav:
            walls = get_walls_from_bhavcopy(bhav[prev_bhav[-1]], open_price)

    # ── ORB range (9:15–9:45) ─────────────────────────────────────────────────
    day_df_hhmm = day_df.copy()
    day_df_hhmm['hhmm'] = day_df_hhmm['ts'].dt.strftime('%H:%M')
    orb_bars    = day_df_hhmm[day_df_hhmm['hhmm'] <= '09:45']
    orb_high    = float(orb_bars['high'].max())  if not orb_bars.empty else open_price * 1.005
    orb_low     = float(orb_bars['low'].min())   if not orb_bars.empty else open_price * 0.995

    _r0945 = day_df_hhmm[day_df_hhmm['hhmm'] == '09:45']
    spot_at_0945 = float(_r0945['close'].iloc[0]) if not _r0945.empty else open_price

    # OI / velocity setup
    day_oi      = opt_oi_cache.get(str(td), {})
    _has_oi     = bool(day_oi) and any(len(df) > 0 for sides in day_oi.values()
                                        for df in sides.values() if hasattr(df, '__len__'))
    vel_cache   = _build_vel_cache(day_oi) if _has_oi else {}
    day_opt_raw = opt_oi_cache.get(str(td), {})
    opt_cache   = _build_opt_price_cache(day_opt_raw) if day_opt_raw else {}

    # Classifier
    clf_lookups  = None
    fii_dii_live = None
    if _FII_CLF_AVAILABLE and _clf is not None and str(td) in opt_oi_cache:
        try:
            clf_lookups = _build_clf_lookups(opt_oi_cache[str(td)], day_df, spot_at_0945)
            if clf_lookups:
                _clf._buffer.clear()
                _clf._daily_oi_adds.clear()
                _clf._prev_snap = None
        except Exception as _e:
            clf_lookups = None

    # ── Bar-by-bar loop ───────────────────────────────────────────────────────
    in_position      = False
    direction_taken  = 0
    entry_opt_px     = None
    entry_opt_strike = None
    entry_opt_side   = None
    entry_time       = None
    exit_opt_px      = None
    exit_time        = None
    exit_reason      = None
    entry_state      = None
    final_state      = None

    for bar_idx in range(len(day_df)):
        current_bar  = day_df.iloc[bar_idx]
        current_hhmm = current_bar['ts'].strftime('%H:%M')
        current_fut  = float(current_bar['close'])

        # Advance classifier
        if clf_lookups is not None:
            try:
                pushed = _push_clf_bar(_clf, clf_lookups, current_hhmm, spot_at_0945)
                if pushed:
                    _r = _clf.classify(fii_cash_5d_norm=fii_cash_5d_norm)
                    if not (_r.get('attribution') == 'UNKNOWN' and _r.get('confidence', 0) == 0):
                        fii_dii_live = _r
            except Exception:
                pass

        if bar_idx < MIN_SIGNAL_BAR:
            continue

        eod = (current_hhmm >= EOD_EXIT_HHMM)

        # In-position: SL / TP / EOD check every bar (O(1))
        if in_position:
            opt_px_now = _opt_px(opt_cache, entry_opt_strike, entry_opt_side, current_hhmm)
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
            if bar_idx % 5 != 0:
                continue

        elif eod:
            continue

        # Velocity
        velocity_data = _compute_velocity_fast(vel_cache, current_fut, bar_idx,
                                               window=VELOCITY_WINDOW) if _has_oi else {}

        # Signal
        df_so_far   = day_df.iloc[:bar_idx + 1]
        no_intraday = (not velocity_data) and (fii_dii_live is None)
        vs_open_pct = (current_fut - open_price) / open_price * 100.0

        state = compute_signal_state(
            df_1m=df_so_far, futures_ltp=current_fut, spot_ltp=float(spot),
            days_to_expiry=dte, pcr=pcr_val, pcr_5d_ma=pcr_ma,
            velocity_data=velocity_data, walls=walls,
            fii_fut_level=fii_fut_level, fii_cash_lag1=fii_cash_lag1,
            timestamp=current_bar['ts'], fii_dii_result=fii_dii_live,
        )

        effective_dir = state.direction

        # ── Filters (identical to main backtest — same filters, different vol context) ──
        # F1: extreme regime → require higher conviction
        if extreme_regime and abs(state.score) < 0.50:
            effective_dir = 0
        # N/A suppression
        if no_intraday and effective_dir != 0:
            effective_dir = 0
        # F2: PCR bearish + LONG + low score
        if effective_dir == 1 and state.pcr == -1 and state.score < 0.55:
            effective_dir = 0
        # F3: FII_BEAR + LONG + low score
        if effective_dir == 1 and fii_dii_live is not None:
            if fii_dii_live.get('attribution') == 'FII_BEAR' and state.score < 0.45:
                effective_dir = 0
        # F4a: OI quadrant bearish + LONG
        if effective_dir == 1 and state.oi_quadrant == -1:
            effective_dir = 0
        # F4b: price run-up + LONG + strike defense against
        if effective_dir == 1 and vs_open_pct > 0.5 and state.strike_defense == -1:
            effective_dir = 0
        # F5: artificial contango in crash
        if effective_dir == 1 and regime_5d_return < -3.0:
            raw_prem  = (current_fut - float(spot)) / float(spot) * 100.0
            fair_prem = 8.0 * (dte / 365)
            if (raw_prem - fair_prem) > 1.0:
                effective_dir = 0
        # Momentum alignment
        if effective_dir != 0 and bar_idx >= MOMENTUM_BARS:
            price_mom = 1 if current_fut > float(day_df.iloc[bar_idx - MOMENTUM_BARS]['close']) else -1
            if price_mom != effective_dir:
                effective_dir = 0
        # Signal consensus
        if effective_dir != 0 and state.signal_count < MIN_SIGNAL_COUNT:
            effective_dir = 0

        # ── ORB MODE: additional intraday momentum check ───────────────────────
        # Market must have already moved INTRADAY_MOVE_THRESH% in signal direction
        # from the open before we enter. In low-vol, this filters for days with
        # genuine momentum vs days that are just oscillating.
        if MODE == 'orb' and effective_dir != 0:
            dir_pct = vs_open_pct if effective_dir == 1 else -vs_open_pct
            if dir_pct < INTRADAY_MOVE_THRESH:
                effective_dir = 0   # market hasn't moved enough yet — wait

        final_state = state

        # In-position reversal check (every 5 bars)
        if in_position:
            if effective_dir != 0 and effective_dir != direction_taken:
                opt_px_now = _opt_px(opt_cache, entry_opt_strike, entry_opt_side, current_hhmm)
                if opt_px_now is not None:
                    exit_opt_px = opt_px_now
                    exit_time   = current_hhmm
                    exit_reason = 'REVERSAL'
                    in_position = False
                    break
            continue

        # Not in position: look for entry
        if current_hhmm <= LAST_ENTRY_HHMM and effective_dir != 0:
            next_idx = bar_idx + 1
            if next_idx >= len(day_df):
                continue
            next_bar  = day_df.iloc[next_idx]
            next_hhmm = next_bar['ts'].strftime('%H:%M')
            if next_hhmm > LAST_ENTRY_HHMM:
                continue

            atm  = round(current_fut / NIFTY_STEP) * NIFTY_STEP
            side = 'CE' if effective_dir == 1 else 'PE'

            if MODE == 'orb':
                # ATM entry — same as main backtest
                actual_strike, opt_px = _opt_px_atm(opt_cache, atm, side, next_hhmm)

            else:  # OTM mode
                # Strike: ATM + OTM_OFFSET (CE) or ATM - OTM_OFFSET (PE)
                otm_target = atm + OTM_OFFSET if effective_dir == 1 else atm - OTM_OFFSET
                actual_strike, opt_px = _opt_px_otm(opt_cache, otm_target, side, next_hhmm,
                                                     min_premium=5.0)
                if actual_strike is None:
                    log.debug(
                        "OTM no price date=%s dir=%d otm_target=%d side=%s bar=%s",
                        td, effective_dir, otm_target, side, next_hhmm,
                    )
                    continue   # no OTM price available — skip this bar

            if actual_strike is not None and opt_px is not None and opt_px > 0:
                in_position      = True
                direction_taken  = effective_dir
                entry_opt_px     = opt_px
                entry_opt_strike = actual_strike
                entry_opt_side   = side
                entry_time       = next_hhmm
                entry_state      = state
                otm_dist         = abs(actual_strike - atm)
                log.info(
                    "ENTRY date=%s mode=%s dir=%+d strike=%d(%dpt-OTM) side=%s "
                    "px=%.1f time=%s score=%.3f vs_open=%.2f%% vol=%.3f%%",
                    td, MODE, effective_dir, actual_strike, otm_dist,
                    side, opt_px, next_hhmm, state.score, vs_open_pct, _vol_today,
                )

    # ── Collect result row ────────────────────────────────────────────────────
    traded = in_position or (exit_reason is not None)

    if not traded:
        # NO_TRADE row — still record for the CSV so we can analyse no-trade days
        results.append({
            'trade_date': str(td), 'mode': MODE, 'vol_20d': round(_vol_today, 3),
            'direction': 0, 'score': round(final_state.score, 3) if final_state else 0,
            'signal_count': final_state.signal_count if final_state else 0,
            'opt_strike': None, 'opt_side': None, 'otm_dist': None,
            'entry_time': None, 'exit_time': None, 'exit_reason': 'NO_TRADE',
            'opt_entry': None, 'opt_exit': None, 'pnl_pts': 0.0, 'pnl_inr': 0.0,
            'result': 'NO_TRADE', 'day_open': round(open_price, 1),
            'regime_5d_ret': round(regime_5d_return, 2), 'vs_open_pct': round(vs_open_pct if final_state else 0, 2),
            'orb_high': round(orb_high, 1), 'orb_low': round(orb_low, 1),
        })
        continue

    # Handle case where we entered but never got an exit bar (market data ended)
    if in_position and exit_reason is None:
        # Use last available bar's option price
        last_hhmm = day_df_hhmm['hhmm'].iloc[-1]
        last_px   = _opt_px(opt_cache, entry_opt_strike, entry_opt_side, last_hhmm)
        if last_px is not None:
            exit_opt_px = last_px
            exit_time   = last_hhmm
            exit_reason = 'EOD'
        else:
            exit_opt_px = entry_opt_px   # flat exit at entry price (no data)
            exit_time   = entry_time
            exit_reason = 'EOD_NO_DATA'
        in_position = False

    pnl_pts = (exit_opt_px - entry_opt_px) * direction_taken if exit_opt_px is not None else 0.0
    pnl_inr = pnl_pts * NIFTY_LOT
    result  = 'WIN' if pnl_pts > 0 else ('LOSS' if pnl_pts < 0 else 'BE')

    results.append({
        'trade_date': str(td), 'mode': MODE, 'vol_20d': round(_vol_today, 3),
        'direction': direction_taken,
        'score': round(entry_state.score, 3) if entry_state else 0,
        'signal_count': entry_state.signal_count if entry_state else 0,
        'opt_strike': entry_opt_strike,
        'opt_side': entry_opt_side,
        'otm_dist': abs(entry_opt_strike - round(open_price / NIFTY_STEP) * NIFTY_STEP),
        'entry_time': entry_time,
        'exit_time': exit_time,
        'exit_reason': exit_reason,
        'opt_entry': round(entry_opt_px, 1) if entry_opt_px else None,
        'opt_exit':  round(exit_opt_px, 1)  if exit_opt_px  else None,
        'pnl_pts': round(pnl_pts, 1),
        'pnl_inr': round(pnl_inr, 0),
        'result': result,
        'day_open': round(open_price, 1),
        'regime_5d_ret': round(regime_5d_return, 2),
        'vs_open_pct': round(vs_open_pct, 2),
        'orb_high': round(orb_high, 1),
        'orb_low':  round(orb_low, 1),
    })
    log.info(
        "EXIT date=%s dir=%+d px_entry=%.1f px_exit=%.1f pnl=%+.1f [%s] %s vol=%.3f%%",
        td, direction_taken, entry_opt_px, exit_opt_px, pnl_pts, exit_reason, result, _vol_today,
    )

# ── Results ───────────────────────────────────────────────────────────────────
res = pd.DataFrame(results)
res.to_csv(OUT_CSV, index=False)
print(f"\nCSV saved: {OUT_CSV}  rows={len(res)}")

trades = res[res['exit_reason'].isin(['SL', 'TP', 'EOD', 'REVERSAL', 'EOD_NO_DATA'])].copy()
if trades.empty:
    print("No trades generated.")
else:
    wins   = trades[trades['pnl_pts'] > 0]
    losses = trades[trades['pnl_pts'] < 0]
    total  = trades['pnl_pts'].sum()

    print(f"\n{'='*60}")
    print(f"NIFTY LOW-VOL [{MODE.upper()}] — vol < {MIN_VOL_PCT}% regime")
    print(f"{'='*60}")
    print(f"Trades  : {len(trades)}")
    print(f"WR      : {len(wins)}/{len(trades)} = {len(wins)/len(trades)*100:.1f}%")
    print(f"Total   : {total:+.1f} pts  =  Rs.{total * NIFTY_LOT:,.0f}")
    if len(wins) > 0:
        print(f"Avg WIN : +{wins['pnl_pts'].mean():.1f} pts")
    if len(losses) > 0:
        print(f"Avg LOSS:  {losses['pnl_pts'].mean():.1f} pts")
    if len(wins) > 0 and len(losses) > 0:
        print(f"W/L     : {abs(wins['pnl_pts'].mean() / losses['pnl_pts'].mean()):.2f}x")
        pf = wins['pnl_pts'].sum() / abs(losses['pnl_pts'].sum())
        print(f"PF      : {pf:.2f}")
    print()

    trades['month'] = pd.to_datetime(trades['trade_date']).dt.strftime('%Y-%m')
    for m, g in trades.groupby('month'):
        w  = g[g['pnl_pts'] > 0]
        lo = g[g['pnl_pts'] < 0]
        avg_vol = g['vol_20d'].mean()
        pf_m = w['pnl_pts'].sum() / abs(lo['pnl_pts'].sum()) if len(lo) > 0 and lo['pnl_pts'].sum() != 0 else float('inf')
        print(f"  {m}: {len(w)}/{len(g)} WR={len(w)/len(g)*100:.0f}%  "
              f"pts={g['pnl_pts'].sum():+.1f}  PF={pf_m:.1f}  avg_vol={avg_vol:.3f}%")

    print()
    print("Exit reasons:", trades['exit_reason'].value_counts().to_dict())
    if MODE == 'otm':
        print(f"Avg OTM distance: {trades['otm_dist'].mean():.0f} pts")
        print(f"Avg entry premium: {trades['opt_entry'].mean():.1f} pts")
