"""
v3/backtest/run_backtest_sensex.py
====================================
Full 6-signal V3 backtest adapted for SENSEX + 3 extra signals.

Data sources (all lag-1 / no lookahead):
  - 1m futures candles:        v3/cache/candles_1m_SENSEX.pkl
  - 1m Nifty candles:          v3/cache/candles_1m_NIFTY.pkl   (leader for cross-index)
  - Per-strike option 1m OI:   v3/cache/option_oi_1m_SENSEX.pkl
  - Bhavcopy EOD:              v3/cache/bhavcopy_SENSEX_all.pkl  (WARNING if missing)
  - PCR:                       from bhavcopy, lag-1
  - FII F&O participant OI:    trade_logs/_fii_fo_cache.pkl
  - FII cash flows:            fii_data.csv
  - Spot (^BSESN):             yfinance 3mo daily

DTE: nearest THURSDAY expiry (Sensex weekly = Thursday)
Exchange: BSE (bhavcopy is BSE, not NSE)

Extra signals (wired in this version):
  7. max_pain        — prev-day bhavcopy OI gravity toward max pain. PRE-MARKET (no lookahead).
  8. expiry_reversal — intraday reversal at max pain on Thursday. INTRADAY (current_bar_idx=EOD).
  9. cross_index     — Nifty as leader, Sensex as lagger. INTRADAY (current_bar_idx=EOD).

Note: expiry_reversal and cross_index use current_bar_idx=len(day_df)-1 (EOD bar).
      They are post-hoc intraday checks, not pre-market signals.  Use them to verify
      signal quality and understand which days had intraday confirmation.

Bhavcopy: if bhavcopy_SENSEX_all.pkl doesn't exist, sets pcr_val=1.0 and walls={}
          with a clear WARNING printed. Does NOT silently swallow this.

Usage: python v3/backtest/run_backtest_sensex.py
"""
import sys, pickle, warnings, logging
warnings.filterwarnings('ignore')
from pathlib import Path
from datetime import date

import pandas as pd
import numpy as np
import yfinance as yf

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('backtest_sensex')

from v3.signals.engine import compute_signal_state, state_to_dict
from v3.data.fetch_option_oi import compute_intraday_velocity, compute_eod_walls


def _derive_pcr_walls_from_oi_cache(oi_cache: dict) -> tuple:
    """
    Derive bhavcopy-equivalent PCR series and per-day strike DataFrames
    from the option_oi_1m_*.pkl cache (EOD last-candle OI per strike).
    BSE bhavcopy is not publicly scrapable, so this is the primary source
    for Sensex PCR and strike walls.

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
candle_file = ROOT / 'v3/cache/candles_1m_SENSEX.pkl'
if not candle_file.exists():
    raise FileNotFoundError(
        f"SENSEX candle cache not found: {candle_file}. "
        f"Run v3/data/fetch_1m_SENSEX.py first."
    )
with open(candle_file, 'rb') as f:
    candles = pickle.load(f)
trade_dates = sorted(candles['date'].unique())
print(f"SENSEX candles: {len(candles)} rows, {len(trade_dates)} trading days")

# Nifty 1m candles — used as leader index for cross_index signal
nifty_candle_file = ROOT / 'v3/cache/candles_1m_NIFTY.pkl'
if nifty_candle_file.exists():
    with open(nifty_candle_file, 'rb') as f:
        nifty_candles = pickle.load(f)
    print(f"NIFTY leader candles: {len(nifty_candles)} rows ({nifty_candles['date'].nunique()} days) for cross-index")
else:
    nifty_candles = pd.DataFrame()
    print("WARNING: candles_1m_NIFTY.pkl not found — cross_index signal disabled")

# Bhavcopy for SENSEX (BSE, not NSE) — if missing, warn loudly
bhav_file = ROOT / 'v3/cache/bhavcopy_SENSEX_all.pkl'
_BHAVCOPY_MISSING = False
bhav = {}
bhav_dates = []
pcr_df = pd.DataFrame(columns=['date', 'pcr', 'pcr_5d_ma'])

if bhav_file.exists():
    with open(bhav_file, 'rb') as f:
        bhav = pickle.load(f)
    bhav_dates = sorted(bhav.keys())
    print(f"SENSEX bhavcopy (BSE): {len(bhav_dates)} dates")

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
        "\nNOTE: bhavcopy_SENSEX_all.pkl not found. BSE bhavcopy is not publicly "
        "scrapable. Falling back to option OI cache for PCR + walls (EOD, lag-1).\n"
    )

# Per-strike option 1m OI
opt_oi_file = ROOT / 'v3/cache/option_oi_1m_SENSEX.pkl'
if opt_oi_file.exists():
    with open(opt_oi_file, 'rb') as f:
        opt_oi_cache = pickle.load(f)
    print(f"SENSEX option OI 1m cache: {len(opt_oi_cache)} days")
else:
    opt_oi_cache = {}
    print("WARNING: option_oi_1m_SENSEX.pkl not found — OI velocity will be 0")
    print("         Run: python v3/data/fetch_option_oi_SENSEX.py")

# Derive PCR + walls from OI cache (primary source for Sensex — BSE bhavcopy not available)
if _BHAVCOPY_MISSING and opt_oi_cache:
    bhav, pcr_df = _derive_pcr_walls_from_oi_cache(opt_oi_cache)
    bhav_dates   = sorted(bhav.keys())
    _BHAVCOPY_MISSING = False
    print(f"[OI-CACHE PCR/WALLS] Derived from option OI for {len(bhav)} days.")

with open(ROOT / 'trade_logs/_fii_fo_cache.pkl', 'rb') as f:
    fii_fo = pickle.load(f)
fii_fo_dates = sorted(fii_fo.keys())

fii_cash_df = pd.read_csv(ROOT / 'fii_data.csv')
fii_cash_df['date'] = pd.to_datetime(fii_cash_df['date']).dt.date

sensex = yf.download('^BSESN', period='3mo', interval='1d', progress=False, auto_adjust=True)
sensex.index = pd.to_datetime(sensex.index).date
spot_close = {d: float(sensex['Close']['^BSESN'].loc[d]) for d in sensex.index}
_spot_dates_sorted = sorted(spot_close.keys())


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_walls_from_bhavcopy(df_strikes, spot, band=2000):
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


def _get_thursday_dte(trade_d: date) -> int:
    """
    Return days to next Thursday expiry on or after trade_d.
    Sensex weekly options expire on Thursdays.
    """
    days_ahead = (3 - trade_d.weekday()) % 7   # 3 = Thursday
    return days_ahead


# ── Backtest loop ─────────────────────────────────────────────────────────────
results = []

for i, td in enumerate(trade_dates):
    day_df = candles[candles['date'] == td].sort_values('ts').reset_index(drop=True)
    if len(day_df) < 20:
        continue

    prev_td    = trade_dates[i - 1] if i > 0 else None
    open_price = float(day_df['open'].iloc[0])
    fut_close  = float(day_df['close'].iloc[-1])

    prior_spot_dates = [d for d in _spot_dates_sorted if d < td]
    spot = spot_close[prior_spot_dates[-1]] if prior_spot_dates else spot_close.get(td, open_price * 0.9985)

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

    # DTE: nearest Thursday expiry
    dte = _get_thursday_dte(td)

    # Strike walls + df_strikes for max_pain: prev-day bhavcopy
    walls       = {}
    df_strikes  = None
    if not _BHAVCOPY_MISSING and bhav_dates:
        prev_bhav_dates = [d for d in bhav_dates if d < str(td)]
        if prev_bhav_dates:
            df_strikes = bhav[prev_bhav_dates[-1]]
            walls = get_walls_from_bhavcopy(df_strikes, open_price)

    velocity_data = {}  # open-bar backtest: no lookahead OI velocity

    # is_expiry: Sensex weekly expires on Thursday
    is_expiry = (td.weekday() == 3)

    # leader_candles: Nifty 1m for that day (cross_index signal)
    leader_df = None
    if not nifty_candles.empty:
        _leader = nifty_candles[nifty_candles['date'] == td].sort_values('ts').reset_index(drop=True)
        if len(_leader) >= 20:
            leader_df = _leader

    # current_bar_idx: use bar 60 (≈10:15 AM) for cross_index + expiry_reversal.
    # Using EOD bar (len-1) for cross_index is WRONG: by 15:30 both Nifty and Sensex
    # have fully moved, so there's no real lag to detect. The signal is designed for
    # mid-morning (bars 40-80) where one index has bounced and the other hasn't yet.
    # Bar 60 = 9:15 + 60min = 10:15 AM — sweet spot for lag detection.
    # Capped at len(day_df)-1 in case the day has fewer bars (short session/halt).
    current_bar_idx = min(60, len(day_df) - 1)

    state = compute_signal_state(
        df_1m=day_df, futures_ltp=open_price, spot_ltp=float(spot),
        days_to_expiry=dte, pcr=pcr_val, pcr_5d_ma=pcr_ma,
        velocity_data=velocity_data, walls=walls,
        fii_fut_level=fii_fut_level, fii_cash_lag1=fii_cash_lag1,
        timestamp=pd.Timestamp(str(td)),
        df_strikes=df_strikes,
        leader_candles=leader_df,
        is_expiry=is_expiry,
        current_bar_idx=current_bar_idx,
    )

    row = state_to_dict(state)
    row.update({
        'trade_date':  str(td),
        'open':        open_price,
        'close':       fut_close,
        'actual':      1 if fut_close > open_price else -1,
        'pcr_input':   round(pcr_val, 3),
        'fii_fut':     fii_fut_level,
        'fii_cash':    fii_cash_lag1,
        'call_wall_in': walls.get('call_wall'),
        'put_wall_in':  walls.get('put_wall'),
        'has_opt_oi':   str(td) in opt_oi_cache,
        'dte':          dte,
        'is_expiry':    is_expiry,
        'has_leader':   leader_df is not None,
        'has_strikes':  df_strikes is not None,
    })
    results.append(row)

res = pd.DataFrame(results)

# ── Output ────────────────────────────────────────────────────────────────────
if _BHAVCOPY_MISSING:
    print(
        "\n[DEGRADED MODE] bhavcopy_SENSEX_all.pkl missing — "
        "PCR=1.0, walls={} for all days. Results will underperform.\n"
    )

print('\n=== SENSEX V3 BACKTEST RESULTS (core signals) ===')
print(res[[
    'trade_date', 'direction', 'actual', 'score', 'signal_count',
    'oi_quadrant', 'futures_basis', 'pcr_signal', 'oi_velocity',
    'strike_defense', 'fii_signature',
    'call_wall_in', 'put_wall_in', 'has_opt_oi', 'dte', 'is_expiry',
]].to_string(index=False))

print('\n=== EXTRA SIGNALS (max_pain=pre-mkt; expiry_reversal+cross_index=intraday EOD) ===')
print(res[[
    'trade_date', 'direction', 'actual',
    'max_pain', 'max_pain_conf',
    'expiry_reversal', 'expiry_reversal_conf',
    'cross_index', 'cross_index_conf',
    'is_expiry', 'has_leader', 'has_strikes',
]].to_string(index=False))

fired   = res[res['direction'] != 0]
correct = fired[fired['direction'] == fired['actual']]
print(f'\nTotal days: {len(res)}  |  Fired: {len(fired)}  |  Correct: {len(correct)}/{len(fired)}', end='')
print(f' = {len(correct)/len(fired)*100:.1f}%' if len(fired) else '')

# Extra signal contribution on fired days
print('\nEXTRA SIGNAL BREAKDOWN on fired days:')
for _, r in fired.iterrows():
    c    = 'OK' if r['direction'] == r['actual'] else 'XX'
    sig  = 'LONG ' if r['direction'] == 1 else 'SHORT'
    act  = 'UP  '  if r['actual'] == 1  else 'DOWN'
    mv   = r['close'] - r['open']
    exp  = 'EXP' if r['is_expiry'] else '   '
    mp   = f"mp={r['max_pain']:+d}({r['max_pain_conf']:.2f})" if r['max_pain'] != 0 else 'mp=0'
    er   = f"er={r['expiry_reversal']:+d}({r['expiry_reversal_conf']:.2f})" if r['expiry_reversal'] != 0 else 'er=0'
    ci   = f"ci={r['cross_index']:+d}({r['cross_index_conf']:.2f})" if r['cross_index'] != 0 else 'ci=0'
    print(f"  {c} {r['trade_date']} {exp}  {sig} actual={act}  move={mv:+6.0f}  "
          f"score={r['score']:+.3f}  sigs={r['signal_count']}  {mp}  {er}  {ci}")
    print(f"     {r['notes'][:160]}")
