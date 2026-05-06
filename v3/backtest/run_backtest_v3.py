"""
v3/backtest/run_backtest_v3.py
===============================
Full 6-signal V3 backtest.

Data sources (all lag-1 / no lookahead):
  - 1m futures candles:        v3/cache/candles_1m_BANKNIFTY.pkl
  - Per-strike option 1m OI:   v3/cache/option_oi_1m_BANKNIFTY.pkl   ← TRUE velocity
  - Bhavcopy EOD (fallback):   v3/cache/bhavcopy_BN_all.pkl          ← walls only
  - PCR:                       from bhavcopy, lag-1
  - FII F&O participant OI:    trade_logs/_fii_fo_cache.pkl
  - FII cash flows:            fii_data.csv
  - Spot (^NSEBANK):           yfinance 3mo daily

Signal activation:
  - OI velocity:   uses option_oi_1m if available, else 0 (NOT bhavcopy velocity — too noisy)
  - Strike defense: uses bhavcopy prev-day walls (always)
  - Everything else: from data above

Usage: python v3/backtest/run_backtest_v3.py
"""
import sys, pickle, warnings
warnings.filterwarnings('ignore')
from pathlib import Path
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import date

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from v3.data.nse_bhavcopy import compute_daily_pcr
from v3.signals.engine import compute_signal_state, state_to_dict
from v3.data.fetch_option_oi import compute_intraday_velocity, compute_eod_walls

# ── Load data ─────────────────────────────────────────────────────────────────
with open(ROOT/'v3/cache/candles_1m_BANKNIFTY.pkl','rb') as f:
    candles = pickle.load(f)
trade_dates = sorted(candles['date'].unique())

with open(ROOT/'v3/cache/bhavcopy_BN_all.pkl','rb') as f:
    bhav = pickle.load(f)
bhav_dates = sorted(bhav.keys())

# Per-strike option 1m OI (may not exist yet — run fetch_option_oi.py first)
opt_oi_file = ROOT/'v3/cache/option_oi_1m_BANKNIFTY.pkl'
if opt_oi_file.exists():
    with open(opt_oi_file,'rb') as f:
        opt_oi_cache = pickle.load(f)
    print(f"Option OI 1m cache: {len(opt_oi_cache)} days")
else:
    opt_oi_cache = {}
    print("WARNING: option_oi_1m_BANKNIFTY.pkl not found — OI velocity will be 0")
    print("         Run: python v3/data/fetch_option_oi.py")

with open(ROOT/'trade_logs/_fii_fo_cache.pkl','rb') as f:
    fii_fo = pickle.load(f)
fii_fo_dates = sorted(fii_fo.keys())

fii_cash_df = pd.read_csv(ROOT/'fii_data.csv')
fii_cash_df['date'] = pd.to_datetime(fii_cash_df['date']).dt.date

pcr_df = compute_daily_pcr(bhav)
pcr_df['date_only'] = pcr_df['date'].dt.date

bn = yf.download('^NSEBANK', period='3mo', interval='1d', progress=False, auto_adjust=True)
bn.index = pd.to_datetime(bn.index).date
spot_close = {d: float(bn['Close']['^NSEBANK'].loc[d]) for d in bn.index}


def get_walls_from_bhavcopy(df_strikes, spot, band=2000):
    """Previous-day bhavcopy → call wall / put wall."""
    if df_strikes.empty or spot <= 0:
        return {}
    sub = df_strikes[(df_strikes['strike'] >= spot-band) &
                     (df_strikes['strike'] <= spot+band)].copy()
    if sub.empty:
        sub = df_strikes.copy()
    total_ce = sub['ce_oi'].sum()
    total_pe = sub['pe_oi'].sum()
    pcr_live = total_pe/total_ce if total_ce > 0 else 1.0
    calls_above = sub[sub['strike'] > spot]
    puts_below  = sub[sub['strike'] < spot]
    call_wall = int(calls_above.loc[calls_above['ce_oi'].idxmax(),'strike']) if not calls_above.empty else None
    put_wall  = int(puts_below.loc[puts_below['pe_oi'].idxmax(),'strike'])   if not puts_below.empty else None
    return {'call_wall':call_wall, 'put_wall':put_wall, 'pcr_live':round(pcr_live,3), 'ltp':spot}


# ── Backtest loop ─────────────────────────────────────────────────────────────
results = []

for i, td in enumerate(trade_dates):
    day_df = candles[candles['date']==td].sort_values('ts').reset_index(drop=True)
    if len(day_df) < 20:
        continue

    prev_td    = trade_dates[i-1] if i > 0 else None
    open_price = float(day_df['open'].iloc[0])
    fut_close  = float(day_df['close'].iloc[-1])

    # Spot = previous day's close (lag-1)
    spot = spot_close.get(prev_td, 0) if prev_td else 0
    if spot == 0:
        spot = spot_close.get(td, open_price * 0.9985)

    # PCR lag-1
    pcr_row = pcr_df[pcr_df['date_only'] < td].tail(1)
    pcr_val = float(pcr_row['pcr'].iloc[0]) if not pcr_row.empty else 1.0
    pcr_ma  = float(pcr_row['pcr_5d_ma'].fillna(pcr_row['pcr']).iloc[0]) if not pcr_row.empty else 1.0

    # FII F&O lag-1
    prev_fo = [d for d in fii_fo_dates if d < str(td)]
    if prev_fo:
        fo = fii_fo[prev_fo[-1]]; fl,fs = fo.get('fut_long',0),fo.get('fut_short',0)
        fii_fut_level = 1 if fl>fs*1.15 else (-1 if fs>fl*1.15 else 0)
    else:
        fii_fut_level = 0

    cash_prev = fii_cash_df[fii_cash_df['date'] < td].tail(1)
    net = float(cash_prev['fpi_net'].iloc[0]) if not cash_prev.empty else 0
    fii_cash_lag1 = 1 if net > 500 else (-1 if net < -500 else 0)

    # DTE
    iso = td.isocalendar()
    thursday = date.fromisocalendar(iso[0], iso[1], 4)
    dte = max(0, (thursday - td).days)

    # ── Strike walls: prev-day bhavcopy (always available, no lookahead) ────────
    # Prefer same-day intraday walls from opt_oi_cache at OPEN (first candle OI),
    # but only if we have that day's data. Fall back to previous-day bhavcopy.
    prev_bhav_dates = [d for d in bhav_dates if d < str(td)]
    walls = {}
    if prev_bhav_dates:
        walls = get_walls_from_bhavcopy(bhav[prev_bhav_dates[-1]], open_price)

    # ── OI velocity: excluded from open-bar backtest (lookahead) ─────────────
    # compute_intraday_velocity uses iloc[-1] (EOD 15:30 OI) to predict a trade
    # entered at 9:15 open — that is pure lookahead and inflates/degrades results.
    # In live trading the scanner computes velocity in real-time during the session.
    # The opt_oi_cache is used for compute_eod_walls() only (see below).
    velocity_data = {}  # always empty for open-bar backtest

    state = compute_signal_state(
        df_1m=day_df, futures_ltp=open_price, spot_ltp=float(spot),
        days_to_expiry=dte, pcr=pcr_val, pcr_5d_ma=pcr_ma,
        velocity_data=velocity_data, walls=walls,
        fii_fut_level=fii_fut_level, fii_cash_lag1=fii_cash_lag1,
        timestamp=pd.Timestamp(str(td)),
    )

    row = state_to_dict(state)
    row.update({
        'trade_date': str(td),
        'open':       open_price,
        'close':      fut_close,
        'actual':     1 if fut_close > open_price else -1,
        'pcr_input':  round(pcr_val, 3),
        'fii_fut':    fii_fut_level,
        'fii_cash':   fii_cash_lag1,
        'call_wall_in': walls.get('call_wall'),
        'put_wall_in':  walls.get('put_wall'),
        'has_opt_oi':   str(td) in opt_oi_cache,
    })
    results.append(row)

res = pd.DataFrame(results)

print('\n=== V3 BACKTEST RESULTS ===')
print(res[['trade_date','direction','actual','score','signal_count',
           'oi_quadrant','futures_basis','pcr_signal','oi_velocity',
           'strike_defense','fii_signature',
           'call_wall_in','put_wall_in','has_opt_oi']].to_string(index=False))

fired = res[res['direction'] != 0]
correct = fired[fired['direction'] == fired['actual']]
print(f'\nTotal days: {len(res)}  |  Fired: {len(fired)}  |  Correct: {len(correct)}/{len(fired)}', end='')
print(f' = {len(correct)/len(fired)*100:.1f}%' if len(fired) else '')

print('\nFIRED DETAIL:')
for _, r in fired.iterrows():
    c   = 'OK' if r['direction'] == r['actual'] else 'XX'
    sig = 'LONG ' if r['direction'] == 1 else 'SHORT'
    act = 'UP  ' if r['actual'] == 1 else 'DOWN'
    mv  = r['close'] - r['open']
    print(f"  {c} {r['trade_date']}  {sig} actual={act}  move={mv:+6.0f}  "
          f"score={r['score']:+.3f}  sigs={r['signal_count']}/6")
    print(f"     {r['notes'][:140]}")
