"""
v3/backtest/options_backtest.py
================================
BankNifty options-buying simulation layered on top of the V3 signal backtest.

Signal source: same as run_backtest_v3.py (6-signal engine, open-bar, lag-1).
Options layer:
  - LONG signal  → buy 1 lot ATM CE at 9:15 open
  - SHORT signal → buy 1 lot ATM PE at 9:15 open
  - ATM = strike closest to futures open price, rounded to nearest 100
  - Exit at day close (last available candle ≤ 15:30)
  - DTE filter: skip trade if DTE < 10 (near-expiry theta trap)

Price data caveat:
  option_oi_1m_BANKNIFTY.pkl stores [ts, oi] ONLY — no price columns.
  Option prices are NOT cached yet. Trades are marked PRICE_NOT_CACHED
  and the required strike/direction/expiry details are printed so the
  user knows what to fetch next.

Output:
  1. Full direction accuracy table (same format as run_backtest_v3.py)
  2. Options layer trade list with status
  3. Summary stats (direction accuracy, trade count, skip reasons)

Usage: python v3/backtest/options_backtest.py
"""
import sys, pickle, warnings
warnings.filterwarnings('ignore')
from datetime import date
from pathlib import Path

import pandas as pd
import numpy as np
import yfinance as yf

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from v3.data.nse_bhavcopy import compute_daily_pcr
from v3.signals.engine import compute_signal_state, state_to_dict
from v3.data.fetch_option_oi import compute_intraday_velocity, compute_eod_walls

LOT_SIZE = 15   # BankNifty lot size
STRIKE_STEP = 100
DTE_MIN = 10    # skip trades with DTE < 10

# ── Load data ─────────────────────────────────────────────────────────────────
with open(ROOT / 'v3/cache/candles_1m_BANKNIFTY.pkl', 'rb') as f:
    candles = pickle.load(f)
trade_dates = sorted(candles['date'].unique())

with open(ROOT / 'v3/cache/bhavcopy_BN_all.pkl', 'rb') as f:
    bhav = pickle.load(f)
bhav_dates = sorted(bhav.keys())

opt_oi_file = ROOT / 'v3/cache/option_oi_1m_BANKNIFTY.pkl'
if opt_oi_file.exists():
    with open(opt_oi_file, 'rb') as f:
        opt_oi_cache = pickle.load(f)
    print(f"Option OI 1m cache: {len(opt_oi_cache)} days")
else:
    opt_oi_cache = {}
    print("WARNING: option_oi_1m_BANKNIFTY.pkl not found — OI velocity will be 0")
    print("         Run: python v3/data/fetch_option_oi.py")

with open(ROOT / 'trade_logs/_fii_fo_cache.pkl', 'rb') as f:
    fii_fo = pickle.load(f)
fii_fo_dates = sorted(fii_fo.keys())

fii_cash_df = pd.read_csv(ROOT / 'fii_data.csv')
fii_cash_df['date'] = pd.to_datetime(fii_cash_df['date']).dt.date

pcr_df = compute_daily_pcr(bhav)
pcr_df['date_only'] = pcr_df['date'].dt.date

bn = yf.download('^NSEBANK', period='3mo', interval='1d', progress=False, auto_adjust=True)
bn.index = pd.to_datetime(bn.index).date
spot_close = {d: float(bn['Close']['^NSEBANK'].loc[d]) for d in bn.index}


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


def _get_monthly_expiry_dte(trade_d: date) -> tuple[str, int]:
    """
    Return (expiry_str, DTE) for the nearest BankNifty monthly expiry
    using the already-loaded bhav cache (no Groww call needed here).

    BankNifty monthly expiry = last Thursday of the month.
    We determine expiry from the bhavcopy dates that are present in bhav,
    or by computing the last Thursday of the nearest month.
    """
    # Compute last Thursday of current month, then next month if needed
    def last_thursday(y: int, m: int) -> date:
        # last day of month
        if m == 12:
            last = date(y + 1, 1, 1) - pd.Timedelta(days=1)
        else:
            last = date(y, m + 1, 1) - pd.Timedelta(days=1)
        last = last.date() if hasattr(last, 'date') else last
        # step back to Thursday (weekday=3)
        offset = (last.weekday() - 3) % 7
        return last - pd.Timedelta(days=offset)

    y, m = trade_d.year, trade_d.month
    for offset in range(3):
        mm = m + offset
        yy = y
        if mm > 12:
            mm -= 12
            yy += 1
        exp = last_thursday(yy, mm)
        if exp >= trade_d:
            dte = (exp - trade_d).days
            return str(exp), dte

    raise RuntimeError(
        f"Could not find BankNifty monthly expiry for {trade_d}"
    )


def atm_strike(open_price: float) -> int:
    """Round to nearest STRIKE_STEP."""
    return round(open_price / STRIKE_STEP) * STRIKE_STEP


# ── Backtest loop ─────────────────────────────────────────────────────────────
results = []       # direction accuracy rows
opt_trades = []    # options layer rows

for i, td in enumerate(trade_dates):
    day_df = candles[candles['date'] == td].sort_values('ts').reset_index(drop=True)
    if len(day_df) < 20:
        continue

    prev_td    = trade_dates[i - 1] if i > 0 else None
    open_price = float(day_df['open'].iloc[0])
    fut_close  = float(day_df['close'].iloc[-1])

    spot = spot_close.get(prev_td, 0) if prev_td else 0
    if spot == 0:
        spot = spot_close.get(td, open_price * 0.9985)

    pcr_row = pcr_df[pcr_df['date_only'] < td].tail(1)
    pcr_val = float(pcr_row['pcr'].iloc[0]) if not pcr_row.empty else 1.0
    pcr_ma  = float(pcr_row['pcr_5d_ma'].fillna(pcr_row['pcr']).iloc[0]) if not pcr_row.empty else 1.0

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

    # DTE: use monthly expiry logic (BankNifty = monthly)
    expiry_str, dte = _get_monthly_expiry_dte(td)

    prev_bhav_dates = [d for d in bhav_dates if d < str(td)]
    walls = {}
    if prev_bhav_dates:
        walls = get_walls_from_bhavcopy(bhav[prev_bhav_dates[-1]], open_price)

    velocity_data = {}  # open-bar backtest: no lookahead OI velocity

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
        'dte':          dte,
        'expiry':       expiry_str,
    })
    results.append(row)

    # ── Options layer (only for fired signals) ────────────────────────────────
    if state.direction == 0:
        continue   # no signal — no options trade

    opt_type   = 'CE' if state.direction == 1 else 'PE'
    strike     = atm_strike(open_price)
    direction_label = 'LONG' if state.direction == 1 else 'SHORT'

    trade_row = {
        'date':        str(td),
        'direction':   direction_label,
        'atm_strike':  strike,
        'option_type': opt_type,
        'dte':         dte,
        'expiry':      expiry_str,
        'open_futures': round(open_price, 2),
        'entry_price': None,
        'exit_price':  None,
        'pnl':         None,
        'status':      None,
    }

    # DTE filter
    if dte < DTE_MIN:
        trade_row['status'] = f'SKIPPED_DTE_LT_{DTE_MIN}'
        opt_trades.append(trade_row)
        continue

    # Check OI cache for price availability
    # The OI cache only has [ts, oi] — no LTP/price columns.
    # We cannot compute P&L without option prices.
    trade_row['status'] = 'PRICE_NOT_CACHED'
    opt_trades.append(trade_row)

# ── Build DataFrames ──────────────────────────────────────────────────────────
res      = pd.DataFrame(results)
opt_df   = pd.DataFrame(opt_trades) if opt_trades else pd.DataFrame()

# ── Section 1: Direction accuracy (identical format to run_backtest_v3.py) ────
print('\n' + '=' * 70)
print('=== V3 BANKNIFTY BACKTEST — DIRECTION ACCURACY ===')
print('=' * 70)
print(res[[
    'trade_date', 'direction', 'actual', 'score', 'signal_count',
    'oi_quadrant', 'futures_basis', 'pcr_signal', 'oi_velocity',
    'strike_defense', 'fii_signature',
    'call_wall_in', 'put_wall_in', 'has_opt_oi',
]].to_string(index=False))

fired   = res[res['direction'] != 0]
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
          f"score={r['score']:+.3f}  sigs={r['signal_count']}/6  "
          f"DTE={r['dte']}  expiry={r['expiry']}")
    print(f"     {r['notes'][:140]}")

# ── Section 2: Options layer ──────────────────────────────────────────────────
print('\n' + '=' * 70)
print('=== OPTIONS BUYING LAYER ===')
print('=' * 70)
print("""
NOTE: option_oi_1m_BANKNIFTY.pkl stores [ts, oi] columns ONLY.
      Option LTP/price data is NOT cached.
      All trades below are marked PRICE_NOT_CACHED.
      To compute real P&L, run a separate fetch for ATM option OHLC candles
      for the strikes listed below.
""")

if not opt_df.empty:
    print(opt_df[[
        'date', 'direction', 'atm_strike', 'option_type',
        'dte', 'expiry', 'open_futures', 'status',
    ]].to_string(index=False))

    skipped_dte   = opt_df[opt_df['status'].str.startswith('SKIPPED_DTE', na=False)]
    need_price    = opt_df[opt_df['status'] == 'PRICE_NOT_CACHED']

    print(f'\nTotal signal fires: {len(opt_df)}')
    print(f'  Skipped (DTE < {DTE_MIN}): {len(skipped_dte)}')
    print(f'  PRICE_NOT_CACHED (need fetch): {len(need_price)}')

    if not need_price.empty:
        print('\nStrikes that need option price fetch:')
        for _, r in need_price.iterrows():
            print(f"  {r['date']}  {r['option_type']}  strike={r['atm_strike']}  "
                  f"expiry={r['expiry']}  DTE={r['dte']}")
else:
    print("No option trades (no signals fired in backtest period).")

# ── Section 3: Summary ────────────────────────────────────────────────────────
print('\n' + '=' * 70)
print('=== SUMMARY ===')
print('=' * 70)
print(f"Backtest period : {res['trade_date'].iloc[0]}  →  {res['trade_date'].iloc[-1]}")
print(f"Total days      : {len(res)}")
print(f"Signals fired   : {len(fired)} / {len(res)}")
if len(fired):
    print(f"Direction acc   : {len(correct)}/{len(fired)} = {len(correct)/len(fired)*100:.1f}%")
if not opt_df.empty:
    print(f"Option trades   : {len(opt_df)}")
    print(f"  DTE-skipped   : {len(opt_df[opt_df['status'].str.startswith('SKIPPED_DTE', na=False)])}")
    print(f"  Need price    : {len(opt_df[opt_df['status'] == 'PRICE_NOT_CACHED'])}")
print(f"Lot size        : {LOT_SIZE}")
print(f"DTE filter      : skip if DTE < {DTE_MIN}")
print("\nTo compute actual options P&L:")
print("  1. Fetch 1m OHLC candles for each strike/expiry listed above")
print("  2. Re-run with prices populated in the cache")
