# ============================================================
# run_full_backtest.py — Hawala v2  Full Walk-Forward 2021-2026
# ============================================================
# Runs the complete 3-strategy pipeline (ORB + VWAP + Options ORB)
# on BANKNIFTY across every year from 2021 through April 2026.
#
# Structure
# ---------
#   Pre-IS  : 2021 – 2023  (historical, before param calibration)
#   IS      : 2024          (param calibration year)
#   OOS     : 2025          (out-of-sample)
#   OOS-YTD : 2026 Jan–Apr  (live out-of-sample)
#
# Equity compounds continuously across all years.
# Each year's starting equity = previous year's ending equity.
#
# Usage
# -----
#   python run_full_backtest.py
#
# Output
# ------
#   trade_logs/full_backtest_YYYY.csv   — per-year trade logs
#   trade_logs/full_backtest_all.csv    — combined all years
# ============================================================

import os, sys, pickle, pathlib
import pandas as pd
from dotenv import load_dotenv

load_dotenv('token.env')
TOKEN = os.getenv('GROWW_API_KEY', '').strip()
if not TOKEN:
    sys.exit("❌  GROWW_API_KEY not found in token.env")

from growwapi import GrowwAPI
groww = GrowwAPI(TOKEN)
print("✅  Groww authenticated")

from config import INSTRUMENTS, STRATEGIES
from data.fetch import fetch_instrument
from strategies.orb import run_orb
from strategies.vwap_reversion import run_vwap_reversion
from strategies.options_orb import run_options_orb
from backtest.engine import _lot_size_for_date
from backtest.compounding_engine import run_compounded, print_compounded_report

# ── Config ────────────────────────────────────────────────────────────────────
INSTRUMENT   = 'BANKNIFTY'
STARTING_CAP = 1_00_000

# Year segments to run — each is (label, start, end, period_type)
SEGMENTS = [
    ('2021',     '2021-01-01', '2021-12-31', 'PRE-IS'),
    ('2022',     '2022-01-01', '2022-12-31', 'PRE-IS'),
    ('2023',     '2023-01-01', '2023-12-31', 'PRE-IS'),
    ('2024',     '2024-01-01', '2024-12-31', 'IS'),
    ('2025',     '2025-01-01', '2025-12-31', 'OOS'),
    ('2026-YTD', '2026-01-01', '2026-04-20', 'OOS-YTD'),
]

TRADE_LOG_DIR = pathlib.Path('trade_logs')
TRADE_LOG_DIR.mkdir(exist_ok=True)

inst_cfg    = INSTRUMENTS[INSTRUMENT]
orb_params  = {**STRATEGIES['orb']['params'],             **inst_cfg.get('strategy_params', {})}
vwap_params = {**STRATEGIES['vwap_reversion']['params'],  **inst_cfg.get('strategy_params', {})}
opt_params  = {**STRATEGIES['options_orb']['params']}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_or_fetch(label: str, start: str, end: str) -> pd.DataFrame:
    cache = TRADE_LOG_DIR / f'_data_cache_{INSTRUMENT}_{start}_{end}.pkl'
    if cache.exists():
        print(f"  📦 Cache hit: {cache.name}")
        with open(cache, 'rb') as f:
            data = pickle.load(f)
        print(f"     {len(data):,} candles  "
              f"({data.index[0].date()} → {data.index[-1].date()})")
        return data

    print(f"  🌐 Fetching {INSTRUMENT} futures {start} → {end} ...")
    data = fetch_instrument(INSTRUMENT, start, end, groww=groww, use_futures=True)
    if data.empty:
        print(f"  ❌  No data returned for {label} — skipping segment.")
        return pd.DataFrame()
    with open(cache, 'wb') as f:
        pickle.dump(data, f)
    print(f"  💾 Cached → {cache.name}  ({len(data):,} candles)")
    return data


def _run_strategies(data: pd.DataFrame, label: str) -> pd.DataFrame:
    print(f"  → Futures ORB ...")
    orb_log  = run_orb(data, inst_cfg, orb_params)
    print(f"     {len(orb_log)} trades")

    print(f"  → VWAP Reversion ...")
    vwap_log = run_vwap_reversion(data, inst_cfg, vwap_params)
    print(f"     {len(vwap_log)} trades")

    print(f"  → Options ORB ...")
    opt_log  = run_options_orb(data, inst_cfg, opt_params, groww=groww)
    print(f"     {len(opt_log)} trades")

    frames = [df for df in [orb_log, vwap_log, opt_log] if not df.empty]
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    if 'entry_ts' in combined.columns and 'entry_time' not in combined.columns:
        combined = combined.rename(columns={'entry_ts': 'entry_time',
                                             'exit_ts':  'exit_time'})
    if 'date' not in combined.columns:
        combined['date'] = pd.to_datetime(combined['entry_time']).dt.date
    if 'year' not in combined.columns:
        combined['year'] = pd.to_datetime(combined['entry_time']).dt.year
    if 'margin_per_lot' not in combined.columns:
        combined['margin_per_lot'] = inst_cfg['margin_per_lot']

    if 'Contract' in data.columns:
        tmp = data.copy()
        tmp['_date'] = tmp.index.date
        date_to_contract = tmp.groupby('_date')['Contract'].first()
        combined['contract'] = combined['date'].map(
            lambda d: date_to_contract.get(d, ''))
    else:
        combined['contract'] = ''

    opt_mask = combined['strategy'] == 'OPT_ORB'
    if opt_mask.any() and 'expiry' in combined.columns:
        combined.loc[opt_mask, 'contract'] = combined.loc[opt_mask].apply(
            lambda r: (f"NSE-BANKNIFTY-{r['expiry']}-"
                       f"{int(r['atm_strike']) if pd.notna(r.get('atm_strike')) else '?'}"
                       f"-{r.get('opt_type','?')}"), axis=1)

    combined['strike'] = combined.get(
        'atm_strike', pd.Series('FUT', index=combined.index))
    combined['strike'] = combined['strike'].fillna('FUT')
    combined = combined.sort_values('entry_time').reset_index(drop=True)
    return combined


def _save(df: pd.DataFrame, name: str) -> None:
    path = TRADE_LOG_DIR / f'{name}.csv'
    out  = df.copy()
    out.insert(0, 'trade_id', range(1, len(out) + 1))

    lots      = out.get('lots_at_entry', pd.Series(1, index=out.index)).fillna(1)
    lot_sizes = out.apply(
        lambda r: _lot_size_for_date(r['date'], inst_cfg), axis=1)

    entry_price = out['entry'].copy()
    exit_price  = out['exit_price'].copy()
    opt_mask    = out['strategy'] == 'OPT_ORB'
    if opt_mask.any() and 'premium_entry' in out.columns:
        entry_price[opt_mask] = out.loc[opt_mask, 'premium_entry']
        exit_price[opt_mask]  = out.loc[opt_mask, 'premium_exit']

    out['entry_value'] = (entry_price * lot_sizes * lots).round(2)
    out['exit_value']  = (exit_price  * lot_sizes * lots).round(2)

    priority = ['trade_id','date','period','strategy','contract','strike',
                'direction','entry','entry_value','exit_price','exit_value',
                'pnl_rs','exit_reason','lots_at_entry','capital_at_entry','equity_after']
    cols = priority + [c for c in out.columns if c not in priority]
    out  = out[[c for c in cols if c in out.columns]]
    out.to_csv(path, index=False)
    print(f"  📝 Saved {len(out)} trades → {path.name}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN WALK-FORWARD LOOP
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "█"*64)
print("  HAWALA v2 — FULL WALK-FORWARD  2021 → 2026")
print(f"  Starting capital: ₹{STARTING_CAP:,.0f}")
print("█"*64)

current_equity = float(STARTING_CAP)
all_enriched   = []
year_summary   = []   # rows for the final summary table

for label, start, end, period in SEGMENTS:
    print(f"\n{'='*64}")
    print(f"  {label}  [{period}]  ({start} → {end})")
    print(f"  Starting equity: ₹{current_equity:,.0f}")
    print(f"{'='*64}")

    data = _load_or_fetch(label, start, end)
    if data.empty:
        year_summary.append({
            'label': label, 'period': period,
            'start_eq': current_equity, 'end_eq': current_equity,
            'trades': 0, 'wr': 0, 'ret_pct': 0, 'max_dd': 0,
        })
        continue

    tl = _run_strategies(data, label)
    if tl.empty:
        print(f"  ⚠  No trades generated for {label}")
        year_summary.append({
            'label': label, 'period': period,
            'start_eq': current_equity, 'end_eq': current_equity,
            'trades': 0, 'wr': 0, 'ret_pct': 0, 'max_dd': 0,
        })
        continue

    tl['period'] = period

    enriched, ec, summary = run_compounded(
        tl, inst_cfg, starting_capital=current_equity)
    print_compounded_report(enriched, summary, title=f"{label} [{period}]")

    end_equity = float(enriched['equity_after'].iloc[-1]) if not enriched.empty else current_equity
    ret_pct    = (end_equity / current_equity - 1) * 100

    print(f"\n  ★  {label} END EQUITY : ₹{end_equity:,.0f}  ({ret_pct:+.1f}%)")

    year_summary.append({
        'label':    label,
        'period':   period,
        'start_eq': current_equity,
        'end_eq':   end_equity,
        'trades':   summary.get('trades', 0),
        'wr':       summary.get('win_rate', 0),
        'ret_pct':  ret_pct,
        'max_dd':   summary.get('max_dd_pct', 0),
    })

    enriched['period'] = period
    all_enriched.append(enriched)

    # Save per-year CSV
    _save(enriched, f'full_backtest_{label}')

    current_equity = end_equity


# ── Grand summary table ───────────────────────────────────────────────────────
print("\n\n" + "█"*64)
print("  GRAND SUMMARY — 2021 → 2026")
print("█"*64)
print(f"\n  {'Year':<10} {'Period':<9} {'Trades':>7} {'WR%':>6} "
      f"{'Start Eq':>12} {'End Eq':>12} {'Return':>8} {'MaxDD':>7}")
print(f"  {'─'*74}")

for row in year_summary:
    print(f"  {row['label']:<10} {row['period']:<9} {row['trades']:>7} "
          f"{row['wr']:>5.1f}%  "
          f"₹{row['start_eq']:>10,.0f}  ₹{row['end_eq']:>10,.0f}  "
          f"{row['ret_pct']:>6.1f}%  {row['max_dd']:>5.1f}%")

print(f"  {'─'*74}")
total_ret = (current_equity / STARTING_CAP - 1) * 100
print(f"\n  ₹{STARTING_CAP:,.0f}  →  ₹{current_equity:,.0f}  "
      f"({total_ret:+.1f}% total, ~{total_ret/5.3:.1f}% CAGR over 5.3 yrs)")

# ── Save combined all-years log ───────────────────────────────────────────────
if all_enriched:
    all_trades = pd.concat(all_enriched, ignore_index=True)
    if 'trade_id' in all_trades.columns:
        all_trades = all_trades.drop(columns=['trade_id'])
    all_trades.insert(0, 'trade_id', range(1, len(all_trades) + 1))
    _save(all_trades, 'full_backtest_all')
    print(f"\n  Total trades 2021-2026: {len(all_trades)}")

print("\n✅  Full walk-forward complete.")
