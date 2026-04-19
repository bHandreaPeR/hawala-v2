# ============================================================
# run_baseline.py — Hawala v2 ORB + VWAP Baseline Runner
# ============================================================
# Runs the two proven strategies on BANKNIFTY:
#   - ORB (Opening Range Breakout) on gap days
#   - VWAP Reversion on no-gap / low-gap days
#
# IS  : 2022-01-01 → 2024-12-31  (train + validation)
# OOS : 2025-01-01 → 2025-12-31  (true out-of-sample)
#
# Output
# ------
# trade_logs/baseline_IS_2022_2024.csv     — all IS trades
# trade_logs/baseline_OOS_2025.csv         — all OOS trades
# trade_logs/baseline_combined_all.csv     — IS + OOS together
#
# Compounding model
# -----------------
# Equity updates after every trade exit.
# OOS starts from the equity achieved at end of 2024.
# ORB and VWAP are mutually exclusive by design
# (ORB fires on gap days, VWAP on no-gap days), so
# sequential compounding is exact — no concurrent-trade overlap.
# ============================================================

import os, sys, math, pickle, pathlib
import numpy as np
import pandas as pd
from datetime import date as dt_date
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

# ── Constants ──────────────────────────────────────────────────────────────────
INSTRUMENT    = 'BANKNIFTY'
IS_START      = '2024-01-01'
IS_END        = '2024-12-31'
OOS_START     = '2025-01-01'
OOS_END       = '2025-12-31'
STARTING_CAP  = 1_00_000

TRADE_LOG_DIR = pathlib.Path('trade_logs')
TRADE_LOG_DIR.mkdir(exist_ok=True)

IS_CACHE  = TRADE_LOG_DIR / f'_data_cache_{INSTRUMENT}_2024-01-01_2024-12-31.pkl'
OOS_CACHE = TRADE_LOG_DIR / f'_data_cache_{INSTRUMENT}_{OOS_START}_{OOS_END}.pkl'

inst_cfg    = INSTRUMENTS[INSTRUMENT]
orb_params  = {**STRATEGIES['orb']['params'],  **inst_cfg.get('strategy_params', {})}
vwap_params = {**STRATEGIES['vwap_reversion']['params'], **inst_cfg.get('strategy_params', {})}
opt_params  = {**STRATEGIES['options_orb']['params']}


# ── Data loading helpers ────────────────────────────────────────────────────────

def _load_or_fetch(cache_path: pathlib.Path, start: str, end: str) -> pd.DataFrame:
    if cache_path.exists():
        print(f"📦  Loading cache: {cache_path.name}")
        with open(cache_path, 'rb') as f:
            data = pickle.load(f)
        print(f"    {len(data):,} candles  "
              f"({data.index[0].date()} → {data.index[-1].date()})")
    else:
        print(f"🌐  Fetching {INSTRUMENT} futures {start} → {end} ...")
        data = fetch_instrument(INSTRUMENT, start, end,
                                groww=groww, use_futures=True)
        if data.empty:
            print("❌  No data returned — aborting.")
            sys.exit(1)
        with open(cache_path, 'wb') as f:
            pickle.dump(data, f)
        print(f"💾  Cached to {cache_path.name}  ({len(data):,} candles)")
    return data


# ── Strategy runner ─────────────────────────────────────────────────────────────

def _run_strategies(data: pd.DataFrame, label: str) -> pd.DataFrame:
    """Run 3-strategy pipeline: Futures ORB + Options ORB + VWAP."""
    print(f"\n── {label}: Running Futures ORB (gap 50-100 pts, Tue/Wed/Fri) ──")
    orb_log = run_orb(data, inst_cfg, orb_params)
    print(f"    Futures ORB: {len(orb_log)} trades")

    print(f"── {label}: Running VWAP Reversion (no-gap days) ──")
    vwap_log = run_vwap_reversion(data, inst_cfg, vwap_params)
    print(f"    VWAP: {len(vwap_log)} trades")

    print(f"── {label}: Running Options ORB (gap >100 pts, Tue/Wed/Fri) ──")
    opt_log = run_options_orb(data, inst_cfg, opt_params, groww=groww)
    print(f"    Options ORB: {len(opt_log)} trades")

    frames = [df for df in [orb_log, vwap_log, opt_log] if not df.empty]
    if not frames:
        print(f"  ⚠ No trades in {label}")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Normalise timestamp column names (ORB/VWAP use entry_ts / exit_ts)
    if 'entry_ts' in combined.columns and 'entry_time' not in combined.columns:
        combined = combined.rename(columns={'entry_ts': 'entry_time',
                                             'exit_ts':  'exit_time'})

    # Ensure date and year columns
    if 'date' not in combined.columns:
        combined['date'] = pd.to_datetime(combined['entry_time']).dt.date
    if 'year' not in combined.columns:
        combined['year'] = pd.to_datetime(combined['entry_time']).dt.year

    # Ensure margin_per_lot column (used by compounding engine)
    if 'margin_per_lot' not in combined.columns:
        combined['margin_per_lot'] = inst_cfg['margin_per_lot']

    # ── Add futures contract name for ORB / VWAP rows ─────────────────────────
    if 'Contract' in data.columns:
        tmp = data.copy()
        tmp['_date'] = tmp.index.date
        date_to_contract = tmp.groupby('_date')['Contract'].first()
        combined['contract'] = combined['date'].map(
            lambda d: date_to_contract.get(d, '')
        )
    else:
        combined['contract'] = ''

    # Options ORB: overwrite contract with full option symbol
    opt_mask = combined['strategy'] == 'OPT_ORB'
    if opt_mask.any() and 'expiry' in combined.columns:
        combined.loc[opt_mask, 'contract'] = combined.loc[opt_mask].apply(
            lambda r: f"NSE-BANKNIFTY-{r['expiry']}-{int(r['atm_strike']) if pd.notna(r.get('atm_strike')) else '?'}-{r.get('opt_type','?')}",
            axis=1
        )

    # ── Unified strike column ─────────────────────────────────────────────────
    # Futures strategies have no strike — label as 'FUT'
    combined['strike'] = combined.get('atm_strike', pd.Series('FUT', index=combined.index))
    combined['strike'] = combined['strike'].fillna('FUT')

    combined = combined.sort_values('entry_time').reset_index(drop=True)
    return combined


# ── Compounding wrapper ─────────────────────────────────────────────────────────

def _compound_and_report(tl: pd.DataFrame,
                          starting: float,
                          label: str) -> tuple:
    """Run compounding, print report, return (enriched, summary)."""
    if tl.empty:
        print(f"⚠  No trades to compound for {label}")
        return tl, {}

    print(f"\n── Compounding {label} (₹{starting:,.0f} start) ──")
    enriched, ec, summary = run_compounded(
        tl, inst_cfg, starting_capital=starting
    )
    print_compounded_report(enriched, summary, title=label)
    return enriched, summary


# ── Report: year-by-year breakdown ─────────────────────────────────────────────

def _print_year_table(enriched: pd.DataFrame, label: str) -> None:
    print(f"\n  {'─'*60}")
    print(f"  {label} — Year-by-Year")
    print(f"  {'Year':<6} {'Strat':<8} {'N':>5} {'WR%':>6} "
          f"{'StartEq':>12} {'EndEq':>12} {'Return':>8}")
    print(f"  {'─'*60}")
    for yr in sorted(enriched['year'].unique()):
        y = enriched[enriched['year'] == yr]
        for strat in sorted(y['strategy'].unique()):
            s = y[y['strategy'] == strat]
            if s.empty:
                continue
            start_eq = s['capital_at_entry'].iloc[0]
            end_eq   = s['equity_after'].iloc[-1]
            yret     = (end_eq / start_eq - 1) * 100 if start_eq > 0 else 0
            wr       = s['win'].mean() * 100
            print(f"  {yr:<6} {strat:<8} {len(s):>5}  {wr:>5.1f}%  "
                  f"₹{start_eq:>10,.0f}  ₹{end_eq:>10,.0f}  {yret:>6.1f}%")


# ── Save CSV ────────────────────────────────────────────────────────────────────

def _save(df: pd.DataFrame, name: str) -> None:
    path = TRADE_LOG_DIR / f'{name}.csv'
    out  = df.copy()
    out.insert(0, 'trade_id', range(1, len(out) + 1))

    # ── Contract value at entry / exit ─────────────────────────────────────────
    # For options (OPT_ORB): value = premium × lot_size × lots
    # For futures (ORB, VWAP): value = price × lot_size × lots
    lots = out.get('lots_at_entry', pd.Series(1, index=out.index)).fillna(1)

    def _lot_size_col(row):
        """Return lot_size for this row's date from instrument config."""
        from backtest.engine import _lot_size_for_date
        return _lot_size_for_date(row['date'], inst_cfg)

    lot_sizes = out.apply(_lot_size_col, axis=1)

    opt_mask = out['strategy'] == 'OPT_ORB'

    entry_price = out['entry'].copy()
    exit_price  = out['exit_price'].copy()
    if opt_mask.any() and 'premium_entry' in out.columns:
        entry_price[opt_mask] = out.loc[opt_mask, 'premium_entry']
        exit_price[opt_mask]  = out.loc[opt_mask, 'premium_exit']

    out['entry_value'] = (entry_price * lot_sizes * lots).round(2)
    out['exit_value']  = (exit_price  * lot_sizes * lots).round(2)

    # Ensure key columns appear early for readability
    priority = ['trade_id', 'date', 'strategy', 'contract', 'strike',
                'direction', 'entry', 'entry_value', 'exit_price', 'exit_value',
                'pnl_rs', 'exit_reason', 'lots_at_entry', 'capital_at_entry',
                'equity_after']
    cols = priority + [c for c in out.columns if c not in priority]
    out  = out[[c for c in cols if c in out.columns]]
    out.to_csv(path, index=False, mode='w')
    print(f"  📝 Saved {len(out)} trades → {path}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "█"*64)
print("  HAWALA v2 — FULL-DAY PIPELINE (ORB + VWAP + NRB + LAST HOUR)")
print(f"  IS: {IS_START} → {IS_END}  |  OOS: {OOS_START} → {OOS_END}")
print("█"*64)

# ── Load data ─────────────────────────────────────────────────────────────────
data_is  = _load_or_fetch(IS_CACHE,  IS_START,  IS_END)
data_oos = _load_or_fetch(OOS_CACHE, OOS_START, OOS_END)

# ── IS: Run strategies ────────────────────────────────────────────────────────
tl_is = _run_strategies(data_is,  'IS 2024')
tl_oos = _run_strategies(data_oos, 'OOS 2025')

# ── IS: Compound ──────────────────────────────────────────────────────────────
print("\n" + "="*64)
print("  IN-SAMPLE (2024)")
print("="*64)

enriched_is, summary_is = _compound_and_report(
    tl_is, STARTING_CAP, 'Full Pipeline / BANKNIFTY  2024 (IS)'
)

# ── 2024-end equity (highlighted) ────────────────────────────────────────────
eq_2024_end = STARTING_CAP
if not enriched_is.empty:
    trades_2024 = enriched_is[enriched_is['year'] == 2024]
    if not trades_2024.empty:
        eq_2024_end = float(trades_2024['equity_after'].iloc[-1])
    else:
        # No 2024 trades — use last IS equity
        eq_2024_end = float(enriched_is['equity_after'].iloc[-1])

print(f"\n{'━'*64}")
print(f"  ★  CAPITAL AT END OF 2024 : ₹{eq_2024_end:>12,.0f}")
print(f"     (OOS 2025 starts here)")
print(f"{'━'*64}")

if not enriched_is.empty:
    _print_year_table(enriched_is, 'IS')

# ── OOS: Compound (starting from 2024-end equity) ────────────────────────────
print("\n" + "="*64)
print("  OUT-OF-SAMPLE (2025)  — zero hindsight, same params")
print("="*64)

enriched_oos, summary_oos = _compound_and_report(
    tl_oos, eq_2024_end, 'Full Pipeline / BANKNIFTY  2025 (OOS)'
)

eq_2025_end = eq_2024_end
if not enriched_oos.empty:
    eq_2025_end = float(enriched_oos['equity_after'].iloc[-1])

print(f"\n{'━'*64}")
print(f"  ★  CAPITAL AT END OF 2025 : ₹{eq_2025_end:>12,.0f}")
print(f"{'━'*64}")

if not enriched_oos.empty:
    _print_year_table(enriched_oos, 'OOS')

# ── Combined summary ──────────────────────────────────────────────────────────
print(f"\n{'='*64}")
print(f"  FULL PICTURE  (2024 → 2025)")
print(f"{'='*64}")
print(f"  Start capital (2024-01-01) : ₹{STARTING_CAP:>12,.0f}")
print(f"  End of 2024 (IS)           : ₹{eq_2024_end:>12,.0f}  "
      f"({(eq_2024_end/STARTING_CAP - 1)*100:+.1f}%)")
print(f"  End of 2025 (OOS)          : ₹{eq_2025_end:>12,.0f}  "
      f"({(eq_2025_end/STARTING_CAP - 1)*100:+.1f}% vs start)")
if summary_is:
    print(f"\n  IS  trades : {summary_is['trades']}  |  WR: {summary_is['win_rate']:.1f}%"
          f"  |  Max DD: {summary_is['max_dd_pct']:.1f}%")
if summary_oos:
    print(f"  OOS trades : {summary_oos['trades']}  |  WR: {summary_oos['win_rate']:.1f}%"
          f"  |  Max DD: {summary_oos['max_dd_pct']:.1f}%")

# ── Save trade logs ───────────────────────────────────────────────────────────
print()
if not enriched_is.empty:
    _save(enriched_is,  'baseline_IS_2024')
if not enriched_oos.empty:
    _save(enriched_oos, 'baseline_OOS_2025')

# Combined all-years log
all_parts = [df for df in [enriched_is, enriched_oos] if not df.empty]
if all_parts:
    all_trades = pd.concat(all_parts, ignore_index=True)
    all_trades['period'] = all_trades['year'].apply(
        lambda y: 'OOS' if y >= 2025 else 'IS'
    )
    _save(all_trades, 'baseline_combined_all')
    print(f"\n  Total trades (2022-2025): {len(all_trades)}")

print("\n✅  Done.")
