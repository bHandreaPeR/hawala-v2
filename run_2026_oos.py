# ============================================================
# run_2026_oos.py — True OOS validation on 2026 data
# ============================================================
# Zero code changes to strategies or params.
# Picks up equity from end of 2025 (₹150,026) and runs forward.
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

INSTRUMENT   = 'BANKNIFTY'
OOS_START    = '2026-01-01'
OOS_END      = '2026-04-17'
STARTING_CAP = 150_026   # actual equity at end of 2025

TRADE_LOG_DIR = pathlib.Path('trade_logs')
TRADE_LOG_DIR.mkdir(exist_ok=True)

CACHE = TRADE_LOG_DIR / f'_data_cache_{INSTRUMENT}_{OOS_START}_{OOS_END}.pkl'

inst_cfg    = INSTRUMENTS[INSTRUMENT]
orb_params  = {**STRATEGIES['orb']['params'],  **inst_cfg.get('strategy_params', {})}
vwap_params = {**STRATEGIES['vwap_reversion']['params'], **inst_cfg.get('strategy_params', {})}
opt_params  = {**STRATEGIES['options_orb']['params']}


def _load_or_fetch(cache_path, start, end):
    if cache_path.exists():
        print(f"📦  Loading cache: {cache_path.name}")
        with open(cache_path, 'rb') as f:
            data = pickle.load(f)
        print(f"    {len(data):,} candles  ({data.index[0].date()} → {data.index[-1].date()})")
    else:
        print(f"🌐  Fetching {INSTRUMENT} futures {start} → {end} ...")
        data = fetch_instrument(INSTRUMENT, start, end, groww=groww, use_futures=True)
        if data.empty:
            sys.exit("❌  No data returned — aborting.")
        with open(cache_path, 'wb') as f:
            pickle.dump(data, f)
        print(f"💾  Cached → {cache_path.name}  ({len(data):,} candles)")
    return data


def _save(df, name):
    path = TRADE_LOG_DIR / f'{name}.csv'
    out  = df.copy()
    out.insert(0, 'trade_id', range(1, len(out) + 1))

    lots = out.get('lots_at_entry', pd.Series(1, index=out.index)).fillna(1)

    def _lot_size_col(row):
        return _lot_size_for_date(row['date'], inst_cfg)

    lot_sizes = out.apply(_lot_size_col, axis=1)
    opt_mask  = out['strategy'] == 'OPT_ORB'

    entry_price = out['entry'].copy()
    exit_price  = out['exit_price'].copy()
    if opt_mask.any() and 'premium_entry' in out.columns:
        entry_price[opt_mask] = out.loc[opt_mask, 'premium_entry']
        exit_price[opt_mask]  = out.loc[opt_mask, 'premium_exit']

    out['entry_value'] = (entry_price * lot_sizes * lots).round(2)
    out['exit_value']  = (exit_price  * lot_sizes * lots).round(2)

    priority = ['trade_id', 'date', 'strategy', 'contract', 'strike',
                'direction', 'entry', 'entry_value', 'exit_price', 'exit_value',
                'pnl_rs', 'exit_reason', 'lots_at_entry', 'capital_at_entry', 'equity_after']
    cols = priority + [c for c in out.columns if c not in priority]
    out  = out[[c for c in cols if c in out.columns]]
    out.to_csv(path, index=False, mode='w')
    print(f"  📝 Saved {len(out)} trades → {path}")


print("\n" + "█"*64)
print("  HAWALA v2 — TRUE OOS 2026  (zero param changes)")
print(f"  Period : {OOS_START} → {OOS_END}")
print(f"  Start equity : ₹{STARTING_CAP:,}  (end-of-2025 carry-forward)")
print("█"*64)

data = _load_or_fetch(CACHE, OOS_START, OOS_END)

print(f"\n── Running Futures ORB (gap 50-100 pts, Tue/Wed/Fri) ──")
orb_log  = run_orb(data, inst_cfg, orb_params)
print(f"    Futures ORB: {len(orb_log)} trades")

print(f"── Running VWAP Reversion (no-gap days) ──")
vwap_log = run_vwap_reversion(data, inst_cfg, vwap_params)
print(f"    VWAP: {len(vwap_log)} trades")

print(f"── Running Options ORB (gap >100 pts, Tue/Wed/Fri) ──")
opt_log  = run_options_orb(data, inst_cfg, opt_params, groww=groww)
print(f"    Options ORB: {len(opt_log)} trades")

frames = [df for df in [orb_log, vwap_log, opt_log] if not df.empty]
if not frames:
    sys.exit("⚠  No trades generated for 2026 period.")

combined = pd.concat(frames, ignore_index=True)

if 'entry_ts' in combined.columns and 'entry_time' not in combined.columns:
    combined = combined.rename(columns={'entry_ts': 'entry_time', 'exit_ts': 'exit_time'})

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
    combined['contract'] = combined['date'].map(lambda d: date_to_contract.get(d, ''))
else:
    combined['contract'] = ''

opt_mask = combined['strategy'] == 'OPT_ORB'
if opt_mask.any() and 'expiry' in combined.columns:
    combined.loc[opt_mask, 'contract'] = combined.loc[opt_mask].apply(
        lambda r: f"NSE-BANKNIFTY-{r['expiry']}-{int(r['atm_strike']) if pd.notna(r.get('atm_strike')) else '?'}-{r.get('opt_type','?')}",
        axis=1
    )

combined['strike'] = combined.get('atm_strike', pd.Series('FUT', index=combined.index))
combined['strike'] = combined['strike'].fillna('FUT')
combined = combined.sort_values('entry_time').reset_index(drop=True)

print(f"\n── Compounding 2026 OOS (₹{STARTING_CAP:,} start) ──")
enriched, ec, summary = run_compounded(combined, inst_cfg, starting_capital=STARTING_CAP)
print_compounded_report(enriched, summary, title='True OOS 2026')

eq_end = float(enriched['equity_after'].iloc[-1]) if not enriched.empty else STARTING_CAP

print(f"\n{'━'*64}")
print(f"  ★  CAPITAL AT END OF APR-2026 : ₹{eq_end:>12,.0f}")
print(f"     Start (Jan 2026)           : ₹{STARTING_CAP:>12,.0f}")
print(f"     Return                     : {(eq_end/STARTING_CAP - 1)*100:+.1f}%")
print(f"{'━'*64}")

print(f"\n  Strategy breakdown:")
for strat in sorted(enriched['strategy'].unique()):
    s = enriched[enriched['strategy'] == strat]
    wr = s['win'].mean() * 100
    pnl = s['pnl_rs'].sum()
    print(f"  {strat:<10}  {len(s):>3} trades  WR: {wr:>5.1f}%  P&L: ₹{pnl:>10,.0f}")

_save(enriched, 'oos_2026')
print("\n✅  Done.")
