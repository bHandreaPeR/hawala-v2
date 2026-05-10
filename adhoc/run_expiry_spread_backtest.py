#!/usr/bin/env python3
# ============================================================
# run_expiry_spread_backtest.py — Hawala v2
# ============================================================
# Multi-instrument backtest + parameter sweep for the Expiry
# Directional Spread (Bull Put / Bear Call) strategy.
#
# Instruments: BANKNIFTY, NIFTY, SENSEX
# Sizing:      Fixed 1 lot per trade (clean WR comparison)
# Period:      2021-01-01 → 2026-04-30
#
# Sweep grid:
#   ES_GAP_THRESHOLD  : [20, 30, 50]
#   ES_PUT_ATR        : [0.35, 0.50, 0.65]
#   ES_CALL_ATR       : (tied to ES_PUT_ATR for symmetry)
#   ES_WING_WIDTH     : [100, 150, 200, 300]
#
# Output:
#   trade_logs/expiry_spread_backtest.csv    — all trades (best params)
#   trade_logs/expiry_spread_sweep.csv       — full sweep results grid
#   Console: per-instrument WR table + best param combo
#
# Usage:
#   python run_expiry_spread_backtest.py
# ============================================================

import os, sys, pickle, pathlib, itertools
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv('token.env')
TOKEN       = os.getenv('GROWW_API_KEY', '').strip()
TOTP_SECRET = os.getenv('GROWW_TOTP_SECRET', '').strip()

if not TOKEN:
    sys.exit("❌  GROWW_API_KEY not found in token.env")
if not TOTP_SECRET:
    sys.exit("❌  GROWW_TOTP_SECRET not found in token.env")

import pyotp
from growwapi import GrowwAPI
access_token = GrowwAPI.get_access_token(api_key=TOKEN, totp=pyotp.TOTP(TOTP_SECRET).now())
groww = GrowwAPI(access_token)
print("✅  Groww authenticated")

from config import INSTRUMENTS, STRATEGIES
from data.fetch import fetch_instrument
from strategies.expiry_spread import run_expiry_spread

# ── Config ────────────────────────────────────────────────────────────────────

# Instruments to sweep (SENSEX needs BSE data — skipped if fetch fails)
INSTRUMENTS_TO_RUN = ['BANKNIFTY', 'NIFTY', 'SENSEX']

START_DATE = '2021-01-01'
END_DATE   = '2026-04-30'

TRADE_LOG_DIR = pathlib.Path('trade_logs')
TRADE_LOG_DIR.mkdir(exist_ok=True)

DATA_CACHE_DIR = pathlib.Path('trade_logs')

# Base sweep grid — used for BANKNIFTY and NIFTY (index ~24k–57k range)
SWEEP_GRID_DEFAULT = {
    'ES_GAP_THRESHOLD': [20, 30, 50],
    'ES_PUT_ATR':       [0.35, 0.50, 0.65],
    'ES_WING_WIDTH':    [100, 150, 200, 300],
}

# Instrument-specific sweep overrides
# SENSEX at ~79k is ~3.3× NIFTY → scale wings proportionally
SWEEP_GRID_OVERRIDES = {
    'SENSEX': {
        'ES_GAP_THRESHOLD': [50, 100, 150],   # SENSEX moves are larger in pts
        'ES_PUT_ATR':       [0.35, 0.50, 0.65],
        'ES_WING_WIDTH':    [400, 600, 800, 1000],  # ~3.3× NIFTY grid [150,200,300,~330]
    },
}

def _get_sweep_grid(instrument: str) -> dict:
    return SWEEP_GRID_OVERRIDES.get(instrument, SWEEP_GRID_DEFAULT)

BASE_PARAMS = {**STRATEGIES['expiry_spread']['params'], 'ES_FIXED_LOT': True}


# ── ATR14 pre-computation (vectorised, avoids O(n²) per-day loop) ─────────────

def _precompute_atr14(data: pd.DataFrame) -> dict:
    """
    Pre-compute ATR14 for every trading date in data — fully vectorised.
    Uses daily H-L range; returns {date: atr14_float}.
    ~100× faster than per-day iterrows() approach used in _atr14().
    """
    # Build daily range series (max intraday H - min intraday L)
    daily_hi = data['High'].resample('D').max().dropna()
    daily_lo = data['Low'].resample('D').min().dropna()
    daily_range = (daily_hi - daily_lo).rename('range')

    # Keep only actual trading days (drop weekends/holidays with no data)
    trading_dates = sorted(set(data.index.date))
    range_by_date = {ts.date(): float(v) for ts, v in daily_range.items()
                     if ts.date() in set(trading_dates)}

    cache = {}
    dates_list = sorted(range_by_date.keys())
    for i, d in enumerate(dates_list):
        if i < 14:
            cache[d] = 600.0   # not enough history
            continue
        window = [range_by_date[dates_list[j]] for j in range(i - 14, i)]
        cache[d] = float(np.mean(window))

    # Fill any dates in trading_dates not in range_by_date with fallback
    for d in trading_dates:
        if d not in cache:
            cache[d] = 600.0
    return cache


# ── Data fetch + cache ────────────────────────────────────────────────────────

def _load_or_fetch(instrument: str) -> pd.DataFrame:
    cache_path = DATA_CACHE_DIR / f'_spread_data_{instrument}_{START_DATE}_{END_DATE}.pkl'
    if cache_path.exists():
        print(f"  📦  Cache hit: {cache_path.name}")
        with open(cache_path, 'rb') as f:
            data = pickle.load(f)
        print(f"       {len(data):,} candles  "
              f"({data.index[0].date()} → {data.index[-1].date()})")
        return data

    print(f"  🌐  Fetching {instrument} futures {START_DATE} → {END_DATE} ...")
    try:
        # SENSEX uses BSE exchange — fetch_instrument reads symbol from config
        data = fetch_instrument(instrument, START_DATE, END_DATE,
                                groww=groww, use_futures=True)
    except Exception as e:
        print(f"  ❌  {instrument} fetch failed: {e}")
        return pd.DataFrame()

    if data.empty:
        print(f"  ❌  No data returned for {instrument}")
        return pd.DataFrame()

    with open(cache_path, 'wb') as f:
        pickle.dump(data, f)
    print(f"  💾  Cached → {cache_path.name}  ({len(data):,} candles)")
    return data


# ── Real Groww expiry calendar ────────────────────────────────────────────────

def _build_real_expiry_calendar(instrument: str) -> set:
    """
    Fetch all real expiry dates for an instrument from the Groww API
    (covers START_DATE → END_DATE).  Returns a set of datetime.date.

    Uses real API data so the calendar reflects:
      - BANKNIFTY: Wed weeklies (2023) → monthly only (Dec-2024) → Tue monthly (Sep-2025+)
      - NIFTY: Thu weeklies → Tue weeklies (Sep-2025+)
      - SENSEX: BSE Fri weeklies (if API supports it)
    """
    from data.contract_resolver import _fetch_expiries_for_month
    import time as _time

    inst_cfg = INSTRUMENTS[instrument]
    exchange = inst_cfg.get('exchange', 'NSE')
    monthly_only = inst_cfg.get('monthly_only', False)
    underlying   = inst_cfg.get('underlying_symbol', instrument)

    start_y = int(START_DATE[:4])
    start_m = int(START_DATE[5:7])
    end_y   = int(END_DATE[:4])
    end_m   = int(END_DATE[5:7])

    all_expiries: set = set()
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        exps = _fetch_expiries_for_month(groww, underlying, y, m, exchange=exchange)
        _time.sleep(0.2)
        if monthly_only:
            # Only keep the last expiry of each month (monthly contract)
            if exps:
                all_expiries.add(sorted(exps)[-1])
        else:
            # All expiries (weekly + monthly)
            all_expiries.update(exps)
        m += 1
        if m > 12:
            m = 1
            y += 1

    return all_expiries


# ── Single sweep run ──────────────────────────────────────────────────────────

def _run_one(data: pd.DataFrame, inst_cfg: dict, sweep_params: dict,
             atr_cache: dict | None = None,
             day_cache: dict | None = None,
             expiry_dates: set | None = None) -> pd.DataFrame:
    """Run strategy with sweep params, return trade log."""
    p = {**BASE_PARAMS, **sweep_params}
    # Tie ES_CALL_ATR to ES_PUT_ATR for symmetric spread
    p['ES_CALL_ATR'] = p['ES_PUT_ATR']
    # Inject pre-computed caches — avoids O(n²) date filtering per combo
    if atr_cache:
        p['_atr_cache'] = atr_cache
    if day_cache:
        p['_day_cache'] = day_cache
    # Inject real Groww expiry calendar — overrides DOW-based guessing
    if expiry_dates is not None:
        p['_expiry_dates'] = expiry_dates
    try:
        log = run_expiry_spread(data, inst_cfg, p, groww=None)
        return log
    except Exception as e:
        print(f"    ⚠  Sweep error: {e}")
        return pd.DataFrame()


# ── Sweep an instrument ───────────────────────────────────────────────────────

def _sweep_instrument(instrument: str, data: pd.DataFrame) -> dict:
    """
    Run all sweep combinations for an instrument.
    Returns: {
        'best_params': dict,
        'best_wr': float,
        'best_log': pd.DataFrame,
        'sweep_results': list[dict],
    }
    """
    inst_cfg  = INSTRUMENTS[instrument]
    sweep_grid = _get_sweep_grid(instrument)
    keys      = list(sweep_grid.keys())
    combos    = list(itertools.product(*[sweep_grid[k] for k in keys]))

    # Fetch real Groww expiry calendar (accurate DOW + holiday adjustments)
    print(f"  📅  Fetching real expiry calendar from Groww API ...")
    expiry_dates = _build_real_expiry_calendar(instrument)
    monthly_only = inst_cfg.get('monthly_only', False)
    print(f"       {len(expiry_dates)} real expiry dates "
          f"({'monthly only' if monthly_only else 'weekly + monthly'})")

    # Pre-compute ATR14 and group data by date ONCE — shared across all 36 combos
    print(f"  📐  Pre-computing ATR14 + grouping {len(data):,} candles ...")
    atr_cache = _precompute_atr14(data)
    day_cache = {d: grp for d, grp in data.groupby(data.index.date)}
    print(f"       Done — {len(atr_cache)} ATR dates, {len(day_cache)} day groups.")

    print(f"\n  {'─'*60}")
    print(f"  Sweeping {instrument}  ({len(combos)} combinations) ...")
    print(f"  {'─'*60}")

    sweep_results = []
    best_wr       = -1.0
    best_log      = pd.DataFrame()
    best_params   = {}

    for ci, combo in enumerate(combos, 1):
        sweep_params = dict(zip(keys, combo))
        log          = _run_one(data, inst_cfg, sweep_params,
                                atr_cache=atr_cache, day_cache=day_cache,
                                expiry_dates=expiry_dates)
        wr = log['win'].mean() if not log.empty and len(log) >= 3 else 0.0
        n  = len(log) if not log.empty else 0
        print(f"    [{ci:>2}/{len(combos)}] gap={sweep_params['ES_GAP_THRESHOLD']:>2.0f}  "
              f"atr={sweep_params['ES_PUT_ATR']:.2f}  "
              f"wing={sweep_params['ES_WING_WIDTH']:>3}  "
              f"→ n={n:>4}  WR={wr:.0%}", flush=True)
        if log.empty or len(log) < 3:
            continue

        trades = len(log)
        wr     = float(log['win'].mean())
        net_rs = float(log['pnl_rs'].sum())
        maxdd  = float(_max_drawdown(log['pnl_rs']))
        calmar = net_rs / max(abs(maxdd), 1)

        bull_wr = float(log[log['direction']=='BULL']['win'].mean()) if (log['direction']=='BULL').any() else 0.0
        bear_wr = float(log[log['direction']=='BEAR']['win'].mean()) if (log['direction']=='BEAR').any() else 0.0
        bull_n  = int((log['direction']=='BULL').sum())
        bear_n  = int((log['direction']=='BEAR').sum())

        sweep_results.append({
            'instrument':  instrument,
            **sweep_params,
            'trades':      trades,
            'win_rate':    round(wr, 4),
            'bull_n':      bull_n,
            'bull_wr':     round(bull_wr, 4),
            'bear_n':      bear_n,
            'bear_wr':     round(bear_wr, 4),
            'net_pnl_rs':  round(net_rs, 2),
            'max_dd_rs':   round(maxdd, 2),
            'calmar':      round(calmar, 4),
        })

        # Best = highest WR with at least 5 trades
        if trades >= 5 and wr > best_wr:
            best_wr     = wr
            best_log    = log.copy()
            best_params = sweep_params.copy()

    return {
        'best_params':   best_params,
        'best_wr':       best_wr,
        'best_log':      best_log,
        'sweep_results': sweep_results,
    }


def _max_drawdown(pnl_series: pd.Series) -> float:
    """Max drawdown from cumulative P&L series."""
    cumulative = pnl_series.cumsum()
    rolling_max = cumulative.cummax()
    drawdown = cumulative - rolling_max
    return float(drawdown.min()) if len(drawdown) > 0 else 0.0


# ── Top-5 combos per instrument ───────────────────────────────────────────────

def _print_top5(sweep_results: list, instrument: str):
    if not sweep_results:
        return
    df = pd.DataFrame(sweep_results)
    df = df[df['trades'] >= 5].sort_values('win_rate', ascending=False)
    print(f"\n  Top-5 param combos for {instrument} (by win rate, min 5 trades):")
    print(f"  {'gap':>4} {'atr':>5} {'wing':>5} | {'n':>4} {'WR':>7} | "
          f"{'bull_n':>6} {'bullWR':>7} | {'bear_n':>6} {'bearWR':>7} | "
          f"{'net_₹':>9} {'maxDD':>9}")
    print(f"  {'─'*80}")
    for _, row in df.head(5).iterrows():
        print(f"  {row['ES_GAP_THRESHOLD']:>4.0f} {row['ES_PUT_ATR']:>5.2f} {row['ES_WING_WIDTH']:>5.0f} | "
              f"{row['trades']:>4.0f} {row['win_rate']:>7.1%} | "
              f"{row['bull_n']:>6.0f} {row['bull_wr']:>7.1%} | "
              f"{row['bear_n']:>6.0f} {row['bear_wr']:>7.1%} | "
              f"{row['net_pnl_rs']:>9,.0f} {row['max_dd_rs']:>9,.0f}")


# ── YoY breakdown for best params ─────────────────────────────────────────────

def _print_yoy(log: pd.DataFrame, instrument: str, best_params: dict):
    if log.empty:
        return
    print(f"\n  YoY breakdown — {instrument}  "
          f"(gap={best_params.get('ES_GAP_THRESHOLD')}, "
          f"atr={best_params.get('ES_PUT_ATR')}, "
          f"wing={best_params.get('ES_WING_WIDTH')})")
    print(f"  {'Year':>6} {'Trades':>7} {'WR':>8} {'Bull':>8} {'Bear':>8} "
          f"{'Net_₹':>10} {'Max_DD':>10}")
    print(f"  {'─'*65}")
    for yr, grp in log.groupby('year'):
        wr     = grp['win'].mean()
        bull_w = grp[grp['direction']=='BULL']['win'].mean() if (grp['direction']=='BULL').any() else float('nan')
        bear_w = grp[grp['direction']=='BEAR']['win'].mean() if (grp['direction']=='BEAR').any() else float('nan')
        net    = grp['pnl_rs'].sum()
        maxdd  = _max_drawdown(grp['pnl_rs'])
        print(f"  {yr:>6} {len(grp):>7} {wr:>8.1%} "
              f"{bull_w:>8.1%} {bear_w:>8.1%} "
              f"{net:>10,.0f} {maxdd:>10,.0f}")
    total_wr  = log['win'].mean()
    total_net = log['pnl_rs'].sum()
    total_dd  = _max_drawdown(log['pnl_rs'])
    print(f"  {'TOTAL':>6} {len(log):>7} {total_wr:>8.1%} "
          f"{'':>8} {'':>8} {total_net:>10,.0f} {total_dd:>10,.0f}")


# ── Exit reason breakdown ──────────────────────────────────────────────────────

def _print_exit_reasons(log: pd.DataFrame):
    if log.empty:
        return
    for reason, grp in log.groupby('exit_reason'):
        wr  = grp['win'].mean()
        n   = len(grp)
        net = grp['pnl_rs'].sum()
        print(f"    {reason:<15} {n:>4} trades  WR={wr:.1%}  Net=₹{net:,.0f}")


# ── Summary table (all instruments) ──────────────────────────────────────────

def _print_summary_table(results: dict):
    print(f"\n{'═'*75}")
    print("EXPIRY SPREAD — MULTI-INSTRUMENT SUMMARY (best WR params, 1 lot)")
    print(f"{'═'*75}")
    print(f"{'Instrument':>12} {'WR':>7} {'Trades':>7} {'Net_₹':>10} {'MaxDD_₹':>10}  "
          f"{'Best Params'}")
    print(f"{'─'*75}")
    for inst, r in results.items():
        if not r['best_log'].empty:
            log = r['best_log']
            wr  = log['win'].mean()
            n   = len(log)
            net = log['pnl_rs'].sum()
            dd  = _max_drawdown(log['pnl_rs'])
            bp  = r['best_params']
            print(f"{inst:>12} {wr:>7.1%} {n:>7} {net:>10,.0f} {dd:>10,.0f}  "
                  f"gap={bp.get('ES_GAP_THRESHOLD')}, "
                  f"atr={bp.get('ES_PUT_ATR')}, "
                  f"wing={bp.get('ES_WING_WIDTH')}")
        else:
            print(f"{inst:>12}  No trades / data unavailable")
    print(f"{'═'*75}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    all_sweep    = []
    all_best_log = []
    results      = {}

    for instrument in INSTRUMENTS_TO_RUN:
        print(f"\n{'═'*60}")
        print(f"  Instrument: {instrument}")
        print(f"{'═'*60}")

        if instrument not in INSTRUMENTS:
            print(f"  ⚠  {instrument} not in config.INSTRUMENTS — skipping")
            results[instrument] = {'best_log': pd.DataFrame(), 'best_params': {}, 'best_wr': 0}
            continue

        data = _load_or_fetch(instrument)
        if data.empty:
            print(f"  ⚠  No data for {instrument} — skipping")
            results[instrument] = {'best_log': pd.DataFrame(), 'best_params': {}, 'best_wr': 0}
            continue

        r = _sweep_instrument(instrument, data)
        results[instrument] = r

        _print_top5(r['sweep_results'], instrument)
        _print_yoy(r['best_log'], instrument, r['best_params'])

        if not r['best_log'].empty:
            print(f"\n  Exit reason breakdown (best params):")
            _print_exit_reasons(r['best_log'])

            best_with_inst = r['best_log'].copy()
            best_with_inst['instrument_key'] = instrument
            all_best_log.append(best_with_inst)

        all_sweep.extend(r['sweep_results'])

    # ── Summary table ─────────────────────────────────────────────────────────
    _print_summary_table(results)

    # ── Save outputs ──────────────────────────────────────────────────────────
    if all_best_log:
        combined = pd.concat(all_best_log, ignore_index=True)
        out_path = TRADE_LOG_DIR / 'expiry_spread_backtest.csv'
        combined.to_csv(out_path, index=False)
        print(f"\n💾  Trades saved → {out_path}")

    if all_sweep:
        sweep_df  = pd.DataFrame(all_sweep).sort_values(
            ['instrument', 'win_rate'], ascending=[True, False])
        sweep_path = TRADE_LOG_DIR / 'expiry_spread_sweep.csv'
        sweep_df.to_csv(sweep_path, index=False)
        print(f"💾  Sweep grid saved → {sweep_path}")


if __name__ == '__main__':
    main()
