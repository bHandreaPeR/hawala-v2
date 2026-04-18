# ============================================================
# backtest/combiner.py — Capital-Aware Multi-Strategy Combiner
# ============================================================
# Replaces cell_8_regime_framework.py.
#
# Key changes from the old regime framework:
#   - Timestamp-based deduplication (entry_ts / exit_ts) instead of date
#   - Capital-bound lot sizing: tracks deployed margin at each timestamp
#   - Bias-weighted capital split when strategies overlap simultaneously
#   - Works with any set of strategies (not gap_fill/ORB/VWAP hardcoded)
#
# Usage:
#   from backtest.combiner import combine_strategies, print_combined_report
#   combined = combine_strategies(
#       {'gap_fill': gap_df, 'orb': orb_df, 'vwap': vwap_df},
#       capital=1_00_000
#   )
# ============================================================

from math import floor
import pandas as pd
import numpy as np


def combine_strategies(strategy_results: dict,
                       capital: float = 1_00_000,
                       margin_config: dict = None) -> pd.DataFrame:
    """
    Merge multiple strategy trade logs into one capital-aware combined log.

    Algorithm (timestamp-ordered):
      1. Merge all trade logs, sort by entry_ts
      2. Walk through trades chronologically
      3. At each entry_ts:
         a. Release capital from positions whose exit_ts <= current entry_ts
         b. Compute deployable capital (capped at max_capital_pct)
         c. If one signal: allocate full deployable (up to margin limit)
         d. If multiple signals at same ts: split by bias_score weight
         e. Calculate lots_used from allocated capital / margin_per_lot
         f. Recalculate pnl_rs using actual lots_used
      4. Return combined log with capital_used, lots_used, pnl_rs updated

    Args:
        strategy_results : dict of {strategy_name: trade_log_df}
                           Each DataFrame must follow the standard schema with
                           entry_ts, exit_ts, bias_score, instrument columns.
        capital          : Starting capital in ₹ (default 1,00,000)
        margin_config    : dict of {instrument: margin_per_lot}
                           If None, reads from config.INSTRUMENTS

    Returns:
        pd.DataFrame: Combined trade log with capital_used, lots_used, pnl_rs
                      recalculated based on actual lot allocation.
    """
    from config import INSTRUMENTS, CAPITAL as CAPITAL_CFG

    max_pct   = CAPITAL_CFG.get('max_capital_pct', 0.90)
    min_lots  = CAPITAL_CFG.get('min_lots', 1)

    if margin_config is None:
        margin_config = {k: v['margin_per_lot'] for k, v in INSTRUMENTS.items()}

    # ── Merge all strategy results ────────────────────────────────────────────
    frames = []
    for name, df in strategy_results.items():
        if df is None or df.empty:
            continue
        df = df.copy()
        if 'strategy' not in df.columns:
            df['strategy'] = name.upper()
        frames.append(df)

    if not frames:
        print("⚠ No strategy results to combine.")
        return pd.DataFrame()

    all_trades = pd.concat(frames, ignore_index=True)

    # Ensure entry_ts and exit_ts are datetime
    for col in ('entry_ts', 'exit_ts'):
        if col in all_trades.columns:
            all_trades[col] = pd.to_datetime(all_trades[col])

    # Sort by entry timestamp
    if 'entry_ts' in all_trades.columns:
        all_trades = all_trades.sort_values('entry_ts').reset_index(drop=True)
    else:
        # Fallback: sort by date (old schema compatibility)
        all_trades = all_trades.sort_values('date').reset_index(drop=True)

    # ── Capital allocation walk ───────────────────────────────────────────────
    available_capital = capital
    open_positions    = []   # [{exit_ts, capital_used}]
    result_rows       = []

    # Group trades that enter at the exact same timestamp
    if 'entry_ts' in all_trades.columns:
        groups = all_trades.groupby('entry_ts', sort=True)
    else:
        groups = [(None, all_trades)]   # single group if no timestamps

    for ts, group in groups:
        # Release capital from positions that have closed before this entry
        if ts is not None:
            still_open = []
            for pos in open_positions:
                if pos['exit_ts'] is not None and pos['exit_ts'] <= ts:
                    available_capital += pos['capital_used']
                else:
                    still_open.append(pos)
            open_positions = still_open

        # Deployable capital (respect max_capital_pct)
        deployable = min(available_capital, capital * max_pct)

        signals = group.to_dict('records')

        if len(signals) == 1:
            # Single signal: use all deployable capital for this instrument
            sig    = signals[0]
            instr  = sig.get('instrument', 'NSE-BANKNIFTY')
            # Normalise instrument key (strip 'NSE-' prefix for margin lookup)
            instr_key = instr.replace('NSE-', '')
            margin = margin_config.get(instr_key, margin_config.get(instr, 75_000))
            lots   = max(floor(deployable / margin), 0)
            if lots < min_lots:
                lots = 0   # can't afford minimum — skip
            _apply_lots(sig, lots, margin)
            if lots > 0:
                result_rows.append(sig)
                open_positions.append({
                    'exit_ts':     sig.get('exit_ts'),
                    'capital_used': lots * margin,
                })
                available_capital -= lots * margin

        else:
            # Multiple simultaneous signals: split by bias_score weight
            total_bias = sum(max(s.get('bias_score', 0.5), 0.01) for s in signals)
            for sig in signals:
                instr    = sig.get('instrument', 'NSE-BANKNIFTY')
                instr_key= instr.replace('NSE-', '')
                margin   = margin_config.get(instr_key, margin_config.get(instr, 75_000))
                weight   = max(sig.get('bias_score', 0.5), 0.01) / total_bias
                alloc    = deployable * weight
                lots     = max(floor(alloc / margin), 0)
                if lots < min_lots:
                    lots = 0
                _apply_lots(sig, lots, margin)
                if lots > 0:
                    result_rows.append(sig)
                    open_positions.append({
                        'exit_ts':     sig.get('exit_ts'),
                        'capital_used': lots * margin,
                    })
                    available_capital -= lots * margin

    if not result_rows:
        print("⚠ No trades allocated — check capital vs margin requirements.")
        return pd.DataFrame()

    combined = pd.DataFrame(result_rows)

    print(f"\n✅ Combined: {len(combined)} trades across "
          f"{combined['strategy'].nunique()} strategies")
    by_strat = combined.groupby('strategy')['pnl_rs'].sum()
    for strat, pl in by_strat.items():
        n = (combined['strategy'] == strat).sum()
        print(f"   {strat}: {n} trades | ₹{pl:,.0f}")

    return combined


def _apply_lots(row: dict, lots: int, margin_per_lot: float) -> None:
    """
    Mutate a trade row to use actual lots_used.
    Recalculates pnl_rs and capital_used based on allocated lots.
    """
    old_lots = row.get('lots_used', 1) or 1
    brokerage = row.get('brokerage', 40) if 'brokerage' in row else 40

    row['lots_used']   = lots
    row['capital_used']= lots * margin_per_lot

    if lots > 0 and 'pnl_pts' in row:
        row['pnl_rs'] = round(row['pnl_pts'] * lots - brokerage, 2)
        row['win']    = 1 if row['pnl_rs'] > 0 else 0
    else:
        row['pnl_rs'] = 0.0
        row['win']    = 0


def print_combined_report(combined: pd.DataFrame,
                           capital: float = 1_00_000) -> None:
    """
    Print a full combined multi-strategy report.
    """
    if combined.empty:
        print("No combined trades to report.")
        return

    total_pl = combined['pnl_rs'].sum()
    total_wr = combined['win'].mean() * 100

    print(f"\n{'='*65}")
    print(f"  HAWALA v2 — COMBINED MULTI-STRATEGY REPORT")
    print(f"{'='*65}")
    print(f"  Starting capital : ₹{capital:,.0f}")
    print(f"  Total trades     : {len(combined)}")
    print(f"  Win rate         : {total_wr:.1f}%")
    print(f"  Total P&L        : ₹{total_pl:,.0f}")
    print(f"  Avg P&L/trade    : ₹{combined['pnl_rs'].mean():,.0f}")

    # Capital utilisation
    if 'capital_used' in combined.columns:
        avg_cap = combined['capital_used'].mean()
        print(f"  Avg capital/trade: ₹{avg_cap:,.0f}  "
              f"({avg_cap/capital*100:.1f}% of capital)")

    # Strategy breakdown
    print(f"\n  By strategy:")
    print(f"  {'Strategy':<15} {'Trades':>7} {'WinRate':>8} {'TotalP&L':>12} {'Avg/Trade':>10}")
    print(f"  {'-'*55}")
    for strat in sorted(combined['strategy'].unique()):
        s  = combined[combined['strategy'] == strat]
        wr = s['win'].mean() * 100
        pl = s['pnl_rs'].sum()
        ap = s['pnl_rs'].mean()
        print(f"  {strat:<15} {len(s):>7}  {wr:>7.1f}%  ₹{pl:>10,.0f}  ₹{ap:>8,.0f}")

    # Year-by-year
    print(f"\n  Year-by-year:")
    print(f"  {'Year':<6} {'Trades':>7} {'WinRate':>8} {'TotalP&L':>12}")
    print(f"  {'-'*38}")
    for yr in sorted(combined['year'].unique()):
        y  = combined[combined['year'] == yr]
        wr = y['win'].mean() * 100
        pl = y['pnl_rs'].sum()
        print(f"  {yr:<6} {len(y):>7}  {wr:>7.1f}%  ₹{pl:>10,.0f}")

    # Drawdown
    cumulative = combined.sort_values('entry_ts' if 'entry_ts' in combined.columns else 'date')['pnl_rs'].cumsum()
    running_max = cumulative.cummax()
    drawdown    = (cumulative - running_max).min()
    print(f"\n  Max drawdown     : ₹{drawdown:,.0f}")

    # Profit factor
    wins   = combined[combined['pnl_rs'] > 0]['pnl_rs'].sum()
    losses = combined[combined['pnl_rs'] < 0]['pnl_rs'].sum()
    pf     = abs(wins / losses) if losses != 0 else float('inf')
    print(f"  Profit factor    : {pf:.2f}")
    print(f"  Gross profit     : ₹{wins:,.0f}")
    print(f"  Gross loss       : ₹{losses:,.0f}")
