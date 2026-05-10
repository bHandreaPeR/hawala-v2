# ============================================================
# backtest/compounding_engine.py — Sequential Compounding Walker
# ============================================================
# Walks a strategy's trade_log in entry-time order, recomputing
# lots after every trade from the current equity. Profits are
# reinvested; losses shrink the next trade's deployable capital.
#
# This is deliberately SEPARATE from backtest/combiner.py (which
# holds capital fixed). Use compounding_engine for strategies
# where the user wants compounded CAGR (candlestick strategy).
#
# Output
# ------
# - enriched_log : original trade_log with three new columns populated:
#                    capital_at_entry, lots_at_entry, equity_after
#                  and pnl_rs recomputed using the lots-at-entry size.
# - equity_curve : DataFrame indexed by exit_ts with columns
#                    [equity, drawdown_pct]
# - summary      : dict with CAGR, total_return_x, max_dd_pct,
#                    win_rate, trades, final_equity.
# ============================================================

import math
import pandas as pd
import numpy as np

from backtest.engine import _lot_size_for_date


def run_compounded(trade_log: pd.DataFrame,
                   instrument_config: dict,
                   starting_capital: float = 1_00_000,
                   max_capital_pct: float = 0.90,
                   min_lots: int = 1,
                   per_trade_risk_cap_pct: float = 0.02,
                   daily_loss_halt_pct: float = 0.05) -> tuple:
    """
    Sequentially walk trades, compounding equity.

    Parameters
    ----------
    trade_log              : DataFrame from a strategy run (needs columns
                             entry_ts, exit_ts, date, pnl_pts).
    instrument_config      : dict from config.INSTRUMENTS[instrument]
    starting_capital       : initial equity (₹)
    max_capital_pct        : fraction of equity deployable per trade (margin cap)
    min_lots               : minimum lots per trade
    per_trade_risk_cap_pct : hard cap on per-trade DOWN-side P&L as fraction of
                             equity-at-entry. e.g. 0.02 ⇒ a single losing trade
                             can never lose more than 2% of equity-at-entry,
                             regardless of how many lots the margin would have
                             allowed. Set to 0 to disable. (P0 risk control.)
    daily_loss_halt_pct    : if cumulative P&L on the trade's date drops below
                             -daily_loss_halt_pct × start-of-day equity, ALL
                             subsequent trades that same day are SKIPPED
                             (lots=0, pnl_rs=0). Set to 0 to disable.

    Returns
    -------
    (enriched_log, equity_curve, summary)
    """
    if trade_log.empty:
        return trade_log.copy(), pd.DataFrame(), {}

    margin    = instrument_config.get('margin_per_lot', 75_000)
    brokerage = instrument_config.get('brokerage', 40)

    sort_col = 'entry_time' if 'entry_time' in trade_log.columns else 'entry_ts'
    tl = trade_log.sort_values(sort_col).reset_index(drop=True).copy()

    equity = float(starting_capital)
    peak   = equity
    rows_cap, rows_lots, rows_eq, rows_pnl, rows_dd = [], [], [], [], []

    # Daily-loss-halt state
    cur_day        = None
    sod_equity     = equity   # equity at start-of-day
    day_realised   = 0.0      # cumulative P&L on cur_day

    halts = 0
    risk_capped = 0

    for _, row in tl.iterrows():
        row_date = pd.Timestamp(row['date']).date()
        if cur_day != row_date:
            cur_day      = row_date
            sod_equity   = equity
            day_realised = 0.0

        # per_trade_risk_pct overrides max_capital_pct (used by options strategies)
        _rp = row.get('per_trade_risk_pct', None)
        trade_risk_pct = float(_rp) if (_rp is not None and not pd.isna(_rp)) else max_capital_pct
        deployable = equity * trade_risk_pct
        # Use per-trade margin (OPT = premium × lot_size; FUT = ₹75k)
        _rm = row.get('margin_per_lot', margin)
        row_margin = margin if (pd.isna(_rm) or _rm is None or float(_rm) <= 0) else float(_rm)
        lots = max(int(math.floor(deployable / row_margin)), min_lots)

        lot_size   = _lot_size_for_date(row['date'], instrument_config)
        pnl_pts    = row.get('pnl_pts', 0) or 0
        direction_sign = 1  # pnl_pts already signed in strategy output

        # ── Daily-loss halt ─────────────────────────────────────────────
        if (daily_loss_halt_pct > 0 and sod_equity > 0
                and day_realised <= -daily_loss_halt_pct * sod_equity):
            # Skip this trade entirely
            halts += 1
            capital_at_entry = equity
            rows_cap.append(capital_at_entry)
            rows_lots.append(0)
            rows_eq.append(equity)
            rows_pnl.append(0.0)
            peak   = max(peak, equity)
            dd_pct = (peak - equity) / peak * 100 if peak > 0 else 0
            rows_dd.append(dd_pct)
            continue

        # ── Per-trade risk cap (downsize lots if a full-margin sizing
        #    would risk more than per_trade_risk_cap_pct of equity) ─────
        if per_trade_risk_cap_pct > 0 and pnl_pts < 0:
            max_loss_rs = per_trade_risk_cap_pct * equity
            # implied loss at current sizing
            implied_loss = abs(pnl_pts) * lots * lot_size + brokerage
            if implied_loss > max_loss_rs and lot_size > 0 and pnl_pts != 0:
                # Cap lots so loss ≤ max_loss_rs
                capped_lots = int(math.floor(
                    (max_loss_rs - brokerage) / (abs(pnl_pts) * lot_size)
                ))
                if capped_lots < lots:
                    lots = max(capped_lots, 0)
                    risk_capped += 1

        pnl_rs     = round(pnl_pts * lots * lot_size * direction_sign - brokerage, 2)
        if lots == 0:
            pnl_rs = 0.0

        capital_at_entry = equity
        equity          += pnl_rs
        day_realised    += pnl_rs
        peak             = max(peak, equity)
        dd_pct           = (peak - equity) / peak * 100 if peak > 0 else 0

        rows_cap.append(capital_at_entry)
        rows_lots.append(lots)
        rows_eq.append(equity)
        rows_pnl.append(pnl_rs)
        rows_dd.append(dd_pct)

    tl['capital_at_entry'] = rows_cap
    tl['lots_at_entry']    = rows_lots
    tl['equity_after']     = rows_eq
    tl['pnl_rs']           = rows_pnl
    tl['win']              = (tl['pnl_rs'] > 0).astype(int)

    # Equity curve indexed by exit timestamp
    exit_col = 'exit_time' if 'exit_time' in tl.columns else 'exit_ts'
    ec = pd.DataFrame({
        exit_col:       tl[exit_col].values,
        'equity':       rows_eq,
        'drawdown_pct': rows_dd,
    }).set_index(exit_col).sort_index()

    # Summary metrics
    if len(tl) > 0:
        first_date = pd.Timestamp(tl['date'].iloc[0])
        last_date  = pd.Timestamp(tl['date'].iloc[-1])
        years      = max((last_date - first_date).days / 365.25, 1e-6)
        total_ret  = equity / starting_capital
        cagr       = (total_ret ** (1 / years) - 1) * 100 if total_ret > 0 else -100.0
    else:
        years = 0; total_ret = 1.0; cagr = 0.0

    summary = {
        'trades':         len(tl),
        'starting':       starting_capital,
        'final_equity':   round(equity, 2),
        'total_return_x': round(total_ret, 3),
        'cagr_pct':       round(cagr, 2),
        'max_dd_pct':     round(max(rows_dd) if rows_dd else 0, 2),
        'win_rate':       round(tl['win'].mean() * 100, 2),
        'years':          round(years, 2),
        'halts_daily':    halts,
        'risk_capped':    risk_capped,
    }
    return tl, ec, summary


def print_compounded_report(enriched_log: pd.DataFrame,
                            summary: dict,
                            title: str = 'CANDLESTICK COMPOUNDED') -> None:
    """Concise report for a compounded run."""
    if enriched_log.empty or not summary:
        print("No trades to report.")
        return

    print(f"\n{'='*58}")
    print(f"  {title}")
    print(f"{'='*58}")
    print(f"  Starting capital : ₹{summary['starting']:>12,.0f}")
    print(f"  Final equity     : ₹{summary['final_equity']:>12,.0f}")
    print(f"  Total return     : {summary['total_return_x']:>6.2f}x  "
          f"over {summary['years']} years")
    print(f"  CAGR             : {summary['cagr_pct']:>6.2f}%")
    print(f"  Max drawdown     : {summary['max_dd_pct']:>6.2f}%")
    print(f"  Trades           : {summary['trades']}")
    print(f"  Win rate         : {summary['win_rate']:>6.2f}%")

    print(f"\n  Year-by-year equity:")
    print(f"  {'Year':<6} {'Trades':>7} {'WinRate':>8} {'StartEq':>12} "
          f"{'EndEq':>12} {'YearRet':>8}")
    print(f"  {'-'*58}")
    for yr in sorted(enriched_log['year'].unique()):
        y = enriched_log[enriched_log['year'] == yr]
        if y.empty: continue
        start_eq = y['capital_at_entry'].iloc[0]
        end_eq   = y['equity_after'].iloc[-1]
        yret     = (end_eq / start_eq - 1) * 100 if start_eq > 0 else 0
        wr       = y['win'].mean() * 100
        print(f"  {yr:<6} {len(y):>7}  {wr:>7.1f}%  "
              f"₹{start_eq:>10,.0f}  ₹{end_eq:>10,.0f}  {yret:>6.1f}%")

    print(f"\n  Exit breakdown:")
    print(enriched_log['exit_reason'].value_counts().to_string())
