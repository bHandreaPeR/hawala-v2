# ============================================================
# run_next_steps.py — Hawala v2 Next-Phase Analysis Runbook
# ============================================================
# Runs the priority tasks from the post-backtest analysis plan.
# Execute top-to-bottom in a Jupyter cell or as a script.
#
# Prerequisites:
#   groww  = authenticated GrowwAPI instance (from cell_1_setup.py)
#   data   = 15-min BANKNIFTY futures data 2022-2024 (from prior backtest)
#            OR re-fetch: data = fetch_instrument('BANKNIFTY', '2022-01-01', '2024-12-31',
#                                                  groww=groww, use_futures=True)
# ============================================================

import pandas as pd
from backtest.engine import run_backtest, print_strategy_report
from backtest.combiner import combine_strategies, print_combined_report
from data.fetch import fetch_instrument

# ── Decision thresholds (from approved plan) ──────────────────────────────────
HEALTHY_THRESHOLD   = 400   # ₹ avg/trade — proceed to live
MARGINAL_THRESHOLD  = 150   # ₹ avg/trade — re-optimise before live


# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 1 — Out-of-sample 2025 test
# ══════════════════════════════════════════════════════════════════════════════

def run_priority_1(groww):
    """
    OOS validation on 2025 data.
    This is the key decision gate:
      avg/trade > ₹400 → strategy healthy, proceed to combine + paper trade
      avg/trade ₹150–400 → re-optimise parameters (run Priority 2 first)
      avg/trade < ₹150  → strategy degraded; focus on ORB + VWAP
    """
    print("\n" + "="*65)
    print("  PRIORITY 1: OUT-OF-SAMPLE 2025 TEST")
    print("="*65)

    trade_log_2025 = run_backtest(
        'gap_fill', 'BANKNIFTY',
        '2025-01-01', '2025-12-31',
        groww=groww,
        use_futures=True,
        apply_macros=True,
    )

    if trade_log_2025.empty:
        print("❌ No 2025 trades — check data availability.")
        return None

    print_strategy_report(trade_log_2025, strategy_name='GAP FILL — 2025 OOS')

    avg_per_trade = trade_log_2025['pnl_rs'].mean()
    print(f"\n  ── Decision gate ──────────────────────────────────────")
    if avg_per_trade >= HEALTHY_THRESHOLD:
        print(f"  ✅ avg/trade ₹{avg_per_trade:,.0f} ≥ ₹{HEALTHY_THRESHOLD} → "
              f"HEALTHY — proceed to Priority 3 (combine all strategies)")
    elif avg_per_trade >= MARGINAL_THRESHOLD:
        print(f"  ⚠  avg/trade ₹{avg_per_trade:,.0f} in ₹{MARGINAL_THRESHOLD}–{HEALTHY_THRESHOLD} → "
              f"MARGINAL — run Priority 2 (parameter sweep) first")
    else:
        print(f"  ❌ avg/trade ₹{avg_per_trade:,.0f} < ₹{MARGINAL_THRESHOLD} → "
              f"DEGRADED — focus resources on ORB + VWAP (Priority 3)")

    return trade_log_2025


# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 2 — Parameter sensitivity sweep (detect drift)
# ══════════════════════════════════════════════════════════════════════════════

def run_priority_2(groww, data=None):
    """
    Run gap fill parameter sweep independently per year.
    If optimal params shift year-over-year → confirmed parameter drift.

    Pass pre-fetched data to avoid re-fetching (saves ~3 minutes).
    """
    from strategies.gap_fill import gap_fill_parameter_sweep
    from config import INSTRUMENTS

    print("\n" + "="*65)
    print("  PRIORITY 2: PARAMETER SENSITIVITY SWEEP")
    print("="*65)

    instrument_config = INSTRUMENTS['BANKNIFTY']

    if data is None:
        print("Fetching BANKNIFTY futures 2022-2024 for sweep...")
        data = fetch_instrument('BANKNIFTY', '2022-01-01', '2024-12-31',
                                groww=groww, use_futures=True)
        if data.empty:
            print("❌ Data fetch failed.")
            return {}

    sweeps = {}
    for year in [2022, 2023, 2024]:
        print(f"\n  ── Year {year} ──")
        year_data = data[data.index.year == year]
        if year_data.empty:
            print(f"  No data for {year}")
            continue
        sweep = gap_fill_parameter_sweep(year_data, instrument_config)
        sweeps[year] = sweep

    # Cross-year optimal params summary
    print(f"\n  ── Optimal params by year ──────────────────────────────────")
    print(f"  {'Year':<6} {'Best STEP':>10} {'Best STOP':>10} {'AvgP&L':>10} {'WinRate':>9}")
    print(f"  {'-'*50}")
    for yr, sw in sweeps.items():
        if sw.empty:
            continue
        best = sw.iloc[0]
        print(f"  {yr:<6} {best['STEP_PTS']:>10.0f} {best['STOP_PTS']:>10.0f} "
              f"₹{best['avg_pl']:>8,.0f} {best['win_rate']:>8.1f}%")

    drift_detected = False
    if len(sweeps) >= 2:
        years = sorted(sweeps.keys())
        first_best_step = sweeps[years[0]].iloc[0]['STEP_PTS'] if not sweeps[years[0]].empty else None
        last_best_step  = sweeps[years[-1]].iloc[0]['STEP_PTS'] if not sweeps[years[-1]].empty else None
        if first_best_step and last_best_step and abs(first_best_step - last_best_step) >= 25:
            drift_detected = True

    if drift_detected:
        print(f"\n  ⚠  Parameter drift detected — optimal STEP_PTS shifted "
              f"across years. Consider adaptive params or walkforward optimisation.")
    else:
        print(f"\n  ✅ Params appear stable across years.")

    return sweeps


# ══════════════════════════════════════════════════════════════════════════════
# PRIORITY 3 — ORB + VWAP backtests, then combine
# ══════════════════════════════════════════════════════════════════════════════

def run_priority_3(groww, gap_fill_log=None):
    """
    Run ORB and VWAP backtests on 2022-2024, then combine all three strategies
    into a capital-aware portfolio P&L.

    Pass gap_fill_log if already computed to avoid re-running.
    """
    print("\n" + "="*65)
    print("  PRIORITY 3: ORB + VWAP BACKTESTS + COMBINED PORTFOLIO")
    print("="*65)

    START = '2022-01-01'
    END   = '2024-12-31'

    # ── Gap Fill (re-run if not provided) ─────────────────────────────────────
    if gap_fill_log is None:
        print("\n── Gap Fill (2022-2024) ──")
        gap_fill_log = run_backtest('gap_fill', 'BANKNIFTY', START, END,
                                    groww=groww, use_futures=True, apply_macros=True)
    else:
        print(f"\n── Gap Fill: using pre-computed log ({len(gap_fill_log)} trades) ──")

    # ── ORB ───────────────────────────────────────────────────────────────────
    print("\n── ORB (2022-2024) ──")
    orb_log = run_backtest('orb', 'BANKNIFTY', START, END,
                           groww=groww, use_futures=True, apply_macros=True)

    if not orb_log.empty:
        print_strategy_report(orb_log, strategy_name='ORB 2022-2024')

    # ── VWAP Reversion ────────────────────────────────────────────────────────
    print("\n── VWAP Reversion (2022-2024) ──")
    vwap_log = run_backtest('vwap_reversion', 'BANKNIFTY', START, END,
                            groww=groww, use_futures=True, apply_macros=True)

    if not vwap_log.empty:
        print_strategy_report(vwap_log, strategy_name='VWAP REVERSION 2022-2024')

    # ── Combined portfolio ────────────────────────────────────────────────────
    print("\n── Combined Portfolio ──")
    strategy_results = {}
    if gap_fill_log is not None and not gap_fill_log.empty:
        strategy_results['gap_fill'] = gap_fill_log
    if not orb_log.empty:
        strategy_results['orb'] = orb_log
    if not vwap_log.empty:
        strategy_results['vwap'] = vwap_log

    if len(strategy_results) >= 2:
        from config import CAPITAL as CAPITAL_CFG
        combined = combine_strategies(strategy_results,
                                      capital=CAPITAL_CFG.get('starting', 1_00_000))
        print_combined_report(combined, capital=CAPITAL_CFG.get('starting', 1_00_000))
        return combined, orb_log, vwap_log
    else:
        print("⚠ Not enough strategies to combine.")
        return None, orb_log, vwap_log


# ══════════════════════════════════════════════════════════════════════════════
# Full run helper
# ══════════════════════════════════════════════════════════════════════════════

def run_all(groww, gap_fill_log_2022_2024=None):
    """
    Run all priorities in order. Pass existing gap_fill_log to skip refetch.

    Example:
        from run_next_steps import run_all
        run_all(groww, gap_fill_log_2022_2024=trade_log)
    """
    print("\n" + "█"*65)
    print("  HAWALA v2 — NEXT PHASE ANALYSIS")
    print("█"*65)

    # P1: OOS 2025
    log_2025 = run_priority_1(groww)

    # P2: Parameter sweep on 2022-2024 data
    sweeps = run_priority_2(groww)

    # P3: ORB + VWAP + combine
    combined, orb_log, vwap_log = run_priority_3(
        groww, gap_fill_log=gap_fill_log_2022_2024
    )

    return {
        'gap_fill_2025':  log_2025,
        'param_sweeps':   sweeps,
        'orb_log':        orb_log,
        'vwap_log':       vwap_log,
        'combined':       combined,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Quick reference — minimal one-liners
# ══════════════════════════════════════════════════════════════════════════════

QUICK_REFERENCE = """
# ── P1: OOS 2025 test ────────────────────────────────────────────────────────
from run_next_steps import run_priority_1
log_2025 = run_priority_1(groww)

# ── P2: Parameter sweep (pass pre-fetched data to skip API calls) ────────────
from run_next_steps import run_priority_2
sweeps = run_priority_2(groww, data=data)          # data = futures 2022-2024

# ── P2: Manual per-year sweep ────────────────────────────────────────────────
from strategies.gap_fill import gap_fill_parameter_sweep
from config import INSTRUMENTS
cfg = INSTRUMENTS['BANKNIFTY']
sweep_2022 = gap_fill_parameter_sweep(data[data.index.year == 2022], cfg)
sweep_2023 = gap_fill_parameter_sweep(data[data.index.year == 2023], cfg)
sweep_2024 = gap_fill_parameter_sweep(data[data.index.year == 2024], cfg)

# ── P3: ORB + VWAP + combine ─────────────────────────────────────────────────
from run_next_steps import run_priority_3
combined, orb_log, vwap_log = run_priority_3(groww, gap_fill_log=trade_log)

# ── ORB sweep (standalone) ────────────────────────────────────────────────────
from strategies.orb import orb_parameter_sweep
orb_sweep = orb_parameter_sweep(data, INSTRUMENTS['BANKNIFTY'])

# ── VWAP sweep (standalone) ───────────────────────────────────────────────────
from strategies.vwap_reversion import vwap_parameter_sweep
vwap_sweep = vwap_parameter_sweep(data, INSTRUMENTS['BANKNIFTY'])

# ── Run everything at once ────────────────────────────────────────────────────
from run_next_steps import run_all
results = run_all(groww, gap_fill_log_2022_2024=trade_log)
"""

if __name__ == '__main__':
    print(QUICK_REFERENCE)
