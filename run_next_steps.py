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
# ORB ATR sweep — validate the structural fix
# ══════════════════════════════════════════════════════════════════════════════

def run_orb_atr_sweep(data=None, groww=None):
    """
    Run the new ATR-based ORB sweep and compare against legacy range-based results.

    Pass pre-fetched futures data to skip API calls:
        run_orb_atr_sweep(data=data)

    Or let it fetch:
        run_orb_atr_sweep(groww=groww)
    """
    from strategies.orb import orb_parameter_sweep
    from config import INSTRUMENTS

    cfg = INSTRUMENTS['BANKNIFTY']

    if data is None:
        if groww is None:
            raise ValueError("Pass either data= or groww=")
        print("Fetching BANKNIFTY futures 2022-2024...")
        data = fetch_instrument('BANKNIFTY', '2022-01-01', '2024-12-31',
                                groww=groww, use_futures=True)

    print("\n" + "="*65)
    print("  ORB ATR STOP SWEEP (vs legacy range-based stops)")
    print("="*65)

    # ATR mode only (faster; legacy already confirmed negative)
    sweep = orb_parameter_sweep(data, cfg, mode='atr')

    if sweep.empty:
        print("❌ No results.")
        return sweep

    best = sweep.iloc[0]
    print(f"\n  ── Best ATR combo ──────────────────────────────────────────")
    print(f"  Window: {best['window']} | stop_atr: {best['stop_atr']:.2f} | "
          f"target_atr: {best['target_atr']:.2f} | buffer: {best['buffer']:.0f}")
    print(f"  {best['trades']:.0f} trades | {best['win_rate']:.1f}% WR | "
          f"₹{best['total_pl']:,.0f} total | ₹{best['avg_pl']:,.0f} avg/trade")

    # Update config suggestion
    if best['total_pl'] > 0:
        print(f"\n  ✅ ATR stops are profitable. Update config.py:")
        print(f"     'ORB_STOP_ATR':   {best['stop_atr']}")
        print(f"     'ORB_TARGET_ATR': {best['target_atr']}")
        print(f"     'ORB_WINDOW_END': '{best['window']}'")
        print(f"     'ORB_BREAKOUT_BUFFER': {best['buffer']:.0f}")
    else:
        print(f"\n  ❌ Even ATR stops can't find edge on BANKNIFTY ORB. "
              f"Consider NIFTY or dropping ORB entirely.")

    return sweep


# ══════════════════════════════════════════════════════════════════════════════
# NIFTY calibration sweeps
# ══════════════════════════════════════════════════════════════════════════════

def run_nifty_calibration(nifty_data=None, groww=None):
    """
    Run gap_fill and VWAP parameter sweeps on NIFTY data to find
    NIFTY-specific optimal params, then print recommended config updates.

    NIFTY at ₹19k needs different absolute STEP_PTS/STOP_PTS/band than
    BANKNIFTY at ₹40k. This replaces the placeholder values in config.py.

    Usage:
        # Option A: pass pre-fetched data (faster — no API calls)
        run_nifty_calibration(nifty_data=nifty_data)

        # Option B: let it fetch
        run_nifty_calibration(groww=groww)
    """
    from strategies.gap_fill import gap_fill_parameter_sweep
    from strategies.vwap_reversion import vwap_parameter_sweep
    from config import INSTRUMENTS

    cfg = INSTRUMENTS['NIFTY']

    if nifty_data is None:
        if groww is None:
            raise ValueError("Pass either nifty_data= or groww=")
        print("Fetching NIFTY futures 2022-2024...")
        nifty_data = fetch_instrument('NIFTY', '2022-01-01', '2024-12-31',
                                      groww=groww, use_futures=True)
        if nifty_data.empty:
            print("❌ NIFTY data fetch failed.")
            return {}

    print("\n" + "="*65)
    print("  NIFTY CALIBRATION — Gap Fill Parameter Sweep")
    print("="*65)
    gf_sweep = gap_fill_parameter_sweep(nifty_data, cfg)

    print("\n" + "="*65)
    print("  NIFTY CALIBRATION — VWAP Parameter Sweep")
    print("="*65)
    vw_sweep = vwap_parameter_sweep(nifty_data, cfg)

    # Print recommended config updates
    print("\n" + "="*65)
    print("  RECOMMENDED config.py updates for NIFTY")
    print("="*65)

    if not gf_sweep.empty:
        best_gf = gf_sweep.iloc[0]
        print(f"\n  Gap Fill (best: ₹{best_gf['total_pl']:,.0f} total, "
              f"{best_gf['win_rate']:.1f}% WR):")
        print(f"    'strategy_params': {{")
        print(f"        'STEP_PTS': {best_gf['STEP_PTS']:.0f},")
        print(f"        'STOP_PTS': {best_gf['STOP_PTS']:.0f},")
        print(f"    }}")
        if best_gf['total_pl'] < 0:
            print(f"  ⚠  Best combo still negative — drop NIFTY gap fill entirely.")

    if not vw_sweep.empty:
        best_vw = vw_sweep.iloc[0]
        print(f"\n  VWAP (best: ₹{best_vw['total_pl']:,.0f} total, "
              f"{best_vw['win_rate']:.1f}% WR, {best_vw['trades']:.0f} trades):")
        print(f"    Add to NIFTY 'strategy_params':")
        print(f"        'VWAP_BAND_PCT':   {best_vw['band_pct']/100:.4f},")
        print(f"        'VWAP_STOP_ATR':   {best_vw['stop_atr']:.2f},")
        print(f"        'VWAP_TARGET_ATR': {best_vw['target_atr']:.2f},")

    return {'gap_fill_sweep': gf_sweep, 'vwap_sweep': vw_sweep}


# ══════════════════════════════════════════════════════════════════════════════
# NIFTY validation — parallel instrument backtest
# ══════════════════════════════════════════════════════════════════════════════

def run_nifty_validation(groww):
    """
    Run all three strategies on NIFTY 2022-2024 to check if:
    1. Gap fill edge survived longer on NIFTY (less HFT, different composition)
    2. ORB with ATR stops works on NIFTY
    3. VWAP reversion on no-gap days works on NIFTY

    This is the instrument diversification test.
    """
    print("\n" + "="*65)
    print("  NIFTY VALIDATION — INSTRUMENT DIVERSIFICATION TEST")
    print("="*65)

    START = '2022-01-01'
    END   = '2024-12-31'
    logs  = {}

    for strategy in ('gap_fill', 'orb', 'vwap_reversion'):
        print(f"\n── NIFTY {strategy} (2022-2024) ──")
        log = run_backtest(strategy, 'NIFTY', START, END,
                           groww=groww, use_futures=True, apply_macros=True)
        if not log.empty:
            print_strategy_report(log, strategy_name=f'NIFTY {strategy.upper()}')
            logs[strategy] = log
        else:
            print(f"  ⚠ No trades for {strategy} on NIFTY")

    if len(logs) >= 2:
        from config import CAPITAL as CAPITAL_CFG
        print("\n── NIFTY Combined ──")
        combined = combine_strategies(logs,
                                      capital=CAPITAL_CFG.get('starting', 1_00_000))
        print_combined_report(combined, capital=CAPITAL_CFG.get('starting', 1_00_000))
        logs['combined'] = combined

    return logs


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
# ── ORB ATR sweep (validate structural fix — no API needed if data is loaded) ─
from run_next_steps import run_orb_atr_sweep
orb_sweep = run_orb_atr_sweep(data=data)           # data = futures 2022-2024

# ── Re-run combined portfolio with fixed ORB + optimal VWAP ──────────────────
from run_next_steps import run_priority_3
combined, orb_log, vwap_log = run_priority_3(groww, gap_fill_log=None)
# (pass gap_fill_log=None to skip dead gap fill; ORB+VWAP only)

# ── NIFTY validation (different instrument, same strategies) ─────────────────
from run_next_steps import run_nifty_validation
nifty_logs = run_nifty_validation(groww)

# ── VWAP sweep — fine-tune band for NIFTY (different price level) ─────────────
from strategies.vwap_reversion import vwap_parameter_sweep
from config import INSTRUMENTS
nifty_data = fetch_instrument('NIFTY', '2022-01-01', '2024-12-31', groww=groww, use_futures=True)
vwap_sweep_nifty = vwap_parameter_sweep(nifty_data, INSTRUMENTS['NIFTY'])

# ── ORB ATR sweep — also on NIFTY ─────────────────────────────────────────────
from strategies.orb import orb_parameter_sweep
orb_sweep_nifty = orb_parameter_sweep(nifty_data, INSTRUMENTS['NIFTY'], mode='atr')

# ── BANKNIFTY ORB + VWAP combined (2022-2024, no gap fill) ───────────────────
from run_next_steps import run_priority_3
combined_bn, orb_bn, vwap_bn = run_priority_3(groww, gap_fill_log=None)
"""

if __name__ == '__main__':
    print(QUICK_REFERENCE)
