# ============================================================
# CELL 8 — Unified Regime-Based Trading Framework
# ============================================================
# Combines all strategies into a single daily decision engine.
#
# Decision logic per day:
#
#   Step 1 — Macro gate (from Cell 5):
#       If 2+ macro filters fire → skip day entirely
#
#   Step 2 — Gap check:
#       If gap < MIN_GAP_PTS → no gap trade; check VWAP only
#       If gap > MAX_GAP_PTS → fundamental move; skip all strategies
#
#   Step 3 — Gap fill attempt (Cell 3 logic):
#       Enter gap fill trade at open
#       Monitor for fill OR stop loss through ORB window (09:15–09:45)
#
#   Step 4 — ORB regime check (if gap fill stopped out before 09:45):
#       If gap DID NOT fill by 09:45 → switch to ORB mode
#       ORB trade fires if breakout confirmed after 09:45
#
#   Step 5 — No-gap days → VWAP reversion (Cell 9)
#
# On any given day, at most ONE strategy fires.
# Gap fill and ORB are mutually exclusive by design.
#
# Summary table produced at end shows:
#   Per year: gap fill trades, ORB trades, VWAP trades,
#             combined P&L, combined win rate
# ============================================================

import pandas as pd
import numpy as np


def run_combined_backtest(gap_df,
                          orb_df,
                          vwap_df=None,
                          macro_df=None):
    """
    Combine gap fill, ORB, and VWAP results into a unified P&L view.

    gap_df  : output of run_gap_fill() from Cell 3
    orb_df  : output of run_orb() from Cell 7
    vwap_df : output of run_vwap_reversion() from Cell 9 (optional)
    macro_df: output of run_macro_backtest() from Cell 5 with trade_ok col

    Strategy assignment per day (priority order):
      1. If macro gate fires → no trade (skip)
      2. If gap fill trade exists (gap_df row) AND trade_ok → gap fill wins
      3. If gap fill was stopped out before ORB window ends → ORB takes over
         (orb_df already handles this via gap_filled_early check)
      4. If no gap → VWAP (if vwap_df provided)

    In practice, since ORB and gap fill run independently and use the
    gap_filled_early guard, the deduplication is:
      - Any date in both gap_df and orb_df is a conflict → gap fill wins
        (gap fill entered at open, ORB checks for fill before firing)
      - Any date in neither → VWAP (if available)

    Returns:
        pd.DataFrame: Combined trade log with strategy column
    """
    frames = []

    # ── Macro gate setup + diagnostic ────────────────────────────────────
    if macro_df is not None and 'trade_ok' in macro_df.columns:
        macro_gate = macro_df[['date', 'trade_ok']].drop_duplicates('date')
        total_gap  = len(macro_df)
        filtered   = (~macro_df['trade_ok']).sum()
        print(f"  Macro gate: {filtered}/{total_gap} gap days blocked "
              f"({filtered/total_gap*100:.1f}%)")

        # Per-year diagnostic — shows whether filter is actually doing anything
        if 'year' in macro_df.columns:
            print(f"  {'Year':<6} {'Total':>7} {'Blocked':>8} {'PassRate':>9}")
            for yr in sorted(macro_df['year'].unique()):
                y   = macro_df[macro_df['year'] == yr]
                blk = (~y['trade_ok']).sum()
                print(f"  {yr:<6} {len(y):>7}  {blk:>8}  "
                      f"{(len(y)-blk)/len(y)*100:>8.1f}%")

        # Critical warning: if FII data absent, 2-of-3 needs VIX+S&P overlap.
        # That's rare — filter will be near-zero for pre-2024 years.
        # This is correct behaviour but worth flagging explicitly.
        if 'filter_fpi' in macro_df.columns:
            fpi_has_data = macro_df['filter_fpi'].any()
            if not fpi_has_data:
                print(f"\n  ⚠  FPI filter has no data — 2-of-3 voting falls back")
                print(f"     to VIX+S&P agreement only. Pre-2024 years may show")
                print(f"     near-zero filtering. This is correct behaviour.")
    else:
        macro_gate = None
        print(f"  ⚠  macro_df not provided — no macro gate applied.")
        print(f"     Run cell_5 first and ensure macro_df is in scope.")

    # ── Gap fill trades ───────────────────────────────────────────────────
    gf = gap_df.copy()
    gf['strategy'] = 'GAP_FILL'

    if macro_gate is not None:
        gf = gf.merge(macro_gate, on='date', how='left')
        gf['trade_ok'] = gf['trade_ok'].fillna(True)
        gf_active = gf[gf['trade_ok']].drop(columns=['trade_ok'])
    else:
        gf_active = gf

    frames.append(gf_active.assign(source='gap_fill'))
    gap_fill_dates = set(gf_active['date'])

    # ── ORB trades (only on days NOT already claimed by gap fill) ─────────
    if orb_df is not None and not orb_df.empty:
        ob = orb_df.copy()
        ob = ob[~ob['date'].isin(gap_fill_dates)]  # dedup
        if macro_gate is not None:
            ob = ob.merge(macro_gate, on='date', how='left')
            ob['trade_ok'] = ob['trade_ok'].fillna(True)
            ob = ob[ob['trade_ok']].drop(columns=['trade_ok'])
        frames.append(ob.assign(source='orb'))
        orb_dates = set(ob['date'])
    else:
        orb_dates = set()

    # ── VWAP trades (only on days with no gap = not in gap_df at all) ─────
    if vwap_df is not None and not vwap_df.empty:
        vw = vwap_df.copy()
        all_gap_dates = set(gap_df['date'])  # all gap days, not just traded
        vw = vw[~vw['date'].isin(all_gap_dates)]  # VWAP = no-gap days only
        if macro_gate is not None:
            vw = vw.merge(macro_gate, on='date', how='left')
            vw['trade_ok'] = vw['trade_ok'].fillna(True)
            vw = vw[vw['trade_ok']].drop(columns=['trade_ok'])
        frames.append(vw.assign(source='vwap'))

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined  = combined.sort_values('date').reset_index(drop=True)

    return combined


def print_combined_report(combined_df):
    """
    Print the unified P&L report across all strategies and years.
    """
    if combined_df.empty:
        print("No combined trades.")
        return

    print(f"\n{'='*70}")
    print(f"  COMBINED REGIME FRAMEWORK — FULL BACKTEST REPORT")
    print(f"{'='*70}")

    # ── Overall summary ───────────────────────────────────────────────────
    strats = combined_df['source'].value_counts()
    total_pl = combined_df['pnl_rs'].sum()
    total_wr = combined_df['win'].mean() * 100
    total_n  = len(combined_df)

    print(f"\n  Strategy mix:")
    for strat, count in strats.items():
        sub = combined_df[combined_df['source'] == strat]
        pl  = sub['pnl_rs'].sum()
        wr  = sub['win'].mean() * 100
        ap  = sub['pnl_rs'].mean()
        print(f"    {strat:<12}: {count:4d} trades | "
              f"Win {wr:.1f}% | ₹{pl:>10,.0f} | Avg ₹{ap:>6,.0f}/trade")

    print(f"\n  Overall : {total_n} trades | Win {total_wr:.1f}% | "
          f"₹{total_pl:,.0f}")

    # ── Year-by-year breakdown ────────────────────────────────────────────
    print(f"\n  Year-by-year (all strategies combined):")
    print(f"  {'Year':<6} {'GapFill':>10} {'ORB':>10} {'VWAP':>10} "
          f"{'Total':>10} {'WinRate':>8} {'TotalP&L':>12}")
    print(f"  {'-'*68}")

    for yr in sorted(combined_df['year'].unique()):
        y     = combined_df[combined_df['year'] == yr]
        gf_pl = y[y['source'] == 'gap_fill']['pnl_rs'].sum()
        ob_pl = y[y['source'] == 'orb']['pnl_rs'].sum()
        vw_pl = y[y['source'] == 'vwap']['pnl_rs'].sum() \
                if 'vwap' in y['source'].values else 0
        yr_pl = y['pnl_rs'].sum()
        yr_wr = y['win'].mean() * 100
        yr_n  = len(y)
        print(f"  {yr:<6} ₹{gf_pl:>8,.0f}  ₹{ob_pl:>8,.0f}  "
              f"₹{vw_pl:>8,.0f}  {yr_n:>5}tr  "
              f"{yr_wr:>7.1f}%  ₹{yr_pl:>10,.0f}")

    print(f"\n  {'TOTAL':<6} {'':>10} {'':>10} {'':>10} "
          f"{total_n:>5}tr  {total_wr:>7.1f}%  ₹{total_pl:>10,.0f}")

    # ── Monthly P&L heatmap (text) ────────────────────────────────────────
    print(f"\n  Monthly P&L (₹):")
    combined_df['month'] = pd.to_datetime(combined_df['date']).dt.month
    pivot = combined_df.groupby(['year', 'month'])['pnl_rs'].sum().unstack(fill_value=0)
    month_names = ['Jan','Feb','Mar','Apr','May','Jun',
                   'Jul','Aug','Sep','Oct','Nov','Dec']
    header = f"  {'Year':<6} " + "".join(f"{month_names[m-1]:>8}" for m in pivot.columns)
    print(header)
    print(f"  {'-'*(6 + 8*len(pivot.columns))}")
    for yr_idx in pivot.index:
        row = f"  {yr_idx:<6} "
        for m in pivot.columns:
            val = pivot.loc[yr_idx, m]
            row += f"₹{val/1000:>6.1f}K" if abs(val) >= 1000 else f"{'—':>8}"
        print(row)

    # ── Drawdown analysis ─────────────────────────────────────────────────
    cumulative = combined_df['pnl_rs'].cumsum()
    running_max= cumulative.cummax()
    drawdown   = cumulative - running_max
    max_dd     = drawdown.min()
    max_dd_idx = drawdown.idxmin()
    max_dd_date= combined_df.loc[max_dd_idx, 'date'] if max_dd_idx in combined_df.index else '—'

    print(f"\n  Risk metrics:")
    print(f"    Max drawdown    : ₹{max_dd:,.0f}  (at {max_dd_date})")
    print(f"    Total P&L       : ₹{total_pl:,.0f}")
    print(f"    Profit factor   : {combined_df[combined_df['pnl_rs']>0]['pnl_rs'].sum() / abs(combined_df[combined_df['pnl_rs']<0]['pnl_rs'].sum()):.2f}x")
    avg_win    = combined_df[combined_df['win']==1]['pnl_rs'].mean()
    avg_loss   = combined_df[combined_df['win']==0]['pnl_rs'].mean()
    print(f"    Avg win         : ₹{avg_win:,.0f}")
    print(f"    Avg loss        : ₹{avg_loss:,.0f}")
    print(f"    Win/Loss ratio  : {abs(avg_win/avg_loss):.2f}x")


# ── Main execution ────────────────────────────────────────────────────────────
print("Building combined regime framework...\n")

_vwap_input  = vwap_df   if 'vwap_df'  in dir() else None
_macro_input = macro_df  if 'macro_df' in dir() else None
_orb_input   = orb_df    if 'orb_df'   in dir() else None

combined_df = run_combined_backtest(
    gap_df   = gap_df,
    orb_df   = _orb_input,
    vwap_df  = _vwap_input,
    macro_df = _macro_input
)

print_combined_report(combined_df)

print("\n✅ Cell 8 complete.")
print("   combined_df available — full trade log across all strategies")
