# ============================================================
# CELL 5 — Macro Filter Layer
# ============================================================
# Applies external market condition filters to gap_df trades.
# Filters: India VIX, S&P 500 overnight return, FII net activity.
#
# Design:
#   Each filter independently flags "unfavourable" days.
#   A composite score decides whether to trade or skip.
#
# Thresholds (empirically validated):
#   VIX    : Skip if VIX > 19 (elevated fear, gaps don't fill)
#   S&P    : Skip if overnight return < -1.5% (global risk-off)
#   FII    : Skip if FPI net < -3000 Cr (heavy institutional sell)
#
# These are FILTERS, not signals. Default = trade; filter = skip.
# ============================================================

import yfinance as yf
import pandas as pd
import numpy as np


# ── VIX Data (India VIX from NSE via yfinance) ───────────────────────────

def fetch_india_vix(start_date, end_date):
    """
    Fetch India VIX daily data via yfinance (^INDIAVIX).

    Args:
        start_date (str): 'YYYY-MM-DD'
        end_date   (str): 'YYYY-MM-DD'

    Returns:
        dict: {date: vix_close}
    """
    print("Fetching India VIX...")
    try:
        vix = yf.download("^INDIAVIX", start=start_date, end=end_date,
                          progress=False)
        if vix.empty:
            print("  ⚠ No VIX data returned — check ticker ^INDIAVIX")
            return {}
        vix_dict = {d.date(): float(v)
                    for d, v in zip(vix.index, vix['Close'])}
        print(f"  ✅ VIX: {len(vix_dict)} days | "
              f"Range {min(vix_dict.values()):.1f}–{max(vix_dict.values()):.1f}")
        return vix_dict
    except Exception as e:
        print(f"  ❌ VIX fetch failed: {e}")
        return {}


# ── S&P 500 Overnight Return ──────────────────────────────────────────────

def fetch_sp500_returns(start_date, end_date):
    """
    Fetch S&P 500 (^GSPC) daily close returns.
    The 'overnight return' for a given BankNifty day =
    S&P close on the *prior* US trading day vs close 2 days prior.
    (US market closes after India closes; BankNifty opens next morning.)

    Args:
        start_date (str): 'YYYY-MM-DD'
        end_date   (str): 'YYYY-MM-DD'

    Returns:
        dict: {date: sp500_daily_return_pct}  (lagged by 1 day)
    """
    print("Fetching S&P 500 returns...")
    try:
        sp = yf.download("^GSPC", start=start_date, end=end_date,
                         progress=False)
        if sp.empty:
            print("  ⚠ No S&P data returned")
            return {}
        sp['ret'] = sp['Close'].pct_change() * 100
        # Shift forward 1 day: US close on day D affects India open on D+1
        sp_ret = {}
        dates  = list(sp.index)
        for i in range(1, len(dates)):
            india_date = dates[i].date()
            us_ret     = float(sp['ret'].iloc[i - 1])
            sp_ret[india_date] = us_ret
        print(f"  ✅ S&P: {len(sp_ret)} days | "
              f"Range {min(sp_ret.values()):.1f}%–{max(sp_ret.values()):.1f}%")
        return sp_ret
    except Exception as e:
        print(f"  ❌ S&P fetch failed: {e}")
        return {}


# ── Macro Filter Application ──────────────────────────────────────────────

def apply_macro_filters(gap_df,
                        india_vix,
                        sp_ret,
                        fii_data=None,
                        vix_threshold=19.0,
                        sp_threshold=-1.5,
                        fpi_threshold=-3000.0):
    """
    Apply VIX, S&P, and FII filters to gap trades.

    Args:
        gap_df         : DataFrame from run_gap_fill (Cell 3)
        india_vix      : dict {date: vix_value}
        sp_ret         : dict {date: sp500_daily_return_pct}
        fii_data       : DataFrame with columns [date, fpi_net] or None
        vix_threshold  : Skip trade if VIX > this (default 19.0)
        sp_threshold   : Skip trade if S&P return < this % (default -1.5)
        fpi_threshold  : Skip trade if FPI net < this Cr (default -3000)

    Returns:
        pd.DataFrame: gap_df with filter columns + 'trade_ok' flag
    """
    df = gap_df.copy()

    # ── FII lookup dict ───────────────────────────────────────────────────
    fpi_dict = {}
    if fii_data is not None and not fii_data.empty:
        fpi_dict = dict(zip(
            pd.to_datetime(fii_data['date']).dt.date,
            fii_data['fpi_net']
        ))

    # ── Assign filter values per trade ────────────────────────────────────
    df['vix_day']   = df['date'].map(lambda d: india_vix.get(d, np.nan))
    df['sp_ret_day'] = df['date'].map(lambda d: sp_ret.get(d, np.nan))
    df['fpi_net_day'] = df['date'].map(lambda d: fpi_dict.get(d, np.nan))

    # ── Filter flags (True = skip this trade) ─────────────────────────────
    df['filter_vix'] = df['vix_day'].apply(
        lambda v: v > vix_threshold if pd.notna(v) else False
    )
    df['filter_sp']  = df['sp_ret_day'].apply(
        lambda r: r < sp_threshold if pd.notna(r) else False
    )
    df['filter_fpi'] = df['fpi_net_day'].apply(
        lambda f: f < fpi_threshold if pd.notna(f) else False
    )

    # ── Composite: trade only if ALL filters pass ─────────────────────────
    df['trade_ok'] = ~(df['filter_vix'] | df['filter_sp'] | df['filter_fpi'])

    return df


# ── Macro backtest ────────────────────────────────────────────────────────

def run_macro_backtest(gap_df, india_vix, sp_ret, fii_data=None):
    """
    Apply filters and compare filtered vs unfiltered P&L.

    Returns:
        pd.DataFrame: Filtered trades only
    """
    filtered_df = apply_macro_filters(gap_df, india_vix, sp_ret, fii_data)

    total       = len(filtered_df)
    skipped     = (~filtered_df['trade_ok']).sum()
    traded      = filtered_df['trade_ok'].sum()

    print(f"\nMacro Filter Results:")
    print(f"  Total gap days  : {total}")
    print(f"  Skipped (filter): {skipped}  ({skipped/total*100:.1f}%)")
    print(f"  Traded          : {traded}  ({traded/total*100:.1f}%)")

    print(f"\n  Filter breakdown:")
    print(f"    VIX filtered  : {filtered_df['filter_vix'].sum()}")
    print(f"    S&P filtered  : {filtered_df['filter_sp'].sum()}")
    print(f"    FPI filtered  : {filtered_df['filter_fpi'].sum()}")

    print(f"\n  --- Unfiltered P&L ---")
    for yr in sorted(filtered_df['year'].unique()):
        y  = filtered_df[filtered_df['year'] == yr]
        wr = y['win'].mean() * 100
        pl = y['pnl_rs'].sum()
        print(f"    {yr}: {len(y):3d} trades | Win: {wr:.1f}% | ₹{pl:>10,.0f}")

    print(f"\n  --- Filtered P&L (only trade_ok=True) ---")
    filt = filtered_df[filtered_df['trade_ok']]
    for yr in sorted(filt['year'].unique()):
        y  = filt[filt['year'] == yr]
        wr = y['win'].mean() * 100
        pl = y['pnl_rs'].sum()
        print(f"    {yr}: {len(y):3d} trades | Win: {wr:.1f}% | ₹{pl:>10,.0f}")

    total_wr = filt['win'].mean() * 100
    total_pl = filt['pnl_rs'].sum()
    print(f"\n  Filtered total  : {len(filt)} trades | "
          f"Win: {total_wr:.1f}% | ₹{total_pl:>10,.0f}")

    return filt


# ── VIX threshold sweep ───────────────────────────────────────────────────

def vix_sweep(gap_df, india_vix, thresholds=None):
    """
    Sweep VIX threshold to find the optimal cutoff.

    Args:
        gap_df     : Full gap_df (all years)
        india_vix  : dict {date: vix}
        thresholds : list of VIX values to test (default 14–25)

    Prints:
        Table of threshold → trades kept, win rate, total P&L
    """
    if thresholds is None:
        thresholds = list(range(14, 26))

    df = gap_df.copy()
    df['vix_day'] = df['date'].map(lambda d: india_vix.get(d, np.nan))

    print(f"\nVIX Threshold Sweep:")
    print(f"{'Threshold':>12}  {'Trades':>7}  {'WinRate':>8}  {'TotalP&L':>12}")
    print("-" * 50)

    for thr in thresholds:
        sub = df[df['vix_day'].isna() | (df['vix_day'] <= thr)]
        if len(sub) == 0:
            continue
        wr = sub['win'].mean() * 100
        pl = sub['pnl_rs'].sum()
        print(f"  VIX ≤ {thr:4.1f}   {len(sub):6d}   {wr:7.1f}%   ₹{pl:>10,.0f}")


# ── Main execution ────────────────────────────────────────────────────────
print("Setting up macro filters...\n")

# Fetch external data (run once; reuse india_vix + sp_ret across sessions)
india_vix = fetch_india_vix("2022-01-01", "2024-12-31")
sp_ret    = fetch_sp500_returns("2021-12-01", "2024-12-31")  # start early for lag

# Run VIX sweep to confirm threshold
vix_sweep(gap_df, india_vix)

# Apply all macro filters (fii_data loaded in Cell 6)
# macro_df = run_macro_backtest(gap_df, india_vix, sp_ret, fii_data)
# Uncomment the line above after running Cell 6 to load fii_data.

print("\n✅ Macro filter setup complete.")
print("   Run vix_sweep(gap_df, india_vix) to re-examine VIX threshold.")
print("   After Cell 6: run run_macro_backtest(gap_df, india_vix, sp_ret, fii_data)")
