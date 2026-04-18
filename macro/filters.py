# ============================================================
# macro/filters.py — Macro Filter Layer  (v2)
# ============================================================
# Applies external market condition filters to any trade_log.
# Reusable across all strategies — not gap-fill specific.
#
# Filters  : India VIX, S&P 500 overnight return, FII net activity
# Regime   : Brent crude overnight move, USD/INR overnight move
#
# v2 Changes:
#   - Attribution analysis: shows P&L of skipped trades per filter
#     (revealed original OR logic was net negative — killing good trades)
#   - min_filters param: require N-of-3 filters before skipping
#     (default = 2; OR=1 was too aggressive)
#   - Threshold sweeps for all 3 filters (not just VIX)
#   - Brent crude + USD/INR as REGIME CLASSIFIERS, not blocking filters
#     (used to tag day type, inform ORB vs gap-fill regime decision)
#   - Full grid search: all threshold + logic combos in one table
#
# Filter logic:
#   Each filter independently flags "unfavourable".
#   trade_ok = True  if  sum(flags) < min_filters
#   Default min_filters=2 means all three must agree before skipping.
# ============================================================

import yfinance as yf
import pandas as pd
import numpy as np


# ════════════════════════════════════════════════════════════
# DATA FETCHERS
# ════════════════════════════════════════════════════════════

def fetch_india_vix(start_date, end_date):
    """
    Fetch India VIX daily data via yfinance (^INDIAVIX).

    Returns:
        dict: {date: vix_close}
    """
    print("Fetching India VIX...")
    try:
        vix = yf.download("^INDIAVIX", start=start_date, end=end_date,
                          progress=False)
        if vix.empty:
            print("  ⚠ No VIX data — check ticker ^INDIAVIX")
            return {}
        # Fix: newer yfinance returns MultiIndex columns — flatten to single level
        if isinstance(vix.columns, pd.MultiIndex):
            vix.columns = vix.columns.droplevel(1)
        vix_dict = {d.date(): float(v)
                    for d, v in zip(vix.index, vix['Close'])}
        print(f"  ✅ VIX: {len(vix_dict)} days | "
              f"Range {min(vix_dict.values()):.1f}–{max(vix_dict.values()):.1f}")
        return vix_dict
    except Exception as e:
        print(f"  ❌ VIX fetch failed: {e}")
        return {}


def fetch_sp500_returns(start_date, end_date):
    """
    Fetch S&P 500 (^GSPC) overnight returns, lagged +1 day for India.
    US close on day D → affects BankNifty open on D+1.

    Returns:
        dict: {india_date: sp500_prev_day_return_pct}
    """
    print("Fetching S&P 500 returns...")
    try:
        sp = yf.download("^GSPC", start=start_date, end=end_date,
                         progress=False)
        if sp.empty:
            print("  ⚠ No S&P data returned")
            return {}
        # Fix: newer yfinance returns MultiIndex columns — flatten to single level
        if isinstance(sp.columns, pd.MultiIndex):
            sp.columns = sp.columns.droplevel(1)
        sp['ret'] = sp['Close'].pct_change() * 100
        sp_ret = {}
        dates  = list(sp.index)
        for i in range(1, len(dates)):
            india_date = dates[i].date()
            val = float(sp['ret'].iloc[i - 1])
            if not np.isnan(val):           # skip the first NaN row
                sp_ret[india_date] = val
        valid_vals = list(sp_ret.values())
        print(f"  ✅ S&P: {len(sp_ret)} days | "
              f"Range {min(valid_vals):.1f}%–{max(valid_vals):.1f}%")
        return sp_ret
    except Exception as e:
        print(f"  ❌ S&P fetch failed: {e}")
        return {}


def fetch_brent_crude(start_date, end_date):
    """
    Fetch Brent crude (BZ=F) overnight % change, lagged +1 day.
    Brent is the primary proxy for oil/geopolitical risk (Iran, Hormuz, etc.)
    A Brent spike > +2% overnight = risk-off event for Indian markets.

    Returns:
        dict: {india_date: brent_prev_day_return_pct}
    """
    print("Fetching Brent crude (BZ=F)...")
    try:
        brent = yf.download("BZ=F", start=start_date, end=end_date,
                            progress=False)
        if brent.empty:
            print("  ⚠ No Brent data — trying CL=F (WTI) as fallback")
            brent = yf.download("CL=F", start=start_date, end=end_date,
                                progress=False)
        if brent.empty:
            print("  ❌ No crude data at all")
            return {}
        # Fix: newer yfinance returns MultiIndex columns — flatten to single level
        if isinstance(brent.columns, pd.MultiIndex):
            brent.columns = brent.columns.droplevel(1)
        brent['ret'] = brent['Close'].pct_change() * 100
        brent_ret = {}
        dates = list(brent.index)
        for i in range(1, len(dates)):
            india_date           = dates[i].date()
            brent_ret[india_date] = float(brent['ret'].iloc[i - 1])
        print(f"  ✅ Brent: {len(brent_ret)} days | "
              f"Range {min(brent_ret.values()):.1f}%–{max(brent_ret.values()):.1f}%")
        return brent_ret
    except Exception as e:
        print(f"  ❌ Brent fetch failed: {e}")
        return {}


def fetch_usdinr(start_date, end_date):
    """
    Fetch USD/INR (USDINR=X) overnight % change, lagged +1 day.
    INR weakening (positive % change) = capital flight / risk-off.
    A move > +0.5% overnight is a meaningful risk-off signal.

    Returns:
        dict: {india_date: usdinr_prev_day_return_pct}
    """
    print("Fetching USD/INR (USDINR=X)...")
    try:
        fx = yf.download("USDINR=X", start=start_date, end=end_date,
                         progress=False)
        if fx.empty:
            print("  ⚠ No USD/INR data")
            return {}
        # Fix: newer yfinance returns MultiIndex columns — flatten to single level
        if isinstance(fx.columns, pd.MultiIndex):
            fx.columns = fx.columns.droplevel(1)
        fx['ret'] = fx['Close'].pct_change() * 100
        fx_ret = {}
        dates  = list(fx.index)
        for i in range(1, len(dates)):
            india_date    = dates[i].date()
            fx_ret[india_date] = float(fx['ret'].iloc[i - 1])
        print(f"  ✅ USD/INR: {len(fx_ret)} days | "
              f"Range {min(fx_ret.values()):.3f}%–{max(fx_ret.values()):.3f}%")
        return fx_ret
    except Exception as e:
        print(f"  ❌ USD/INR fetch failed: {e}")
        return {}


# ════════════════════════════════════════════════════════════
# REGIME CLASSIFIER
# ════════════════════════════════════════════════════════════

def classify_regime(trade_log,
                    brent_ret,
                    usdinr_ret,
                    brent_spike_thr=2.0,
                    usdinr_spike_thr=0.4):
    """
    Classify each trade day as 'risk_off', 'risk_on', or 'neutral'
    based on Brent crude and USD/INR overnight moves.

    This is NOT a blocking filter — it's a regime tag used downstream
    to decide which strategy fires (gap fill vs ORB).

    Risk-off criteria (either condition):
        Brent  overnight change > +brent_spike_thr%  (oil shock)
        USD/INR overnight change > +usdinr_spike_thr% (capital flight)

    Risk-on criteria (either condition):
        Brent  overnight change < -brent_spike_thr%   (oil easing)
        USD/INR overnight change < -usdinr_spike_thr% (INR strengthening)

    Args:
        gap_df          : DataFrame from run_gap_fill
        brent_ret       : dict {date: brent_pct_change}
        usdinr_ret      : dict {date: usdinr_pct_change}
        brent_spike_thr : Brent % move threshold (default 2.0)
        usdinr_spike_thr: USD/INR % move threshold (default 0.4)

    Returns:
        pd.DataFrame: gap_df with columns [brent_ret_day, usdinr_ret_day, regime]
    """
    df = trade_log.copy()
    df['brent_ret_day']  = df['date'].map(lambda d: brent_ret.get(d, np.nan))
    df['usdinr_ret_day'] = df['date'].map(lambda d: usdinr_ret.get(d, np.nan))

    def _label(row):
        b = row['brent_ret_day']
        u = row['usdinr_ret_day']
        risk_off = (pd.notna(b) and b >  brent_spike_thr) or \
                   (pd.notna(u) and u >  usdinr_spike_thr)
        risk_on  = (pd.notna(b) and b < -brent_spike_thr) or \
                   (pd.notna(u) and u < -usdinr_spike_thr)
        if risk_off:
            return 'risk_off'
        elif risk_on:
            return 'risk_on'
        return 'neutral'

    df['regime'] = df.apply(_label, axis=1)

    counts = df['regime'].value_counts()
    print(f"\nRegime classification:")
    print(f"  neutral  : {counts.get('neutral',  0):4d} days")
    print(f"  risk_off : {counts.get('risk_off', 0):4d} days")
    print(f"  risk_on  : {counts.get('risk_on',  0):4d} days")

    # P&L by regime
    print(f"\n  P&L by regime (gap fill strategy):")
    print(f"  {'Regime':<12} {'Trades':>7} {'WinRate':>8} {'TotalP&L':>12} {'Avg/Trade':>10}")
    print(f"  {'-'*54}")
    for reg in ['neutral', 'risk_on', 'risk_off']:
        sub = df[df['regime'] == reg]
        if sub.empty:
            continue
        wr = sub['win'].mean() * 100
        pl = sub['pnl_rs'].sum()
        ap = sub['pnl_rs'].mean()
        print(f"  {reg:<12} {len(sub):>7}  {wr:>7.1f}%  ₹{pl:>10,.0f}  ₹{ap:>8,.0f}")

    return df


# ════════════════════════════════════════════════════════════
# ATTRIBUTION ANALYSIS
# ════════════════════════════════════════════════════════════

def filter_attribution(trade_log,
                       india_vix,
                       sp_ret,
                       fii_data=None,
                       vix_threshold=19.0,
                       sp_threshold=-1.5,
                       fpi_threshold=-3000.0):
    """
    Show the P&L of trades that EACH filter would skip, independently.
    This reveals whether each filter is actually removing bad trades
    or accidentally removing good ones.

    Prints per-filter breakdown:
        - How many trades it flags
        - Win rate and P&L of flagged (skipped) trades
        - Win rate and P&L of non-flagged (kept) trades
        - Net impact of the filter

    Args: same as apply_macro_filters

    Returns:
        pd.DataFrame: gap_df with all filter columns attached
    """
    df = trade_log.copy()

    fpi_dict = {}
    if fii_data is not None and not fii_data.empty:
        fpi_dict = dict(zip(
            pd.to_datetime(fii_data['date']).dt.date,
            fii_data['fpi_net']
        ))

    df['vix_day']    = df['date'].map(lambda d: india_vix.get(d, np.nan))
    df['sp_ret_day'] = df['date'].map(lambda d: sp_ret.get(d, np.nan))
    df['fpi_net_day']= df['date'].map(lambda d: fpi_dict.get(d, np.nan))

    df['filter_vix'] = df['vix_day'].apply(
        lambda v: v > vix_threshold if pd.notna(v) else False)
    df['filter_sp']  = df['sp_ret_day'].apply(
        lambda r: r < sp_threshold if pd.notna(r) else False)
    df['filter_fpi'] = df['fpi_net_day'].apply(
        lambda f: f < fpi_threshold if pd.notna(f) else False)

    print(f"\n{'='*65}")
    print(f"  FILTER ATTRIBUTION ANALYSIS")
    print(f"  Thresholds: VIX>{vix_threshold}  S&P<{sp_threshold}%  FPI<{fpi_threshold}Cr")
    print(f"{'='*65}")
    print(f"\n  Total trades in gap_df: {len(df)}")
    print(f"  Baseline  : Win {df['win'].mean()*100:.1f}%  |  "
          f"Total ₹{df['pnl_rs'].sum():,.0f}  |  "
          f"Avg ₹{df['pnl_rs'].mean():,.0f}/trade")

    filters = [
        ('VIX',  'filter_vix',  'vix_day'),
        ('S&P',  'filter_sp',   'sp_ret_day'),
        ('FPI',  'filter_fpi',  'fpi_net_day'),
    ]

    print(f"\n  {'Filter':<8} {'Flagged':>8} {'Flag WR':>8} {'Flag P&L':>12} "
          f"{'Keep WR':>8} {'Keep P&L':>12} {'Net Impact':>12}")
    print(f"  {'-'*75}")

    for name, col, val_col in filters:
        flagged = df[df[col] == True]
        kept    = df[df[col] == False]
        if flagged.empty:
            print(f"  {name:<8} {'0':>8} {'—':>8} {'—':>12} "
                  f"{kept['win'].mean()*100:>7.1f}%  "
                  f"₹{kept['pnl_rs'].sum():>10,.0f}  {'N/A':>12}")
            continue
        f_wr  = flagged['win'].mean() * 100
        f_pl  = flagged['pnl_rs'].sum()
        k_wr  = kept['win'].mean() * 100
        k_pl  = kept['pnl_rs'].sum()
        # Net impact = what you gain by skipping flagged trades
        # Positive = the filter correctly removed losing trades
        # Negative = the filter is removing profitable trades (bad filter)
        net   = -f_pl   # skipping flagged trades means you forgo f_pl
        sign  = "✅ GOOD" if f_pl < 0 else "❌ BAD "
        print(f"  {name:<8} {len(flagged):>8}  {f_wr:>7.1f}%  "
              f"₹{f_pl:>10,.0f}  {k_wr:>7.1f}%  "
              f"₹{k_pl:>10,.0f}  {sign} (₹{net:>+,.0f})")

    # AND logic comparison
    df['filter_count'] = (df['filter_vix'].astype(int) +
                          df['filter_sp'].astype(int) +
                          df['filter_fpi'].astype(int))

    print(f"\n  LOGIC COMPARISON (same thresholds, different voting rules):")
    print(f"  {'Logic':<20} {'Skipped':>8} {'Traded':>8} {'WinRate':>8} {'TotalP&L':>12}")
    print(f"  {'-'*62}")

    for min_f, label in [(1, 'OR  (any 1 of 3)'),
                          (2, 'AND (any 2 of 3)'),
                          (3, 'AND (all 3 of 3)')]:
        traded  = df[df['filter_count'] < min_f]
        skipped = df[df['filter_count'] >= min_f]
        wr = traded['win'].mean() * 100 if len(traded) > 0 else 0
        pl = traded['pnl_rs'].sum()
        print(f"  {label:<20} {len(skipped):>8}  {len(traded):>8}  "
              f"{wr:>7.1f}%  ₹{pl:>10,.0f}")

    return df


# ════════════════════════════════════════════════════════════
# FILTER APPLICATION (with min_filters voting)
# ════════════════════════════════════════════════════════════

def apply_macro_filters(trade_log,
                        india_vix,
                        sp_ret,
                        fii_data=None,
                        vix_threshold=19.0,
                        sp_threshold=-1.5,
                        fpi_threshold=-3000.0,
                        min_filters=2):
    """
    Apply VIX, S&P, and FII filters with configurable voting logic.

    Args:
        gap_df         : DataFrame from run_gap_fill (Cell 3)
        india_vix      : dict {date: vix_value}
        sp_ret         : dict {date: sp500_daily_return_pct}
        fii_data       : DataFrame with [date, fpi_net] or None
        vix_threshold  : Skip trade if VIX > this (default 19.0)
        sp_threshold   : Skip trade if S&P return < this % (default -1.5)
        fpi_threshold  : Skip trade if FPI net < this Cr (default -3000)
        min_filters    : Number of filters that must fire to skip a trade
                         1 = OR logic (original, too aggressive)
                         2 = 2-of-3 (recommended)
                         3 = AND logic (strictest)

    Returns:
        pd.DataFrame: gap_df + filter columns + filter_count + trade_ok
    """
    df = trade_log.copy()

    fpi_dict = {}
    if fii_data is not None and not fii_data.empty:
        fpi_dict = dict(zip(
            pd.to_datetime(fii_data['date']).dt.date,
            fii_data['fpi_net']
        ))

    df['vix_day']    = df['date'].map(lambda d: india_vix.get(d, np.nan))
    df['sp_ret_day'] = df['date'].map(lambda d: sp_ret.get(d, np.nan))
    df['fpi_net_day']= df['date'].map(lambda d: fpi_dict.get(d, np.nan))

    df['filter_vix'] = df['vix_day'].apply(
        lambda v: v > vix_threshold if pd.notna(v) else False)
    df['filter_sp']  = df['sp_ret_day'].apply(
        lambda r: r < sp_threshold if pd.notna(r) else False)
    df['filter_fpi'] = df['fpi_net_day'].apply(
        lambda f: f < fpi_threshold if pd.notna(f) else False)

    df['filter_count'] = (df['filter_vix'].astype(int) +
                          df['filter_sp'].astype(int) +
                          df['filter_fpi'].astype(int))

    # Trade if fewer than min_filters have fired
    df['trade_ok'] = df['filter_count'] < min_filters

    # ── Diagnostics ───────────────────────────────────────────────────────────
    n          = len(df)
    vix_flags  = df['filter_vix'].sum()
    sp_flags   = df['filter_sp'].sum()
    fpi_flags  = df['filter_fpi'].sum()
    blocked    = (~df['trade_ok']).sum()

    # How many trade dates had no matching data at all
    vix_missing = df['vix_day'].isna().sum()
    sp_missing  = df['sp_ret_day'].isna().sum()

    print(f"\n  Filter breakdown ({n} trades, min_filters={min_filters}):")
    print(f"    VIX  > {vix_threshold:.0f}     : {vix_flags:>4} flags  "
          f"({vix_missing} dates missing data)")
    print(f"    S&P  < {sp_threshold:.1f}%  : {sp_flags:>4} flags  "
          f"({sp_missing} dates missing data)")
    print(f"    FPI  < {fpi_threshold:,.0f}Cr: {fpi_flags:>4} flags")
    print(f"    ─────────────────────────────────────")
    print(f"    Overlap (≥{min_filters} filters)  : {blocked:>4} trades blocked")

    if vix_flags > 0 and sp_flags == 0:
        print(f"  ⚠  S&P filter never fired — check if sp_ret dates match trade dates")
        if sp_missing == n:
            print(f"  ❌ ALL S&P lookups returned NaN — date key mismatch in sp_ret dict")
    if blocked == 0 and (vix_flags > 0 or sp_flags > 0):
        print(f"  ℹ  Individual filters fired but never on the same day. "
              f"Try min_filters=1 to test.")

    return df


# ════════════════════════════════════════════════════════════
# BACKTEST WITH FULL REPORTING
# ════════════════════════════════════════════════════════════

def run_macro_backtest(trade_log, india_vix, sp_ret, fii_data=None,
                       min_filters=2):
    """
    Apply filters and compare filtered vs unfiltered P&L, year by year.

    Returns:
        pd.DataFrame: full gap_df with filter columns; trade_ok=True rows are live trades
    """
    filtered_df = apply_macro_filters(gap_df, india_vix, sp_ret, fii_data,
                                      min_filters=min_filters)

    total   = len(filtered_df)
    skipped = (~filtered_df['trade_ok']).sum()
    traded  = filtered_df['trade_ok'].sum()

    print(f"\n{'='*60}")
    print(f"  MACRO FILTER RESULTS  (min_filters={min_filters})")
    print(f"{'='*60}")
    print(f"  Total gap days  : {total}")
    print(f"  Skipped (filter): {skipped}  ({skipped/total*100:.1f}%)")
    print(f"  Traded          : {traded}  ({traded/total*100:.1f}%)")
    print(f"\n  Filter fires (independent counts, may overlap):")
    print(f"    VIX filtered  : {filtered_df['filter_vix'].sum()}")
    print(f"    S&P filtered  : {filtered_df['filter_sp'].sum()}")
    print(f"    FPI filtered  : {filtered_df['filter_fpi'].sum()}")

    print(f"\n  {'Year':<6} {'Unfiltered':>12} {'Filtered':>12} {'Kept':>12}")
    print(f"  {'-'*48}")
    for yr in sorted(filtered_df['year'].unique()):
        y    = filtered_df[filtered_df['year'] == yr]
        yf_  = y[y['trade_ok']]
        u_pl = y['pnl_rs'].sum()
        f_pl = yf_['pnl_rs'].sum()
        kept = f"{len(yf_)}/{len(y)}"
        print(f"  {yr:<6} ₹{u_pl:>10,.0f}  ₹{f_pl:>10,.0f}  {kept:>12}")

    filt     = filtered_df[filtered_df['trade_ok']]
    total_wr = filt['win'].mean() * 100
    total_pl = filt['pnl_rs'].sum()
    unfilt_pl= filtered_df['pnl_rs'].sum()
    print(f"\n  Unfiltered total: {total} trades  |  ₹{unfilt_pl:,.0f}")
    print(f"  Filtered total  : {len(filt)} trades  |  "
          f"Win: {total_wr:.1f}%  |  ₹{total_pl:,.0f}")
    delta = total_pl - unfilt_pl
    sign  = "+" if delta >= 0 else ""
    print(f"  Filter delta    : {sign}₹{delta:,.0f}  "
          f"({'BETTER' if delta >= 0 else 'WORSE than unfiltered'})")

    return filtered_df


# ════════════════════════════════════════════════════════════
# THRESHOLD SWEEPS
# ════════════════════════════════════════════════════════════

def vix_sweep(trade_log, india_vix, thresholds=None):
    """
    Sweep VIX threshold to find optimal cutoff.
    Shows trades kept, win rate, and total P&L at each level.
    """
    if thresholds is None:
        thresholds = [14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]

    df = trade_log.copy()
    df['vix_day'] = df['date'].map(lambda d: india_vix.get(d, np.nan))

    print(f"\nVIX Threshold Sweep:")
    print(f"  {'Threshold':>10}  {'Trades':>7}  {'WinRate':>8}  {'TotalP&L':>12}")
    print(f"  {'-'*46}")
    for thr in thresholds:
        sub = df[df['vix_day'].isna() | (df['vix_day'] <= thr)]
        if len(sub) == 0:
            continue
        wr = sub['win'].mean() * 100
        pl = sub['pnl_rs'].sum()
        print(f"  VIX ≤ {thr:4.1f}    {len(sub):6d}   {wr:7.1f}%   ₹{pl:>10,.0f}")


def sp_sweep(trade_log, sp_ret, thresholds=None):
    """
    Sweep S&P threshold to find optimal cutoff.
    """
    if thresholds is None:
        thresholds = [-3.0, -2.5, -2.0, -1.75, -1.5, -1.25, -1.0, -0.75]

    df = trade_log.copy()
    df['sp_ret_day'] = df['date'].map(lambda d: sp_ret.get(d, np.nan))

    print(f"\nS&P Threshold Sweep:")
    print(f"  {'Threshold':>12}  {'Trades':>7}  {'WinRate':>8}  {'TotalP&L':>12}")
    print(f"  {'-'*48}")
    for thr in thresholds:
        sub = df[df['sp_ret_day'].isna() | (df['sp_ret_day'] >= thr)]
        if len(sub) == 0:
            continue
        wr = sub['win'].mean() * 100
        pl = sub['pnl_rs'].sum()
        print(f"  S&P ≥ {thr:5.2f}%    {len(sub):6d}   {wr:7.1f}%   ₹{pl:>10,.0f}")


def fpi_sweep(trade_log, fii_data, thresholds=None):
    """
    Sweep FPI threshold to find optimal cutoff.
    """
    if thresholds is None:
        thresholds = [-1000, -1500, -2000, -2500, -3000, -3500, -4000, -5000]

    df = trade_log.copy()
    if fii_data is not None and not fii_data.empty:
        fpi_dict = dict(zip(
            pd.to_datetime(fii_data['date']).dt.date,
            fii_data['fpi_net']
        ))
        df['fpi_net_day'] = df['date'].map(lambda d: fpi_dict.get(d, np.nan))
    else:
        print("  ⚠ No FII data — FPI sweep skipped")
        return

    print(f"\nFPI Threshold Sweep:")
    print(f"  {'Threshold':>12}  {'Trades':>7}  {'WinRate':>8}  {'TotalP&L':>12}")
    print(f"  {'-'*48}")
    for thr in thresholds:
        sub = df[df['fpi_net_day'].isna() | (df['fpi_net_day'] >= thr)]
        if len(sub) == 0:
            continue
        wr = sub['win'].mean() * 100
        pl = sub['pnl_rs'].sum()
        print(f"  FPI ≥ {thr:>6,.0f}Cr  {len(sub):6d}   {wr:7.1f}%   ₹{pl:>10,.0f}")


def full_filter_grid(trade_log, india_vix, sp_ret, fii_data=None):
    """
    Grid search: test multiple threshold + voting logic combinations.
    Finds the combination that maximises filtered P&L.

    Prints top 10 combinations ranked by filtered P&L.
    """
    df = trade_log.copy()

    fpi_dict = {}
    if fii_data is not None and not fii_data.empty:
        fpi_dict = dict(zip(
            pd.to_datetime(fii_data['date']).dt.date,
            fii_data['fpi_net']
        ))

    df['vix_day']    = df['date'].map(lambda d: india_vix.get(d, np.nan))
    df['sp_ret_day'] = df['date'].map(lambda d: sp_ret.get(d, np.nan))
    df['fpi_net_day']= df['date'].map(lambda d: fpi_dict.get(d, np.nan))

    vix_thrs  = [17, 18, 19, 20, 22]
    sp_thrs   = [-1.0, -1.5, -2.0, -2.5]
    fpi_thrs  = [-2000, -3000, -4000]
    min_votes = [1, 2, 3]

    results = []
    for vt in vix_thrs:
        for st in sp_thrs:
            for ft in fpi_thrs:
                for mv in min_votes:
                    fv  = (df['vix_day']    > vt).fillna(False)
                    fs  = (df['sp_ret_day'] < st).fillna(False)
                    ff  = (df['fpi_net_day']< ft).fillna(False)
                    cnt = fv.astype(int) + fs.astype(int) + ff.astype(int)
                    ok  = cnt < mv
                    sub = df[ok]
                    if len(sub) < 50:   # ignore extreme over-filtering
                        continue
                    results.append({
                        'vix_thr': vt, 'sp_thr': st, 'fpi_thr': ft,
                        'min_v': mv,
                        'trades': len(sub),
                        'win_rate': sub['win'].mean() * 100,
                        'total_pl': sub['pnl_rs'].sum(),
                        'avg_pl':   sub['pnl_rs'].mean(),
                    })

    if not results:
        print("No valid combinations found.")
        return

    res_df = pd.DataFrame(results).sort_values('total_pl', ascending=False)

    print(f"\n{'='*80}")
    print(f"  FILTER GRID SEARCH — Top 15 combinations by Total P&L")
    print(f"  Baseline (no filters): {len(df)} trades | "
          f"₹{df['pnl_rs'].sum():,.0f}")
    print(f"{'='*80}")
    print(f"  {'VIX':>5} {'S&P':>6} {'FPI':>7} {'Vote':>5} "
          f"{'Trades':>7} {'WinRate':>8} {'TotalP&L':>12} {'Avg/Trade':>10}")
    print(f"  {'-'*65}")

    for _, row in res_df.head(15).iterrows():
        logic = f"{int(row['min_v'])}-of-3"
        print(f"  {row['vix_thr']:>5.0f} {row['sp_thr']:>6.2f} "
              f"{row['fpi_thr']:>7,.0f}  {logic:>5}  "
              f"{row['trades']:>7.0f}  {row['win_rate']:>7.1f}%  "
              f"₹{row['total_pl']:>10,.0f}  ₹{row['avg_pl']:>8,.0f}")

    best = res_df.iloc[0]
    print(f"\n  ★ Best combo: VIX≤{best['vix_thr']:.0f}  S&P≥{best['sp_thr']:.2f}%  "
          f"FPI≥{best['fpi_thr']:,.0f}Cr  vote={int(best['min_v'])}-of-3")
    print(f"    → {best['trades']:.0f} trades | "
          f"Win {best['win_rate']:.1f}% | ₹{best['total_pl']:,.0f}")

    return res_df


# ════════════════════════════════════════════════════════════
# MAIN EXECUTION — only runs when called directly, not on import
# ════════════════════════════════════════════════════════════
if __name__ == '__main__':
    print("Setting up macro filters (v2)...\n")

    # Fetch all external data
    india_vix  = fetch_india_vix("2022-01-01", "2025-12-31")
    sp_ret     = fetch_sp500_returns("2021-12-01", "2025-12-31")
    brent_ret  = fetch_brent_crude("2021-12-01", "2025-12-31")
    usdinr_ret = fetch_usdinr("2021-12-01", "2025-12-31")

    print("\n── Step 1: Attribution — is each filter actually helping? ──")
    attr_df = filter_attribution(gap_df, india_vix, sp_ret,
                                 fii_data if 'fii_data' in dir() else None)

    print("\n── Step 2: Regime classification (Brent + USD/INR) ──")
    regime_df = classify_regime(gap_df, brent_ret, usdinr_ret)

    print("\n── Step 3: Individual threshold sweeps ──")
    vix_sweep(gap_df, india_vix)
    sp_sweep(gap_df, sp_ret)
    if 'fii_data' in dir():
        fpi_sweep(gap_df, fii_data)

    print("\n── Step 4: Grid search — best filter combo ──")
    if 'fii_data' in dir():
        grid_results = full_filter_grid(gap_df, india_vix, sp_ret, fii_data)
    else:
        grid_results = full_filter_grid(gap_df, india_vix, sp_ret)

    print("\n── Step 5: Apply best filters (2-of-3 voting, original thresholds) ──")
    macro_df = run_macro_backtest(gap_df, india_vix, sp_ret,
                                  fii_data if 'fii_data' in dir() else None,
                                  min_filters=2)

    # Attach regime tags to macro_df
    macro_df['brent_ret_day']  = macro_df['date'].map(lambda d: brent_ret.get(d, np.nan))
    macro_df['usdinr_ret_day'] = macro_df['date'].map(lambda d: usdinr_ret.get(d, np.nan))

    print("\n✅ Cell 5 complete.")
    print("   Available: india_vix, sp_ret, brent_ret, usdinr_ret")
    print("   Available: attr_df, regime_df, macro_df, grid_results")
    print("   macro_df has trade_ok + regime columns — feed into Cell 7 (ORB)")
