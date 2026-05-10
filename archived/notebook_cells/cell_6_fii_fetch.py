# ============================================================
# CELL 6 — FII / FPI Data Fetch (nselib)
# ============================================================
# Fetches daily FPI (Foreign Portfolio Investor) net activity
# from NSE via the nselib library.
#
# Data availability:
#   Jan 2024 onwards: nselib category_turnover_cash() → 'FPI' category
#   2022–2023       : Old format (BNK/DFI/PRO-TRADES), no FPI column
#   → We fetch 2024–present only and handle NaN gracefully for older dates.
#
# Column meanings (all in ₹ Crore):
#   fpi_net : FPI buy - sell  (negative = net seller, bearish for market)
#   dii_net : DII buy - sell  (usually counter to FPI)
#   mf_net  : Mutual fund activity
#   ins_net : Insurance companies
#
# Threshold guidance:
#   fpi_net < -3000 Cr → skip trade (heavy institutional selling)
#   This captures ~15–20% of days; -2000 Cr was too aggressive (37%)
# ============================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

try:
    from nselib import capital_market
except ImportError:
    print("❌ nselib not installed. Run: !pip install nselib xlrd -q")


# ── FPI Fetch ─────────────────────────────────────────────────────────────

def fetch_fpi_data(start_date, end_date):
    """
    Fetch daily FPI/FII net activity from NSE via nselib.

    Uses category_turnover_cash(trade_date='DD-MM-YYYY') which
    returns intraday category breakdown. FPI net is computed as:
        buy_value - sell_value for the 'FPI' category.

    Args:
        start_date (str): 'YYYY-MM-DD'  (use 2024-01-01 or later)
        end_date   (str): 'YYYY-MM-DD'

    Returns:
        pd.DataFrame: columns [date, fpi_net, dii_net, mf_net, ins_net]
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")

    records  = []
    failures = 0
    cursor   = start

    print(f"Fetching FPI data: {start_date} → {end_date}")
    print("(Skipping weekends. Expect ~250 trading days per year.)\n")

    while cursor <= end:
        # Skip weekends
        if cursor.weekday() >= 5:
            cursor += timedelta(days=1)
            continue

        date_str = cursor.strftime("%d-%m-%Y")   # DD-MM-YYYY format for nselib

        try:
            raw = capital_market.category_turnover_cash(trade_date=date_str)

            if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
                cursor += timedelta(days=1)
                time.sleep(0.3)
                continue

            df = pd.DataFrame(raw) if not isinstance(raw, pd.DataFrame) else raw

            # Normalise column names
            df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

            # Identify the category column
            cat_col = next((c for c in df.columns
                            if 'category' in c or 'client' in c), None)
            if cat_col is None:
                cursor += timedelta(days=1)
                time.sleep(0.3)
                continue

            df[cat_col] = df[cat_col].astype(str).str.strip().str.upper()

            # Helper to extract net for a given category label
            def get_net(label):
                row = df[df[cat_col].str.contains(label, na=False)]
                if row.empty:
                    return np.nan
                # Look for buy/sell/net columns
                buy_col  = next((c for c in df.columns if 'buy' in c), None)
                sell_col = next((c for c in df.columns if 'sell' in c), None)
                net_col  = next((c for c in df.columns
                                 if 'net' in c and 'purchase' not in c), None)
                try:
                    if net_col:
                        val = row[net_col].iloc[0]
                        return float(str(val).replace(',', ''))
                    elif buy_col and sell_col:
                        b = float(str(row[buy_col].iloc[0]).replace(',', ''))
                        s = float(str(row[sell_col].iloc[0]).replace(',', ''))
                        return b - s
                except Exception:
                    pass
                return np.nan

            fpi_net = get_net('FPI')
            dii_net = get_net('DII')
            mf_net  = get_net('MF')
            ins_net = get_net('INS')

            if not np.isnan(fpi_net):
                records.append({
                    'date':    cursor.date(),
                    'fpi_net': fpi_net,
                    'dii_net': dii_net,
                    'mf_net':  mf_net,
                    'ins_net': ins_net,
                })

        except Exception as e:
            failures += 1
            if failures <= 5:
                print(f"  ⚠ {date_str}: {e}")

        cursor += timedelta(days=1)
        time.sleep(0.3)

    fii_df = pd.DataFrame(records)
    print(f"\n✅ FPI data: {len(fii_df)} trading days fetched")
    if not fii_df.empty:
        print(f"   FPI net range: ₹{fii_df['fpi_net'].min():,.0f} Cr "
              f"→ ₹{fii_df['fpi_net'].max():,.0f} Cr")
        heavy_sell = (fii_df['fpi_net'] < -3000).sum()
        print(f"   Heavy sell days (< -3000 Cr): {heavy_sell} "
              f"({heavy_sell/len(fii_df)*100:.1f}%)")
    return fii_df


# ── FPI threshold sweep ───────────────────────────────────────────────────

def fpi_sweep(gap_df, fii_data, thresholds=None):
    """
    Sweep FPI threshold to find optimal cutoff for skipping trades.

    Args:
        gap_df     : Full gap_df (Cell 3 output)
        fii_data   : DataFrame from fetch_fpi_data
        thresholds : list of net values to test in ₹ Cr (default -1000 to -5000)
    """
    if thresholds is None:
        thresholds = [-1000, -1500, -2000, -2500, -3000, -3500, -4000, -5000]

    fpi_dict = dict(zip(
        pd.to_datetime(fii_data['date']).dt.date,
        fii_data['fpi_net']
    ))

    df = gap_df.copy()
    df['fpi_net'] = df['date'].map(lambda d: fpi_dict.get(d, np.nan))

    # Only evaluate rows where we actually have FPI data
    df_with_fpi = df[df['fpi_net'].notna()]

    print(f"\nFPI Threshold Sweep ({len(df_with_fpi)} days with FPI data):")
    print(f"{'Threshold (Cr)':>16}  {'Trades':>7}  {'WinRate':>8}  {'TotalP&L':>12}")
    print("-" * 55)

    # Baseline: no filter
    wr = df_with_fpi['win'].mean() * 100
    pl = df_with_fpi['pnl_rs'].sum()
    print(f"  No filter        {len(df_with_fpi):6d}   {wr:7.1f}%   ₹{pl:>10,.0f}")

    for thr in thresholds:
        sub = df_with_fpi[df_with_fpi['fpi_net'] >= thr]
        if len(sub) == 0:
            continue
        wr = sub['win'].mean() * 100
        pl = sub['pnl_rs'].sum()
        print(f"  FPI ≥ {thr:6,.0f} Cr  {len(sub):6d}   {wr:7.1f}%   ₹{pl:>10,.0f}")


# ── Run ───────────────────────────────────────────────────────────────────
print("Fetching FPI/FII data...\n")

# Fetch 2024–2025 (format supports FPI category from Jan 2024)
fii_data = fetch_fpi_data("2024-01-01", "2025-12-31")

# Save to CSV for reuse (avoids re-fetching daily)
fii_data.to_csv("fii_data.csv", index=False)
print("\nSaved to fii_data.csv")

# Reload example (use this at top of session instead of re-fetching):
# fii_data = pd.read_csv("fii_data.csv", parse_dates=['date'])
# fii_data['date'] = fii_data['date'].dt.date

# Run sweep to find optimal threshold
# fpi_sweep(gap_df, fii_data)
print("\nUncomment fpi_sweep(gap_df, fii_data) to run threshold analysis.")
