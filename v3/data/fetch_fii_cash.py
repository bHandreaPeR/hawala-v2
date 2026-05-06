"""
v3/data/fetch_fii_cash.py
==========================
Fetches NSE FPI/FII daily cash market net activity and appends to
fii_data.csv (root of project).

Source: nselib.capital_market.category_turnover_cash(trade_date='DD-MM-YYYY')
Column stored: fpi_net (FPI buy minus sell in ₹ Crore)

Incremental: only fetches dates missing from fii_data.csv.
NSE publishes prior-day cash data overnight — safe to run each morning.

Usage:
    cd "Hawala v2/Hawala v2"
    python3 v3/data/fetch_fii_cash.py             # fills up to yesterday
    python3 v3/data/fetch_fii_cash.py --days 10   # go back 10 calendar days
"""
import sys, time, argparse
from datetime import date, timedelta, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT    = Path(__file__).resolve().parents[2]
CSV_OUT = ROOT / 'fii_data.csv'
DATE_FMT = '%d-%m-%Y'   # nselib expects DD-MM-YYYY


def _load_csv() -> pd.DataFrame:
    if CSV_OUT.exists():
        df = pd.read_csv(CSV_OUT)
        df['date'] = pd.to_datetime(df['date']).dt.date
        return df
    return pd.DataFrame(columns=['date', 'fpi_net', 'dii_net', 'mf_net', 'ins_net'])


def _save_csv(df: pd.DataFrame) -> None:
    df = df.sort_values('date').drop_duplicates('date')
    df.to_csv(CSV_OUT, index=False)


def _trading_days_in_range(start: date, end: date) -> list[date]:
    days = []
    cur  = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _fetch_one_day(trade_date: date) -> dict | None:
    from nselib import capital_market

    date_str = trade_date.strftime(DATE_FMT)
    try:
        raw = capital_market.category_turnover_cash(trade_date=date_str)
    except Exception as e:
        print(f"  ERROR fetching {date_str}: {e}", file=sys.stderr)
        return None

    if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
        return None

    df = pd.DataFrame(raw) if not isinstance(raw, pd.DataFrame) else raw
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

    cat_col = next((c for c in df.columns if 'category' in c or 'client' in c), None)
    if cat_col is None:
        return None

    df[cat_col] = df[cat_col].astype(str).str.strip().str.upper()

    def _get_net(label: str) -> float:
        row = df[df[cat_col].str.contains(label, na=False)]
        if row.empty:
            return np.nan
        buy_col  = next((c for c in df.columns if 'buy'  in c), None)
        sell_col = next((c for c in df.columns if 'sell' in c), None)
        net_col  = next((c for c in df.columns if 'net'  in c and 'purchase' not in c), None)
        try:
            if net_col:
                return float(str(row[net_col].iloc[0]).replace(',', ''))
            elif buy_col and sell_col:
                b = float(str(row[buy_col].iloc[0]).replace(',', ''))
                s = float(str(row[sell_col].iloc[0]).replace(',', ''))
                return b - s
        except Exception:
            pass
        return np.nan

    fpi_net = _get_net('FPI')
    if np.isnan(fpi_net):
        return None

    return {
        'date':    trade_date,
        'fpi_net': fpi_net,
        'dii_net': _get_net('DII'),
        'mf_net':  _get_net('MF'),
        'ins_net': _get_net('INS'),
    }


def _fetch_rest_api_latest() -> dict | None:
    """
    Fetch the most-recent FII/DII cash entry directly from NSE's REST API.
    Returns a row dict {date, fpi_net, dii_net, mf_net, ins_net} or None.

    NOTE: The NSE REST API ignores any date parameter and always returns the
    single most-recent published date.  Use this ONLY to fill the gap when
    nselib fails for the latest trading day — not for historical backfill.
    """
    import requests
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'Accept': 'application/json, */*',
        'Referer': 'https://www.nseindia.com/reports/fii-dii',
    }
    try:
        sess = requests.Session()
        sess.headers.update(HEADERS)
        sess.get('https://www.nseindia.com/', timeout=10)
        r = sess.get('https://www.nseindia.com/api/fiidiiTradeReact', timeout=10)
        r.raise_for_status()
        rows = r.json()   # [{category, date, buyValue, sellValue, netValue}, ...]
    except Exception as e:
        print(f"  REST API error: {e}", file=sys.stderr)
        return None

    # Parse FII/FPI and DII rows
    fpi_net = dii_net = np.nan
    api_date = None
    for row in rows:
        cat = str(row.get('category', '')).upper()
        try:
            net = float(str(row.get('netValue', 'nan')).replace(',', ''))
        except ValueError:
            net = np.nan
        # Parse date string "04-May-2026"
        if api_date is None:
            try:
                from datetime import datetime as _dt
                api_date = _dt.strptime(row.get('date', ''), '%d-%b-%Y').date()
            except Exception:
                pass
        if 'FII' in cat or 'FPI' in cat:
            fpi_net = net
        elif 'DII' in cat:
            dii_net = net

    if api_date is None or np.isnan(fpi_net):
        return None

    return {
        'date':    api_date,
        'fpi_net': fpi_net,
        'dii_net': dii_net,
        'mf_net':  np.nan,
        'ins_net': np.nan,
    }


def main(days_back: int = 7) -> None:
    from nselib import capital_market  # validate import early

    existing = _load_csv()
    existing_dates = set(existing['date'].tolist()) if not existing.empty else set()

    today = date.today()
    # Fetch up to today — NSE publishes FPI cash data the same evening (~21:00 IST).
    # "No data" is handled gracefully for unpublished dates.
    end   = today
    start = end - timedelta(days=days_back)
    days  = _trading_days_in_range(start, end)
    missing = [d for d in days if d not in existing_dates]

    if not missing:
        last = str(existing['date'].max()) if not existing.empty else 'empty'
        print(f"FII cash CSV up to date (last={last})")
        return

    print(f"FII cash: fetching {len(missing)} missing day(s): {missing[0]} → {missing[-1]}")
    new_rows = []
    nselib_failed = []
    for d in missing:
        row = _fetch_one_day(d)
        if row is not None:
            new_rows.append(row)
            print(f"  {d}: fpi_net={row['fpi_net']:+,.0f} Cr")
        else:
            print(f"  {d}: nselib no data (holiday/unavailable)")
            nselib_failed.append(d)
        time.sleep(0.4)

    # For any recently-failed dates, try the REST API once.
    # The REST API only has the single most-recent day — it ignores date params.
    # If it matches a missing date, accept it.
    if nselib_failed:
        already_filled = {r['date'] for r in new_rows}
        still_missing = [d for d in nselib_failed if d not in already_filled]
        if still_missing:
            print(f"  REST API fallback for {len(still_missing)} day(s) nselib could not fill…")
            rest_row = _fetch_rest_api_latest()
            if rest_row is not None and rest_row['date'] in still_missing:
                new_rows.append(rest_row)
                print(f"  REST: {rest_row['date']}: fpi_net={rest_row['fpi_net']:+,.0f} Cr")
            elif rest_row is not None:
                print(
                    f"  REST returned {rest_row['date']} which is not in missing list "
                    f"{[str(d) for d in still_missing]} — skipping"
                )
            else:
                print(f"  REST API also returned no data — {len(still_missing)} day(s) remain unfilled")

    if new_rows:
        appended = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
        _save_csv(appended)
        last = appended['date'].max()
        print(f"Saved {CSV_OUT.name} — {len(appended)} rows total, last={last}")
    else:
        print("No new data to save.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch FII/FPI cash activity into fii_data.csv')
    parser.add_argument('--days', type=int, default=7,
                        help='How many calendar days back to check for missing data (default: 7)')
    args = parser.parse_args()
    main(days_back=args.days)
