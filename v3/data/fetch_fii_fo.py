"""
v3/data/fetch_fii_fo.py
=======================
Fetches NSE FII F&O participant-wise open interest and appends to
trade_logs/_fii_fo_cache.pkl.

Source: nselib.derivatives.participant_wise_open_interest(trade_date='DD-MM-YYYY')
FII row keys stored: fut_long, fut_short, ce_long, pe_long

Incremental: only fetches dates missing from the cache (up to today or --date arg).
Safe to run daily before market opens — NSE publishes prior-day data overnight.

Usage:
    cd "Hawala v2/Hawala v2"
    python3 v3/data/fetch_fii_fo.py             # fills up to yesterday
    python3 v3/data/fetch_fii_fo.py --days 10   # go back 10 calendar days
"""
import sys, pickle, time, argparse
from datetime import date, timedelta, datetime
from pathlib import Path

import pandas as pd

ROOT      = Path(__file__).resolve().parents[2]
CACHE     = ROOT / 'trade_logs' / '_fii_fo_cache.pkl'
DATE_FMT  = '%d-%m-%Y'   # nselib expects DD-MM-YYYY


def _load_cache() -> dict:
    if CACHE.exists():
        with open(CACHE, 'rb') as f:
            return pickle.load(f)
    return {}


def _save_cache(data: dict) -> None:
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE, 'wb') as f:
        pickle.dump(data, f)


def _trading_days_in_range(start: date, end: date) -> list[date]:
    """Return weekdays in [start, end] inclusive (no holiday filter — NSE returns empty for holidays)."""
    days = []
    cur  = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _fetch_one_day(trade_date: date) -> dict | None:
    """
    Fetch FII row from participant_wise_open_interest for one date.
    Returns {'fut_long', 'fut_short', 'ce_long', 'pe_long'} or None.
    """
    from nselib import derivatives

    date_str = trade_date.strftime(DATE_FMT)
    try:
        df = derivatives.participant_wise_open_interest(trade_date=date_str)
    except Exception as e:
        print(f"  ERROR fetching {date_str}: {e}", file=sys.stderr)
        return None

    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return None

    if not isinstance(df, pd.DataFrame):
        try:
            df = pd.DataFrame(df)
        except Exception:
            return None

    # Normalise column names
    df.columns = [str(c).strip() for c in df.columns]

    # Identify the client-type column
    ct_col = next((c for c in df.columns if 'client' in c.lower() or 'type' in c.lower()), None)
    if ct_col is None:
        print(f"  WARN {date_str}: cannot find Client Type column — columns: {df.columns.tolist()}", file=sys.stderr)
        return None

    df[ct_col] = df[ct_col].astype(str).str.strip().str.upper()
    fii_row = df[df[ct_col] == 'FII']
    if fii_row.empty:
        return None

    def _get(pattern: str) -> int:
        col = next((c for c in df.columns if pattern.lower() in c.lower()), None)
        if col is None:
            return 0
        try:
            raw = fii_row[col].iloc[0]
            return int(str(raw).replace(',', '').strip())
        except Exception:
            return 0

    return {
        'fut_long':  _get('future index long'),
        'fut_short': _get('future index short'),
        'ce_long':   _get('option index call long'),
        'pe_long':   _get('option index put long'),
    }


def main(days_back: int = 7) -> None:
    from nselib import derivatives  # validate import early

    cache = _load_cache()
    today = date.today()
    # Fetch up to today — NSE publishes F&O participant OI the same evening (~22:00 IST).
    # "No data" is handled gracefully for future/unpublished dates.
    end   = today
    start = end - timedelta(days=days_back)

    days  = _trading_days_in_range(start, end)
    missing = [d for d in days if str(d) not in cache]

    if not missing:
        print(f"FII F&O cache up to date (last={sorted(cache.keys())[-1] if cache else 'empty'})")
        return

    print(f"FII F&O: fetching {len(missing)} missing day(s): {missing[0]} → {missing[-1]}")
    added = 0
    for d in missing:
        row = _fetch_one_day(d)
        if row is not None:
            cache[str(d)] = row
            added += 1
            print(f"  {d}: fut_long={row['fut_long']:,}  fut_short={row['fut_short']:,}  "
                  f"ce_long={row['ce_long']:,}  pe_long={row['pe_long']:,}")
        else:
            print(f"  {d}: no data (holiday/weekend/unavailable)")
        time.sleep(0.4)

    if added:
        _save_cache(cache)
        last = sorted(cache.keys())[-1]
        print(f"Saved {CACHE.name} — {len(cache)} dates total, last={last}")
    else:
        print("No new data to save.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch FII F&O participant OI into cache')
    parser.add_argument('--days', type=int, default=7,
                        help='How many calendar days back to check for missing data (default: 7)')
    args = parser.parse_args()
    main(days_back=args.days)
