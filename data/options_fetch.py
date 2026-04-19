# ============================================================
# data/options_fetch.py — Option Candle Fetcher (Groww API)
# ============================================================
# Fetches actual 15-min OHLCV + OI for specific option contracts
# so the candlestick strategy can use real option premiums for P&L.
#
# Groww option symbol format (from API docs):
#   NSE-BANKNIFTY-25Jan24-52000-CE
#   NSE-NIFTY-30Sep25-24650-PE
# (day has NO leading zero per docs)
#
# Public API:
#   get_nearest_expiry(groww, underlying, trade_date, min_days=1)
#   fetch_option_candles(groww, underlying, expiry_date, strike, opt_type,
#                         start_date, end_date)
#   build_option_cache(groww, underlying, trade_df)
#   lookup_option_price(opt_df, ts, field='Close')
# ============================================================

import time
from datetime import date as date_type, timedelta

import pandas as pd

from data.contract_resolver import (
    build_options_symbol,
    _fetch_expiries_for_month,
)


EXCHANGE  = 'NSE'
CHUNK_DAYS = 88          # stay under 90-day API limit
RATE_SLEEP = 0.35        # seconds between API calls


# ── Expiry resolution ─────────────────────────────────────────────────────────
def get_nearest_expiry(groww, underlying: str, trade_date,
                        min_days: int = 1):
    """
    Return the nearest expiry date (datetime.date) such that
    expiry >= trade_date + min_days.

    Looks in the current month and up to 2 months ahead.
    Returns None if nothing found.
    """
    d = pd.Timestamp(trade_date).date()

    for offset in range(3):
        m = d.month + offset
        y = d.year
        if m > 12:
            m -= 12
            y += 1
        expiries = _fetch_expiries_for_month(groww, underlying, y, m)
        time.sleep(RATE_SLEEP)

        cutoff = d + timedelta(days=min_days)
        for exp in sorted(expiries):
            if exp >= cutoff:
                return exp

    return None


# ── Single contract candle fetch ──────────────────────────────────────────────
def fetch_option_candles(groww,
                          underlying: str,
                          expiry_date,
                          strike: int,
                          opt_type: str,
                          start_date: str,
                          end_date: str) -> pd.DataFrame:
    """
    Fetch 15-min OHLCV + OI for one option contract.

    Args
    ----
    groww       : Authenticated GrowwAPI instance
    underlying  : 'BANKNIFTY' or 'NIFTY'
    expiry_date : datetime.date or 'YYYY-MM-DD' string
    strike      : integer strike price
    opt_type    : 'CE' or 'PE'
    start_date  : 'YYYY-MM-DD'
    end_date    : 'YYYY-MM-DD'

    Returns
    -------
    pd.DataFrame with columns [Open, High, Low, Close, Volume, Oi]
    indexed by tz-naive IST datetime. Empty DataFrame on failure.
    """
    exp_d  = pd.Timestamp(expiry_date).date()
    symbol = build_options_symbol(EXCHANGE, underlying, exp_d, strike, opt_type)

    start_ts = pd.Timestamp(start_date)
    end_ts   = pd.Timestamp(end_date + ' 23:59:59')
    frames   = []
    cursor   = start_ts

    while cursor < end_ts:
        chunk_end = min(cursor + pd.Timedelta(days=CHUNK_DAYS), end_ts)
        s_str = cursor.strftime('%Y-%m-%d 00:00:00')
        e_str = chunk_end.strftime('%Y-%m-%d 23:59:59')

        try:
            resp    = groww.get_historical_candles(
                exchange        = EXCHANGE,
                segment         = groww.SEGMENT_FNO,
                groww_symbol    = symbol,
                start_time      = s_str,
                end_time        = e_str,
                candle_interval = groww.CANDLE_INTERVAL_MIN_15,
            )
            candles = (resp.get('candles', []) if isinstance(resp, dict)
                       else resp if isinstance(resp, list) else [])
            if candles:
                frames.append(_parse_candles(candles))
        except Exception as e:
            msg = str(e).lower()
            if 'rate limit' in msg or '429' in msg:
                time.sleep(5)
                try:
                    resp    = groww.get_historical_candles(
                        exchange        = EXCHANGE,
                        segment         = groww.SEGMENT_FNO,
                        groww_symbol    = symbol,
                        start_time      = s_str,
                        end_time        = e_str,
                        candle_interval = groww.CANDLE_INTERVAL_MIN_15,
                    )
                    candles = (resp.get('candles', []) if isinstance(resp, dict)
                               else resp if isinstance(resp, list) else [])
                    if candles:
                        frames.append(_parse_candles(candles))
                except Exception as e2:
                    print(f"    ↳ retry failed ({symbol}): {e2}")
            else:
                print(f"  ⚠ fetch_option_candles {symbol}: {e}")

        cursor = chunk_end + pd.Timedelta(minutes=1)
        time.sleep(RATE_SLEEP)

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames).sort_index()
    df = df[~df.index.duplicated(keep='first')]
    df = df.between_time('09:15', '15:30')
    return df


def _parse_candles(raw: list) -> pd.DataFrame:
    """
    Convert Groww candle list-of-lists to a DataFrame.
    Format: [timestamp, open, high, low, close, volume, OI]
    """
    rows = []
    for c in raw:
        if not c or len(c) < 5:
            continue
        rows.append({
            'datetime': pd.Timestamp(c[0]),
            'Open':     float(c[1]),
            'High':     float(c[2]),
            'Low':      float(c[3]),
            'Close':    float(c[4]),
            'Volume':   int(c[5])   if len(c) > 5 and c[5] else 0,
            'Oi':       float(c[6]) if len(c) > 6 and c[6] else 0.0,
        })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).set_index('datetime')


# ── Batch pre-loader ──────────────────────────────────────────────────────────
def build_option_cache(groww,
                        underlying: str,
                        trade_df: pd.DataFrame,
                        min_days_to_expiry: int = 1) -> tuple:
    """
    Pre-fetch all option candle data needed for OPT-mode trades.

    Args
    ----
    groww              : Authenticated GrowwAPI instance
    underlying         : 'BANKNIFTY' or 'NIFTY'
    trade_df           : trade log with columns [date, atm_strike, option_type, fno_mode]
    min_days_to_expiry : minimum calendar days between trade date and expiry

    Returns
    -------
    (data_cache, expiry_map)
        data_cache  : dict  (expiry_str, strike, opt_type) → pd.DataFrame
        expiry_map  : dict  trade_date (date) → expiry date string ('YYYY-MM-DD')
    """
    opt_rows = trade_df[trade_df.get('fno_mode', pd.Series('FUT', index=trade_df.index)) == 'OPT']
    if opt_rows.empty:
        return {}, {}

    print(f"\n── Pre-fetching option data for {len(opt_rows)} OPT trades ──")

    # ── Step 1: resolve expiry per unique trade date ──────────────────────────
    expiry_map: dict = {}
    for tdate in sorted(opt_rows['date'].unique()):
        exp = get_nearest_expiry(groww, underlying, tdate, min_days_to_expiry)
        if exp is not None:
            expiry_map[tdate] = str(exp)   # store as 'YYYY-MM-DD' string
        else:
            print(f"  ⚠ No expiry found for {tdate} — trade will fall back to FUT")

    opt_rows = opt_rows[opt_rows['date'].isin(expiry_map)].copy()
    opt_rows['_expiry'] = opt_rows['date'].map(expiry_map)

    # ── Step 2: unique (expiry, strike, opt_type) combos ─────────────────────
    combos = (opt_rows[['_expiry', 'atm_strike', 'option_type']]
              .drop_duplicates()
              .values.tolist())

    print(f"  {len(combos)} unique option contracts to fetch:")

    data_cache: dict = {}
    for expiry_str, strike, otype in combos:
        strike = int(strike)
        key    = (expiry_str, strike, otype)
        symbol = build_options_symbol(
            EXCHANGE, underlying,
            pd.Timestamp(expiry_str).date(), strike, otype
        )
        print(f"  → {symbol}", end='  ')

        # Fetch from 1 month before expiry (covers all trade dates on this expiry)
        exp_ts      = pd.Timestamp(expiry_str)
        range_start = (exp_ts - pd.Timedelta(days=35)).strftime('%Y-%m-%d')
        range_end   = expiry_str

        df = fetch_option_candles(
            groww, underlying, expiry_str, strike, otype,
            range_start, range_end,
        )
        if not df.empty:
            data_cache[key] = df
            print(f"✅ {len(df)} bars")
        else:
            print(f"⚠ no data (will fall back to FUT)")

    n_ok = sum(1 for k in data_cache)
    print(f"\n  Option cache: {n_ok}/{len(combos)} contracts loaded")
    return data_cache, expiry_map


# ── Price lookup ──────────────────────────────────────────────────────────────
def lookup_option_price(opt_df: pd.DataFrame,
                         ts,
                         field: str = 'Close',
                         tolerance_min: int = 15):
    """
    Return the option price (or full bar) at timestamp ts.

    If field is None, returns the full bar as a dict {Open, High, Low, Close, ...}.
    If field is a string, returns that field as float.
    Returns None if no bar found within tolerance.
    """
    ts = pd.Timestamp(ts)
    if opt_df.empty:
        return None

    import numpy as np
    secs   = np.abs((opt_df.index - ts).total_seconds())
    deltas = pd.Series(secs, index=opt_df.index)
    within = deltas[deltas <= tolerance_min * 60]
    if within.empty:
        return None

    nearest = within.idxmin()
    if field is None:
        return opt_df.loc[nearest].to_dict()
    val = opt_df.loc[nearest, field]
    return float(val) if pd.notna(val) else None
