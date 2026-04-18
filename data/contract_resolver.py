# ============================================================
# data/contract_resolver.py — Futures & Options Contract Resolver
# ============================================================
# Converts a date range into a mapping of {date → active contract}
# using the Groww get_expiries() API.
#
# Usage:
#   from data.contract_resolver import build_expiry_calendar, build_futures_symbol
#   calendar = build_expiry_calendar('BANKNIFTY', '2022-01-01', '2024-12-31', groww)
#   # calendar[date(2022, 4, 15)] → (date(2022, 4, 28), 'NSE-BANKNIFTY-28Apr22-FUT')
# ============================================================

import time
from datetime import date, datetime, timedelta


def build_futures_symbol(exchange: str, ticker: str, expiry_date: date) -> str:
    """
    Build the Groww futures symbol from an expiry date.

    Format: {Exchange}-{Ticker}-{D}{Mon}{YY}-FUT
    Example: 2022-04-28 → 'NSE-BANKNIFTY-28Apr22-FUT'
             2025-01-02 → 'NSE-NIFTY-2Jan25-FUT'

    Note: Day has NO leading zero (confirmed from Groww docs example: '2Jan25').
    """
    day_str = str(expiry_date.day)           # '2' or '28' — no leading zero
    mon_str = expiry_date.strftime('%b')     # 'Jan', 'Apr', etc.
    yr_str  = expiry_date.strftime('%y')     # '22', '25'
    return f"{exchange}-{ticker}-{day_str}{mon_str}{yr_str}-FUT"


def build_options_symbol(exchange: str, ticker: str, expiry_date: date,
                         strike: int, option_type: str) -> str:
    """
    Build the Groww options symbol.

    Format: {Exchange}-{Ticker}-{D}{Mon}{YY}-{Strike}-{CE/PE}
    Example: 'NSE-BANKNIFTY-2May22-36000-CE'
    """
    day_str = str(expiry_date.day)
    mon_str = expiry_date.strftime('%b')
    yr_str  = expiry_date.strftime('%y')
    return f"{exchange}-{ticker}-{day_str}{mon_str}{yr_str}-{strike}-{option_type}"


def get_atm_strike(price: float, strike_interval: int = 100) -> int:
    """Round a futures price to the nearest ATM strike."""
    return int(round(price / strike_interval) * strike_interval)


_EXPIRY_CACHE: dict = {}   # module-level cache: (symbol, year, month) → [date, ...]


def _fetch_expiries_for_month(groww, underlying_symbol: str,
                               year: int, month: int,
                               max_retries: int = 4) -> list:
    """
    Call get_expiries() for one year-month and return sorted list of date objects.
    Results are cached so the same month is never fetched twice in a session.
    Retries with exponential backoff on rate-limit errors.
    Returns [] on persistent error.
    """
    cache_key = (underlying_symbol, year, month)
    if cache_key in _EXPIRY_CACHE:
        return _EXPIRY_CACHE[cache_key]

    delay = 1.0
    for attempt in range(max_retries):
        try:
            resp = groww.get_expiries(
                exchange          = groww.EXCHANGE_NSE,
                underlying_symbol = underlying_symbol,
                year              = year,
                month             = month,
            )
            if isinstance(resp, dict):
                raw = resp.get('expiries', [])
            elif isinstance(resp, list):
                raw = resp
            else:
                raw = []
            result = sorted(datetime.strptime(d, '%Y-%m-%d').date() for d in raw if d)
            _EXPIRY_CACHE[cache_key] = result
            return result
        except Exception as e:
            msg = str(e).lower()
            if 'rate limit' in msg or 'rate_limit' in msg or '429' in msg:
                wait = delay * (2 ** attempt)
                print(f"  ⏳ Rate limited — waiting {wait:.0f}s "
                      f"(attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                print(f"  ⚠ get_expiries({underlying_symbol}, {year}-{month:02d}): {e}")
                break

    _EXPIRY_CACHE[cache_key] = []
    return []


def build_expiry_calendar(underlying_symbol: str,
                           start_date: str,
                           end_date: str,
                           groww,
                           roll_days_before: int = 1,
                           futures_only: bool = True) -> dict:
    """
    Build a mapping of {calendar_date → (expiry_date, futures_symbol)} for every
    calendar date in [start_date, end_date].

    Args:
        underlying_symbol : 'BANKNIFTY' or 'NIFTY' (no exchange prefix)
        start_date        : 'YYYY-MM-DD'
        end_date          : 'YYYY-MM-DD'
        groww             : Authenticated GrowwAPI instance
        roll_days_before  : Roll to next contract this many days before expiry.
                            Default 1 = roll ON the expiry day (trade next contract).
        futures_only      : If True, keep only the last (monthly) expiry per month.
                            If False, return all expiries (includes weeklies).

    Returns:
        dict: {date → (expiry_date, contract_symbol)}
              Every calendar date in range is covered, including weekends
              (strategies filter to trading days themselves).
    """
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end   = datetime.strptime(end_date,   '%Y-%m-%d').date()

    # ── Fetch all expiry dates for the range ──────────────────────────────────
    # Expand range by ±1 month to ensure we always have a next contract
    fetch_start = date(start.year, start.month, 1)
    if end.month == 12:
        fetch_end = date(end.year + 1, 1, 1)
    else:
        fetch_end = date(end.year, end.month + 1, 1)

    all_expiries = []
    cursor = fetch_start
    while cursor <= fetch_end:
        month_expiries = _fetch_expiries_for_month(
            groww, underlying_symbol, cursor.year, cursor.month
        )
        all_expiries.extend(month_expiries)
        # Advance to next month
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)
        time.sleep(0.2)  # rate limit

    all_expiries = sorted(set(all_expiries))

    if not all_expiries:
        print(f"  ❌ No expiries found for {underlying_symbol} in range")
        return {}

    if futures_only:
        # Keep only the last expiry per calendar month (= monthly futures)
        monthly: dict = {}
        for exp in all_expiries:
            key = (exp.year, exp.month)
            if key not in monthly or exp > monthly[key]:
                monthly[key] = exp
        all_expiries = sorted(monthly.values())

    print(f"  📅 {len(all_expiries)} {'monthly' if futures_only else ''} "
          f"expiries found for {underlying_symbol}: "
          f"{all_expiries[0]} → {all_expiries[-1]}")

    # ── Build date → (expiry, symbol) mapping ─────────────────────────────────
    exchange = 'NSE'
    calendar = {}
    current_day = start

    while current_day <= end:
        # Find the active contract for this date
        # Active = smallest expiry where (expiry - roll_days_before) >= current_day
        # i.e. we roll roll_days_before days before expiry
        active_expiry = None
        for exp in all_expiries:
            if exp - timedelta(days=roll_days_before) >= current_day:
                active_expiry = exp
                break

        if active_expiry is None:
            # Date is past all known expiries — use the last one
            active_expiry = all_expiries[-1]

        symbol = build_futures_symbol(exchange, underlying_symbol, active_expiry)
        calendar[current_day] = (active_expiry, symbol)
        current_day += timedelta(days=1)

    # Summary
    unique_contracts = sorted(set(v[1] for v in calendar.values()))
    print(f"  🗂  {len(unique_contracts)} contracts covering {start} → {end}:")
    for c in unique_contracts:
        print(f"      {c}")

    return calendar


def get_weekly_expiry(trade_date: date,
                      underlying_symbol: str,
                      groww) -> date | None:
    """
    Find the nearest weekly expiry >= trade_date for options simulation.
    Uses get_expiries() for the current and next month (includes weeklies).

    Returns the expiry date, or None if unavailable.
    """
    expiries = []
    for offset in (0, 1):  # current month + next month
        m = trade_date.month + offset
        y = trade_date.year
        if m > 12:
            m -= 12
            y += 1
        expiries.extend(_fetch_expiries_for_month(groww, underlying_symbol, y, m))
        time.sleep(0.1)

    expiries = sorted(set(expiries))
    for exp in expiries:
        if exp >= trade_date:
            return exp
    return None
