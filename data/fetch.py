# ============================================================
# data/fetch.py — Generic Instrument OHLCV Fetcher
# ============================================================
# Replaces cell_2_data_fetch.py.
# Parameterized by instrument — supports any symbol in config.INSTRUMENTS.
#
# Usage:
#   from data.fetch import fetch_instrument
#   data = fetch_instrument('BANKNIFTY', '2022-01-01', '2025-12-31')
#   data = fetch_instrument('NIFTY',     '2022-01-01', '2025-12-31')
# ============================================================

import time
import pandas as pd
from datetime import datetime, timedelta, timezone


# IST = UTC+5:30
_IST = timezone(timedelta(hours=5, minutes=30))


def _to_epoch_s(dt: datetime) -> int:
    """Convert a naive datetime (treated as IST) to UTC epoch seconds."""
    return int(dt.replace(tzinfo=_IST).timestamp())


def fetch_instrument(instrument: str, start_date: str, end_date: str,
                     groww=None, use_futures: bool = False) -> pd.DataFrame:
    """
    Fetch 15-min OHLCV candles for any registered instrument via Groww API.

    Args:
        instrument  : Key from config.INSTRUMENTS, e.g. 'BANKNIFTY' or 'NIFTY'
        start_date  : 'YYYY-MM-DD'
        end_date    : 'YYYY-MM-DD'
        groww       : Authenticated GrowwAPI instance (required)
        use_futures : If True, fetch rolling near-month futures (SEGMENT_FNO)
                      instead of spot index (SEGMENT_CASH). Trade log will
                      include Contract, Expiry, Oi columns.

    Returns:
        pd.DataFrame: OHLCV indexed by datetime (tz-naive IST), sorted ascending
                      Spot:    Open, High, Low, Close, Volume
                      Futures: Open, High, Low, Close, Volume, Oi, Contract, Expiry

    Raises:
        ValueError: if instrument is not in config.INSTRUMENTS
    """
    if use_futures:
        from data.futures_fetch import fetch_futures_rolling
        return fetch_futures_rolling(instrument, start_date, end_date, groww)
    from config import INSTRUMENTS

    if instrument not in INSTRUMENTS:
        raise ValueError(
            f"Unknown instrument '{instrument}'. "
            f"Registered: {list(INSTRUMENTS.keys())}"
        )
    if groww is None:
        raise ValueError("Pass an authenticated GrowwAPI instance via `groww=`")

    cfg            = INSTRUMENTS[instrument]
    groww_symbol   = cfg['symbol']
    segment        = groww.SEGMENT_CASH
    candle_interval= groww.CANDLE_INTERVAL_MIN_15
    chunk_days     = 88   # stay under 90-day API limit

    start  = datetime.strptime(start_date, "%Y-%m-%d")
    end    = datetime.strptime(end_date,   "%Y-%m-%d")
    frames = []
    cursor = start

    print(f"Fetching {instrument} 15-min data {start_date} → {end_date}...")

    while cursor < end:
        chunk_end = min(cursor + timedelta(days=chunk_days), end)

        # Groww API accepts "YYYY-MM-DD HH:MM:SS" or epoch seconds
        start_str = cursor.strftime("%Y-%m-%d 00:00:00")
        end_str   = chunk_end.strftime("%Y-%m-%d 23:59:59")

        try:
            result = groww.get_historical_candles(
                exchange        = "NSE",
                segment         = segment,
                groww_symbol    = groww_symbol,
                start_time      = start_str,
                end_time        = end_str,
                candle_interval = candle_interval,
            )
            if isinstance(result, dict):
                candles = result.get('candles', result.get('data', []))
            elif isinstance(result, list):
                candles = result
            else:
                candles = []

            if candles:
                frames.append(pd.DataFrame(candles))
                print(f"  ✓ {cursor.date()} → {chunk_end.date()}: {len(candles)} candles")
            else:
                print(f"  ⚠ {cursor.date()} → {chunk_end.date()}: empty response")

        except Exception as e:
            print(f"  ✗ {cursor.date()} → {chunk_end.date()}: {e}")
            # Fallback: epoch seconds
            try:
                result = groww.get_historical_candles(
                    exchange        = "NSE",
                    segment         = segment,
                    groww_symbol    = groww_symbol,
                    start_time      = _to_epoch_s(cursor.replace(hour=0, minute=0, second=0)),
                    end_time        = _to_epoch_s(chunk_end.replace(hour=23, minute=59, second=59)),
                    candle_interval = candle_interval,
                )
                if isinstance(result, dict):
                    candles = result.get('candles', result.get('data', []))
                elif isinstance(result, list):
                    candles = result
                else:
                    candles = []
                if candles:
                    frames.append(pd.DataFrame(candles))
                    print(f"    ↳ epoch-seconds fallback succeeded: {len(candles)} candles")
                else:
                    print(f"    ↳ epoch-seconds fallback: empty response")
            except Exception as e2:
                print(f"    ↳ epoch-seconds fallback failed: {e2}")

        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.3)

    if not frames:
        print(f"❌ No {instrument} data fetched — check API token and symbol")
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)

    # ── Normalise columns ─────────────────────────────────────────────────────
    # Groww spot candles: list-of-lists [ISO_ts, O, H, L, C, Vol] (6 elements)
    # FNO candles add OI as 7th element (handled in futures_fetch.py).
    if raw.columns.dtype == object:
        raw.columns = [str(c).capitalize() for c in raw.columns]
    else:
        n = len(raw.columns)
        std_names = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Oi']
        raw.columns = std_names[:n] if n <= len(std_names) else (
            std_names + [f'col_{i}' for i in range(n - len(std_names))]
        )

    # ── Parse timestamp ───────────────────────────────────────────────────────
    # Spot candles use ISO string timestamps e.g. "2022-04-15T09:15:00"
    time_col = next(
        (c for c in raw.columns if c.lower() in ('timestamp', 'datetime', 'time', 'date')),
        None
    )
    if time_col is None:
        print(f"  ❌ Cannot find timestamp column. Columns: {list(raw.columns)}")
        return pd.DataFrame()

    raw.index = pd.to_datetime(raw[time_col], errors='coerce')
    if raw.index.tz is not None:
        raw.index = raw.index.tz_localize(None)
    else:
        raw.index = raw.index.tz_localize(None)
    raw = raw.sort_index()
    raw = raw.between_time('09:00', '15:30')
    raw = raw[~raw.index.duplicated(keep='first')]

    print(f"  ✅ {instrument}: {len(raw):,} candles | "
          f"{raw.index[0].date()} → {raw.index[-1].date()}")
    return raw
