# ============================================================
# data/futures_fetch.py — Rolling Near-Month Futures Fetcher
# ============================================================
# Builds a continuous 15-min OHLCV DataFrame across multiple
# futures contracts by stitching contract periods back-to-back.
#
# Usage:
#   from data.futures_fetch import fetch_futures_rolling
#   data = fetch_futures_rolling('BANKNIFTY', '2022-01-01', '2024-12-31', groww)
#   # data has columns: Open, High, Low, Close, Volume, Oi, Contract, Expiry
# ============================================================

import time
import pandas as pd
from datetime import datetime, timedelta


def fetch_futures_rolling(instrument: str,
                           start_date: str,
                           end_date: str,
                           groww) -> pd.DataFrame:
    """
    Fetch continuous 15-min OHLCV for near-month futures across the date range.

    Args:
        instrument  : Key from config.INSTRUMENTS (e.g. 'BANKNIFTY')
        start_date  : 'YYYY-MM-DD'
        end_date    : 'YYYY-MM-DD'
        groww       : Authenticated GrowwAPI instance

    Returns:
        pd.DataFrame with columns:
            Open, High, Low, Close, Volume, Oi  (float)
            Contract  (str)  — e.g. 'NSE-BANKNIFTY-28Apr22-FUT'
            Expiry    (date) — expiry date of this candle's contract
        Indexed by IST datetime, market hours 09:00–15:30 only.
    """
    from config import INSTRUMENTS
    from data.contract_resolver import build_expiry_calendar

    cfg               = INSTRUMENTS[instrument]
    underlying_symbol = cfg['underlying_symbol']

    print(f"\nFetching {instrument} FUTURES data {start_date} → {end_date}...")

    # ── Step 1: Build expiry calendar ─────────────────────────────────────────
    print(f"  Building expiry calendar...")
    calendar = build_expiry_calendar(
        underlying_symbol = underlying_symbol,
        start_date        = start_date,
        end_date          = end_date,
        groww             = groww,
        roll_days_before  = 1,
        futures_only      = True,
    )

    if not calendar:
        print("  ❌ Empty calendar — cannot fetch futures data")
        return pd.DataFrame()

    # ── Step 2: Group calendar dates by contract ───────────────────────────────
    # Build {contract_symbol → (period_start, period_end, expiry_date)}
    contract_periods = {}   # symbol → {'start': date, 'end': date, 'expiry': date}
    for cal_date, (expiry, symbol) in sorted(calendar.items()):
        if symbol not in contract_periods:
            contract_periods[symbol] = {
                'start':  cal_date,
                'end':    cal_date,
                'expiry': expiry,
            }
        else:
            contract_periods[symbol]['end'] = cal_date

    print(f"  {len(contract_periods)} contract periods to fetch")

    # ── Step 3: Fetch each contract ────────────────────────────────────────────
    frames = []

    for symbol, period in sorted(contract_periods.items(),
                                  key=lambda x: x[1]['start']):
        period_start = period['start'].strftime('%Y-%m-%d 00:00:00')
        period_end   = period['end'].strftime('%Y-%m-%d 23:59:59')
        expiry       = period['expiry']

        # SEGMENT_FNO is the correct constant; fall back to string 'FNO' if renamed
        _seg_fno = getattr(groww, 'SEGMENT_FNO',
                   getattr(groww, 'SEGMENT_FO', 'FNO'))

        try:
            result = groww.get_historical_candles(
                exchange        = groww.EXCHANGE_NSE,
                segment         = _seg_fno,
                groww_symbol    = symbol,
                start_time      = period_start,
                end_time        = period_end,
                candle_interval = groww.CANDLE_INTERVAL_MIN_15,
            )

            if isinstance(result, dict):
                candles = result.get('candles', result.get('data', []))
            elif isinstance(result, list):
                candles = result
            else:
                candles = []

            if not candles:
                print(f"  ⚠ {symbol}: empty response")
                time.sleep(0.3)
                continue

            df = _parse_fno_candles(candles, symbol, expiry)
            if not df.empty:
                frames.append(df)
                print(f"  ✓ {symbol}: {len(df):,} candles "
                      f"({df.index[0].date()} → {df.index[-1].date()})")

        except Exception as e:
            print(f"  ✗ {symbol}: {e}")

        time.sleep(0.3)

    if not frames:
        print("  ❌ No futures data fetched")
        return pd.DataFrame()

    # ── Step 4: Concatenate and clean ──────────────────────────────────────────
    combined = pd.concat(frames)
    combined = combined.sort_index()
    combined = combined[~combined.index.duplicated(keep='first')]
    combined = combined.between_time('09:00', '15:30')

    print(f"\n  ✅ {instrument} FUTURES: {len(combined):,} candles | "
          f"{combined.index[0].date()} → {combined.index[-1].date()} | "
          f"{combined['Contract'].nunique()} contracts")

    return combined


def _parse_fno_candles(candles: list, symbol: str, expiry) -> pd.DataFrame:
    """
    Parse a list-of-lists FNO candle response into a DataFrame.

    FNO candle format (7 elements):
        [ISO_timestamp, open, high, low, close, volume, open_interest]

    Returns DataFrame indexed by IST datetime (tz-naive).
    """
    if not candles:
        return pd.DataFrame()

    # FNO candles: 7 elements including OI
    std_names = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Oi']

    if isinstance(candles[0], (list, tuple)):
        n = len(candles[0])
        df = pd.DataFrame(candles, columns=std_names[:n])
    else:
        # list of dicts
        df = pd.DataFrame(candles)
        df.columns = [c.capitalize() for c in df.columns]

    # Parse ISO timestamp (format: "2022-04-15T09:15:00" or "2022-04-15 09:15:00")
    df.index = pd.to_datetime(df['Timestamp'], errors='coerce')
    df.index = df.index.tz_localize(None)  # strip tz if present

    # Ensure numeric columns
    for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'Oi']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Fill missing Oi with 0 (some early candles may be null)
    if 'Oi' in df.columns:
        df['Oi'] = df['Oi'].fillna(0)
    else:
        df['Oi'] = 0

    # Tag each candle with its contract
    df['Contract'] = symbol
    df['Expiry']   = expiry

    df = df.sort_index()
    return df
