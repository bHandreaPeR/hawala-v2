# ============================================================
# CELL 2 — BankNifty Historical Data Fetch
# ============================================================
# Fetches 15-min OHLCV candles from Groww API.
# Chunks requests into 90-day windows (API limit).
# Filters to market hours 09:00–15:30.
# ============================================================

def fetch_banknifty(start_date, end_date):
    """
    Fetch BankNifty 15-min candles from Groww API.
    Automatically chunks into 90-day windows to respect API limits.

    Args:
        start_date (str): 'YYYY-MM-DD'
        end_date   (str): 'YYYY-MM-DD'

    Returns:
        pd.DataFrame: OHLCV DataFrame indexed by datetime
    """
    groww_symbol    = "NSE-BANKNIFTY"
    segment         = groww.SEGMENT_CASH
    candle_interval = groww.CANDLE_INTERVAL_MIN_15
    chunk_days      = 88   # stay under 90-day API limit

    start  = datetime.strptime(start_date, "%Y-%m-%d")
    end    = datetime.strptime(end_date,   "%Y-%m-%d")
    frames = []
    cursor = start

    while cursor < end:
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        try:
            result = groww.get_historical_candles(
                exchange        = "NSE",
                segment         = segment,
                groww_symbol    = groww_symbol,
                start_time      = cursor.strftime("%Y-%m-%d"),
                end_time        = chunk_end.strftime("%Y-%m-%d"),
                candle_interval = candle_interval
            )
            if isinstance(result, dict):
                candles = result.get('candles', result.get('data', []))
            elif isinstance(result, list):
                candles = result
            else:
                candles = []

            if candles:
                df = pd.DataFrame(candles)
                frames.append(df)

        except Exception as e:
            print(f"  Warning: chunk {cursor.date()} → {chunk_end.date()}: {e}")

        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.3)

    if not frames:
        print("❌ No data fetched — check API token and symbol")
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)

    # Normalise column names
    raw.columns = [c.capitalize() for c in raw.columns]
    time_col    = [c for c in raw.columns
                   if c.lower() in ('timestamp', 'datetime', 'time', 'date')][0]
    raw.index   = pd.to_datetime(raw[time_col], unit='ms', errors='coerce') \
                  .fillna(pd.to_datetime(raw[time_col], errors='coerce'))
    raw.index   = raw.index.tz_localize(None)
    raw         = raw.sort_index()
    raw         = raw.between_time('09:00', '15:30')
    raw         = raw[~raw.index.duplicated(keep='first')]

    print(f"✅ {len(raw):,} candles | "
          f"{raw.index[0].date()} → {raw.index[-1].date()}")
    return raw


# ── Fetch 2022–2025 (training 2022–2024 + out-of-sample 2025) ────────────
data = fetch_banknifty("2022-01-01", "2025-12-31")
