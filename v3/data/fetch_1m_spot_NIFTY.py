"""
v3/data/fetch_1m_spot_NIFTY.py
================================
Fetch & cache 1m NIFTY spot (index) candles from Groww.
Uses segment='CASH', groww_symbol='NSE-NIFTY'.

Note: spot index candles have no volume/OI — those fields are None from the API.
Stored columns: [ts, open, high, low, close, date, time]

Cache: v3/cache/candles_1m_spot_NIFTY.pkl
"""
import os, sys, pickle, time, pyotp, logging
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
CACHE_FILE = ROOT / 'v3' / 'cache' / 'candles_1m_spot_NIFTY.pkl'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('fetch_1m_spot_nifty')


# ── Auth ──────────────────────────────────────────────────────────────────────
def _get_groww():
    from growwapi import GrowwAPI
    env = {}
    with open(ROOT / 'token.env') as f:
        for line in f:
            if '=' in line:
                k, _, v = line.strip().partition('=')
                env[k] = v
    totp = pyotp.TOTP(env['GROWW_TOTP_SECRET']).now()
    token = GrowwAPI.get_access_token(api_key=env['GROWW_API_KEY'], totp=totp)
    return GrowwAPI(token=token)


# ── Auth validation ───────────────────────────────────────────────────────────
def _validate_auth(g):
    """
    Validate that the Groww token is live by fetching a known spot candle.
    Uses April 28, 2026 09:15-09:17 on NSE-NIFTY (CASH segment).
    Raises RuntimeError with clear message if 0 candles returned or any error.
    """
    test_sym  = "NSE-NIFTY"
    test_date = "2026-04-28"
    start     = f"{test_date}T09:15:00"
    end       = f"{test_date}T09:17:00"
    try:
        r = g.get_historical_candles(
            exchange='NSE', segment='CASH', groww_symbol=test_sym,
            start_time=start, end_time=end,
            candle_interval=g.CANDLE_INTERVAL_MIN_1,
        )
        candles = r.get('candles', [])
        if not candles:
            raise RuntimeError(
                "Auth validation FAILED: Groww token appears expired or invalid. "
                f"Test symbol={test_sym} segment=CASH date={test_date} returned 0 candles. "
                "Re-generate your token.env with a fresh TOTP and retry."
            )
        log.info(
            "Auth validated OK — test fetch returned %d candles",
            len(candles)
        )
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(
            f"Auth validation FAILED: Groww API error on test fetch. "
            f"symbol={test_sym} segment=CASH date={test_date} error={e}. "
            "Check token.env and network connectivity."
        ) from e


# ── Single-day fetch ──────────────────────────────────────────────────────────
def _fetch_day_1m(g, trade_date: date) -> pd.DataFrame:
    """
    Fetch 1m spot candles for trade_date from NSE-NIFTY (CASH segment).
    Returns DataFrame[ts, open, high, low, close, date, time] or empty DataFrame.
    volume/oi are omitted — they are None for index candles.
    """
    symbol = "NSE-NIFTY"
    start  = f"{trade_date}T09:15:00"
    end    = f"{trade_date}T15:30:00"

    for attempt in range(3):
        try:
            r = g.get_historical_candles(
                exchange='NSE', segment='CASH', groww_symbol=symbol,
                start_time=start, end_time=end,
                candle_interval=g.CANDLE_INTERVAL_MIN_1,
            )
            candles = r.get('candles', [])
            if not candles:
                return pd.DataFrame()

            # Candle row: [ts, open, high, low, close, volume, oi]
            # volume and oi are None for the CASH index — we drop them.
            df = pd.DataFrame(
                candles, columns=['ts', 'open', 'high', 'low', 'close', '_vol', '_oi']
            )
            df.drop(columns=['_vol', '_oi'], inplace=True)
            df['ts'] = pd.to_datetime(df['ts'])
            df['date'] = df['ts'].dt.date
            df['time'] = df['ts'].dt.time
            df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].apply(
                pd.to_numeric, errors='coerce'
            )
            return df[['ts', 'open', 'high', 'low', 'close', 'date', 'time']]

        except Exception as e:
            err_str = str(e)
            is_rate_limit = 'rate limit' in err_str.lower()
            if is_rate_limit:
                backoff = 10.0 * (2 ** attempt)   # 10s, 20s, 40s
                log.warning(
                    "RATE LIMIT symbol=%s trade_date=%s attempt=%d — sleeping %.0fs",
                    symbol, trade_date, attempt + 1, backoff,
                )
                time.sleep(backoff)
                if attempt == 2:
                    raise RuntimeError(
                        f"Rate limit persisted after 3 attempts. "
                        f"trade_date={trade_date} symbol={symbol} last_error={e}"
                    ) from e
            else:
                raise RuntimeError(
                    f"Failed to fetch NIFTY spot 1m candles: "
                    f"trade_date={trade_date} symbol={symbol} error={e}"
                ) from e

    # Should not reach here, but satisfy type checkers
    return pd.DataFrame()


# ── Main fetch loop ───────────────────────────────────────────────────────────
def fetch_and_cache(lookback_days: int = 120, force_full: bool = False):
    """
    Fetch 1m spot NIFTY candles for last `lookback_days` calendar days.
    Skips weekends. Skips dates already in cache unless force_full=True.
    Saves incrementally after each day.
    """
    g = _get_groww()
    _validate_auth(g)

    if CACHE_FILE.exists() and not force_full:
        with open(CACHE_FILE, 'rb') as f:
            existing = pickle.load(f)
        cached_dates = set(existing['date'].unique()) if not existing.empty else set()
    else:
        existing = pd.DataFrame()
        cached_dates = set()

    log.info("Cached dates: %d", len(cached_dates))

    today = date.today()
    new_frames = []
    fetched = 0

    for i in range(lookback_days, -1, -1):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:   # Saturday=5, Sunday=6
            continue
        if d in cached_dates:
            continue
        if d > today:
            continue

        log.info("Fetching spot %s ...", d)
        df_day = _fetch_day_1m(g, d)
        if not df_day.empty:
            new_frames.append(df_day)
            fetched += 1
            log.info(
                "Spot day fetched trade_date=%s candles=%d total_new=%d",
                d, len(df_day), fetched
            )
            # Incremental save — preserves progress if process is killed mid-run
            combined = pd.concat([existing] + new_frames, ignore_index=True)
            combined.drop_duplicates(subset=['ts'], inplace=True)
            combined.sort_values('ts', inplace=True)
            combined.reset_index(drop=True, inplace=True)
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump(combined, f)
        else:
            log.info(
                "No spot data trade_date=%s reason=holiday_or_market_closed_or_gap",
                d
            )
        time.sleep(0.4)

    if not new_frames:
        log.info("Nothing new to add.")
        return existing

    combined = pd.concat([existing] + new_frames, ignore_index=True)
    combined.drop_duplicates(subset=['ts'], inplace=True)
    combined.sort_values('ts', inplace=True)
    combined.reset_index(drop=True, inplace=True)

    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(combined, f)

    log.info(
        "Spot cache updated total_candles=%d new_days=%d path=%s",
        len(combined), fetched, CACHE_FILE
    )
    return combined


if __name__ == '__main__':
    df = fetch_and_cache(lookback_days=120)
    print(f"\nTotal 1m spot candles: {len(df)}")
    if not df.empty:
        print(f"Date range: {df['date'].min()} → {df['date'].max()}")
        print(f"Unique days: {df['date'].nunique()}")
        print(f"Columns: {list(df.columns)}")
        print(df.head(3).to_string())
