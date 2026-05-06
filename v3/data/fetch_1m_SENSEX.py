"""
v3/data/fetch_1m_SENSEX.py
===========================
Fetch & cache 1m SENSEX futures candles from Groww.
Window: last ~30 trading days (API limit per request). Run daily to stay current.

Sensex futures expiry: LAST THURSDAY of each month (monthly contract).
  - Weekly options also expire Thursdays, but the LAST Thursday = monthly futures.
  - Do NOT use get_expiries() to find this — use _last_thursday() directly.
  - e.g. April 2026 futures → 30Apr26

Cache: v3/cache/candles_1m_SENSEX.pkl
Format: DataFrame[ts, open, high, low, close, volume, oi, date, time]
"""
import sys, pickle, time, pyotp, logging
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
CACHE_FILE = ROOT / 'v3' / 'cache' / 'candles_1m_SENSEX.pkl'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('fetch_1m_sensex')


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


# ── Contract resolver ─────────────────────────────────────────────────────────
def _last_thursday(year: int, month: int) -> date:
    """
    Return last Thursday of the given month.
    BSE/NSE monthly futures always expire on the last Thursday.
    """
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    # weekday 3 = Thursday
    days_back = (last_day.weekday() - 3) % 7
    return last_day - timedelta(days=days_back)


def _get_near_monthly_expiry(trade_date: date) -> date:
    """
    Return nearest SENSEX monthly futures expiry (last Thursday) on or after trade_date.
    Does NOT call get_expiries() — monthly futures expiry = last Thursday,
    which for Sensex coincides with the last weekly option expiry of the month,
    but using _last_thursday() is explicit and unambiguous.
    """
    y, m = trade_date.year, trade_date.month
    for _ in range(3):
        exp = _last_thursday(y, m)
        if exp >= trade_date:
            return exp
        if m == 12:
            m, y = 1, y + 1
        else:
            m += 1
    raise RuntimeError(
        f"Cannot find SENSEX monthly futures expiry for trade_date={trade_date}"
    )


def _expiry_to_symbol(expiry: date) -> str:
    """date(2026, 4, 30) → 'BSE-SENSEX-30Apr26-FUT'"""
    return f"BSE-SENSEX-{expiry.day}{expiry.strftime('%b')}{expiry.strftime('%y')}-FUT"


# ── Single-day fetch ──────────────────────────────────────────────────────────
def _fetch_day_1m(g, trade_date: date) -> pd.DataFrame:
    expiry = _get_near_monthly_expiry(trade_date)
    symbol = _expiry_to_symbol(expiry)
    start  = f"{trade_date}T09:15:00"
    end    = f"{trade_date}T15:30:00"
    try:
        r = g.get_historical_candles(
            exchange='BSE', segment='FNO', groww_symbol=symbol,
            start_time=start, end_time=end,
            candle_interval=g.CANDLE_INTERVAL_MIN_1
        )
        candles = r.get('candles', [])
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(
            candles, columns=['ts', 'open', 'high', 'low', 'close', 'volume', 'oi']
        )
        df['ts'] = pd.to_datetime(df['ts'])
        df['date'] = df['ts'].dt.date
        df['time'] = df['ts'].dt.time
        df['oi'] = df['oi'].ffill()
        df[['open', 'high', 'low', 'close', 'volume', 'oi']] = \
            df[['open', 'high', 'low', 'close', 'volume', 'oi']].apply(
                pd.to_numeric, errors='coerce'
            )
        return df
    except Exception as e:
        log.error(
            "fetch_day_1m trade_date=%s symbol=%s error=%s", trade_date, symbol, e
        )
        raise RuntimeError(
            f"Failed to fetch SENSEX 1m candles: trade_date={trade_date} "
            f"symbol={symbol} error={e}"
        ) from e


# ── Main fetch loop ───────────────────────────────────────────────────────────
def fetch_and_cache(lookback_days: int = 35, force_full: bool = False):
    """
    Fetch 1m candles for last `lookback_days` trading days.
    Skips dates already in cache unless force_full=True.
    """
    g = _get_groww()

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
        if d.weekday() >= 5:
            continue
        if d in cached_dates:
            continue
        if d > today:
            continue

        log.info("Fetching %s ...", d)
        df_day = _fetch_day_1m(g, d)
        if not df_day.empty:
            new_frames.append(df_day)
            fetched += 1
            log.info("  %s: %d candles  (total new=%d)", d, len(df_day), fetched)
            # Incremental save — preserves progress if process is killed
            combined = pd.concat([existing] + new_frames, ignore_index=True)
            combined.drop_duplicates(subset=['ts'], inplace=True)
            combined.sort_values('ts', inplace=True)
            combined.reset_index(drop=True, inplace=True)
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump(combined, f)
        else:
            log.info("  %s: no data (holiday/market closed/Groww gap)", d)
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
        "Cache updated: total_candles=%d new_days=%d path=%s",
        len(combined), fetched, CACHE_FILE
    )
    return combined


if __name__ == '__main__':
    df = fetch_and_cache(lookback_days=35)
    print(f"\nTotal SENSEX 1m candles: {len(df)}")
    if not df.empty:
        print(f"Date range: {df['date'].min()} → {df['date'].max()}")
        print(f"Unique days: {df['date'].nunique()}")
        print(df.head(3).to_string())
