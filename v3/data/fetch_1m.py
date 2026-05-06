"""
v3/data/fetch_1m.py
===================
Fetch & cache 1m BANKNIFTY futures candles from Groww.
Window: last ~30 trading days (API limit). Run daily to stay current.

Cache: v3/cache/candles_1m_BANKNIFTY.pkl
Format: DataFrame[ts, open, high, low, close, volume, oi, date, time]
"""
import os, sys, pickle, time, pyotp, logging
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]          # Hawala v2/
sys.path.insert(0, str(ROOT))
CACHE_FILE = ROOT / 'v3' / 'cache' / 'candles_1m_BANKNIFTY.pkl'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('fetch_1m')


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
def _get_near_expiry(g, trade_date: date) -> str:
    """Return nearest monthly BankNifty futures expiry on or after trade_date."""
    d = trade_date
    for offset in range(3):
        m = d.month + offset
        y = d.year
        if m > 12:
            m -= 12
            y += 1
        try:
            result = g.get_expiries(exchange='NSE', underlying_symbol='BANKNIFTY',
                                    year=y, month=m)
            for exp in sorted(result.get('expiries', [])):
                exp_d = date.fromisoformat(exp)
                if exp_d >= trade_date:
                    return exp
        except Exception as e:
            log.warning(f"get_expiries {y}-{m:02d}: {e}")
        time.sleep(0.3)
    raise RuntimeError(f"No expiry found for {trade_date}")


def _expiry_to_symbol(expiry_str: str) -> str:
    """'2026-05-26' → 'NSE-BANKNIFTY-26May26-FUT'"""
    d = date.fromisoformat(expiry_str)
    return f"NSE-BANKNIFTY-{d.day}{d.strftime('%b')}{d.strftime('%y')}-FUT"


# ── Single-day fetch ──────────────────────────────────────────────────────────
def _fetch_day_1m(g, trade_date: date) -> pd.DataFrame:
    expiry = _get_near_expiry(g, trade_date)
    symbol = _expiry_to_symbol(expiry)
    start  = f"{trade_date}T09:15:00"
    end    = f"{trade_date}T15:30:00"
    try:
        r = g.get_historical_candles(
            exchange='NSE', segment='FNO', groww_symbol=symbol,
            start_time=start, end_time=end,
            candle_interval=g.CANDLE_INTERVAL_MIN_1
        )
        candles = r.get('candles', [])
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(candles, columns=['ts', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        df['ts'] = pd.to_datetime(df['ts'])
        df['date'] = df['ts'].dt.date
        df['time'] = df['ts'].dt.time
        # OI only on first candle from Groww — forward fill
        df['oi'] = df['oi'].ffill()
        df[['open','high','low','close','volume','oi']] = \
            df[['open','high','low','close','volume','oi']].apply(pd.to_numeric, errors='coerce')
        return df
    except Exception as e:
        log.error(f"fetch day {trade_date} ({symbol}): {e}")
        return pd.DataFrame()


# ── Main fetch loop ───────────────────────────────────────────────────────────
def fetch_and_cache(lookback_days: int = 35, force_full: bool = False):
    """
    Fetch 1m candles for last `lookback_days` trading days.
    Skips dates already in cache unless force_full=True.
    """
    g = _get_groww()

    # Load existing cache
    if CACHE_FILE.exists() and not force_full:
        with open(CACHE_FILE, 'rb') as f:
            existing = pickle.load(f)
        cached_dates = set(existing['date'].unique()) if not existing.empty else set()
    else:
        existing = pd.DataFrame()
        cached_dates = set()

    log.info(f"Cached dates: {len(cached_dates)}")

    # Build list of trading days to fetch
    today = date.today()
    new_frames = []
    fetched = 0

    for i in range(lookback_days, -1, -1):
        d = today - timedelta(days=i)
        if d.weekday() >= 5:       # skip weekends
            continue
        if d in cached_dates:       # already have it
            continue
        if d > today:
            continue

        log.info(f"Fetching {d} ...")
        df_day = _fetch_day_1m(g, d)
        if not df_day.empty:
            new_frames.append(df_day)
            fetched += 1
            log.info(f"  {d}: {len(df_day)} candles")
        else:
            log.info(f"  {d}: no data (holiday/future)")
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

    log.info(f"Cache updated: {len(combined)} total candles, {fetched} new days")
    return combined


if __name__ == '__main__':
    df = fetch_and_cache(lookback_days=35)
    print(f"\nTotal 1m candles: {len(df)}")
    if not df.empty:
        print(f"Date range: {df['date'].min()} → {df['date'].max()}")
        print(f"Unique days: {df['date'].nunique()}")
        print(df.head(3).to_string())
