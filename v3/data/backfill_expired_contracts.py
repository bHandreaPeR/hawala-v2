"""
v3/data/backfill_expired_contracts.py
======================================
Weekly backfill: re-fetch 1m candle data for any date where OI is NaN
AND the futures contract that was active on that date has since EXPIRED.

Groww returns 7 cols (including OI) for expired contracts, 6 cols (no OI)
for the active contract. Running this weekly fills the OI gap retroactively
as each monthly contract rolls off.

Instruments: NIFTY, BANKNIFTY
Schedule: Sunday 02:30 UTC (08:00 IST) via weekly_backfill.sh

Usage:
    python v3/data/backfill_expired_contracts.py
    python v3/data/backfill_expired_contracts.py --instrument NIFTY
    python v3/data/backfill_expired_contracts.py --instrument BANKNIFTY
    python v3/data/backfill_expired_contracts.py --dry-run   # shows what would be fetched
"""

import sys, os, pickle, time, pyotp, logging, argparse
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger('backfill_expired')


# ── Contract resolution ────────────────────────────────────────────────────────

EXPIRY_OVERRIDES: dict[date, date] = {
    date(2026, 3, 31): date(2026, 3, 30),   # Mar 2026: last-Tue holiday → Mar 30
}

def _last_tuesday(year: int, month: int) -> date:
    """Last Tuesday of the given month (NSE Nifty/BankNifty monthly expiry day)."""
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    days_back = (last_day.weekday() - 1) % 7   # weekday 1 = Tuesday
    return last_day - timedelta(days=days_back)


def _monthly_expiry(year: int, month: int) -> date:
    raw = _last_tuesday(year, month)
    return EXPIRY_OVERRIDES.get(raw, raw)


def _active_contract_expiry(trade_date: date) -> date:
    """Return the monthly futures expiry that was active on trade_date."""
    y, m = trade_date.year, trade_date.month
    for _ in range(3):
        exp = _monthly_expiry(y, m)
        if exp >= trade_date:
            return exp
        if m == 12:
            m, y = 1, y + 1
        else:
            m += 1
    raise RuntimeError(f"Cannot resolve expiry for {trade_date}")


def _expiry_to_symbol(instrument: str, expiry: date) -> str:
    """e.g. ('NIFTY', date(2026,4,28)) → 'NSE-NIFTY-28Apr26-FUT'"""
    return f"NSE-{instrument}-{expiry.day}{expiry.strftime('%b')}{expiry.strftime('%y')}-FUT"


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_groww():
    from growwapi import GrowwAPI
    env = {}
    with open(ROOT / 'token.env') as f:
        for line in f:
            if '=' in line:
                k, _, v = line.strip().partition('=')
                env[k] = v
    totp  = pyotp.TOTP(env['GROWW_TOTP_SECRET']).now()
    token = GrowwAPI.get_access_token(api_key=env['GROWW_API_KEY'], totp=totp)
    return GrowwAPI(token=token)


# ── Single-day fetch ──────────────────────────────────────────────────────────

def _fetch_day(g, instrument: str, trade_date: date, expiry: date) -> pd.DataFrame:
    symbol = _expiry_to_symbol(instrument, expiry)
    start  = f"{trade_date} 09:15:00"
    end    = f"{trade_date} 15:30:00"
    try:
        r = g.get_historical_candles(
            exchange='NSE', segment='FNO', groww_symbol=symbol,
            start_time=start, end_time=end,
            candle_interval=g.CANDLE_INTERVAL_MIN_1,
        )
        candles = r.get('candles', [])
        if not candles:
            log.info("  %s %s: no candles returned (holiday / market closed)", trade_date, symbol)
            return pd.DataFrame()

        n_cols = len(candles[0])
        if n_cols >= 7:
            df = pd.DataFrame([c[:7] for c in candles],
                              columns=['ts', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        else:
            # Contract not yet expired from Groww's perspective — OI unavailable
            log.warning(
                "  %s %s: got %d-col response — contract not fully expired in Groww yet, skipping",
                trade_date, symbol, n_cols,
            )
            return pd.DataFrame()

        df['ts']   = pd.to_datetime(df['ts'])
        df['date'] = df['ts'].dt.date
        df['time'] = df['ts'].dt.time
        for col in ['open', 'high', 'low', 'close', 'volume', 'oi']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        oi_pct = df['oi'].notna().mean() * 100
        log.info(
            "  %s %s: %d bars  OI coverage=%.0f%%",
            trade_date, symbol, len(df), oi_pct,
        )
        return df

    except Exception as e:
        log.error(
            "fetch_day FAILED: instrument=%s trade_date=%s symbol=%s error=%s",
            instrument, trade_date, symbol, e,
        )
        raise RuntimeError(
            f"Failed to fetch {instrument} 1m candles: "
            f"trade_date={trade_date} symbol={symbol} error={e}"
        ) from e


# ── Cache helpers ─────────────────────────────────────────────────────────────

_CACHE_PATHS = {
    'NIFTY':     ROOT / 'v3' / 'cache' / 'candles_1m_NIFTY.pkl',
    'BANKNIFTY': ROOT / 'v3' / 'cache' / 'candles_1m_BANKNIFTY.pkl',
}


def _load_cache(instrument: str) -> pd.DataFrame:
    p = _CACHE_PATHS[instrument]
    if not p.exists():
        return pd.DataFrame()
    with open(p, 'rb') as f:
        return pickle.load(f)


def _save_cache(instrument: str, df: pd.DataFrame) -> None:
    p = _CACHE_PATHS[instrument]
    with open(p, 'wb') as f:
        pickle.dump(df, f)
    log.info("Cache saved: %s (%d rows)", p.name, len(df))


def _find_nan_oi_dates(df: pd.DataFrame) -> list[date]:
    """Return sorted list of dates where OI is entirely NaN."""
    if df.empty or 'oi' not in df.columns or 'date' not in df.columns:
        return []
    nan_dates = (
        df.groupby('date')['oi']
        .apply(lambda s: s.isna().all())
        .pipe(lambda s: s[s].index.tolist())
    )
    return sorted(nan_dates)


# ── Main backfill ──────────────────────────────────────────────────────────────

def backfill(instrument: str, dry_run: bool = False) -> None:
    today = date.today()

    log.info("=" * 60)
    log.info("Backfill %s  |  today=%s  dry_run=%s", instrument, today, dry_run)
    log.info("=" * 60)

    cache = _load_cache(instrument)
    if cache.empty:
        log.warning("%s cache is empty — nothing to backfill", instrument)
        return

    nan_dates = _find_nan_oi_dates(cache)
    log.info("Dates with NaN OI: %d", len(nan_dates))

    if not nan_dates:
        log.info("No NaN-OI dates found — cache is clean.")
        return

    # Group by contract expiry so we auth once per instrument, not per date
    # Filter: only dates whose contract has since expired
    to_fetch: list[tuple[date, date]] = []   # (trade_date, expiry)
    skipped_active = []

    for d in nan_dates:
        try:
            expiry = _active_contract_expiry(d)
        except RuntimeError as e:
            log.warning("Cannot resolve expiry for %s: %s — skipping", d, e)
            continue

        if expiry >= today:
            skipped_active.append((d, expiry))
        else:
            to_fetch.append((d, expiry))

    log.info(
        "Eligible for backfill (contract expired): %d dates",
        len(to_fetch),
    )
    if skipped_active:
        log.info(
            "Skipped (active contract, expiry=%s): %d dates — will backfill after expiry",
            skipped_active[0][1] if skipped_active else '?',
            len(skipped_active),
        )

    if not to_fetch:
        log.info("Nothing to fetch — all NaN-OI dates are in the active contract window.")
        return

    if dry_run:
        log.info("DRY RUN — would fetch:")
        for d, exp in to_fetch:
            log.info("  %s  contract=%s", d, _expiry_to_symbol(instrument, exp))
        return

    g = _get_groww()

    new_frames: list[pd.DataFrame] = []
    replaced_dates: set[date] = set()

    for trade_date, expiry in to_fetch:
        log.info("Fetching %s %s (expiry %s) ...", instrument, trade_date, expiry)
        try:
            df_day = _fetch_day(g, instrument, trade_date, expiry)
        except RuntimeError as e:
            log.error("Skipping %s due to fetch error: %s", trade_date, e)
            continue

        if df_day.empty:
            # Could be holiday or Groww gap — leave cache as-is for this date
            continue

        if df_day['oi'].isna().all():
            log.warning(
                "%s %s: OI still NaN after re-fetch — Groww may not have settled OI for this date yet",
                trade_date, instrument,
            )
            continue

        new_frames.append(df_day)
        replaced_dates.add(trade_date)
        time.sleep(0.4)

    if not new_frames:
        log.info("No new data fetched.")
        return

    # Merge: drop replaced dates from cache, add fresh data
    updated = cache[~cache['date'].isin(replaced_dates)].copy()
    updated = pd.concat([updated] + new_frames, ignore_index=True)
    updated.drop_duplicates(subset=['ts'], inplace=True)
    updated.sort_values(['date', 'ts'], inplace=True)
    updated.reset_index(drop=True, inplace=True)

    _save_cache(instrument, updated)

    # Summary
    log.info("-" * 60)
    log.info(
        "Backfill complete: instrument=%s replaced_dates=%d total_rows=%d",
        instrument, len(replaced_dates), len(updated),
    )
    log.info("Replaced dates: %s", sorted(replaced_dates))

    # Verify OI coverage after backfill
    remaining_nan = _find_nan_oi_dates(updated)
    active_nan    = [d for d in remaining_nan if _active_contract_expiry(d) >= today]
    resolved_nan  = [d for d in remaining_nan if d not in active_nan]
    if resolved_nan:
        log.warning(
            "Still NaN-OI after backfill (Groww gap / holiday): %d dates — %s",
            len(resolved_nan), resolved_nan,
        )
    if active_nan:
        log.info(
            "NaN-OI in active contract window (%d dates) — expected, will resolve at month-end expiry",
            len(active_nan),
        )
    log.info("=" * 60)


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Backfill expired futures OI into candle cache")
    parser.add_argument(
        '--instrument', choices=['NIFTY', 'BANKNIFTY', 'ALL'], default='ALL',
        help="Which instrument to backfill (default: ALL)",
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help="Print what would be fetched without making any API calls or cache writes",
    )
    args = parser.parse_args()

    instruments = ['NIFTY', 'BANKNIFTY'] if args.instrument == 'ALL' else [args.instrument]

    for inst in instruments:
        try:
            backfill(inst, dry_run=args.dry_run)
        except Exception as e:
            log.error(
                "backfill FAILED: instrument=%s error=%s",
                inst, e,
            )
            # Don't abort the other instrument
            continue


if __name__ == '__main__':
    main()
