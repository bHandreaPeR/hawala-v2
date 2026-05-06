"""
Phase-4 driver: walk NSE trade-dates Sep 2025 → today, fetch all Tier-A
reports in-memory, extract daily intel, write daily_intel_{date}.json,
discard raw bytes.

- Trading-day calendar from pandas_market_calendars 'NSE'.
- Checkpoint-resume: dates with an existing daily_intel_*.json are skipped.
- Per-date save (incremental, survives bash 45 s timeout).
- Hard-fail on any per-date error: raises and stops the loop.

Usage:
    cd "Hawala v2/Hawala v2"
    # default: from 2025-09-01 to today, batch=8 dates, fail-fast
    python3 v3/scripts/fetch_nse_reports_history.py
    # specific window:
    python3 v3/scripts/fetch_nse_reports_history.py --start 2025-09-01 --end 2026-05-04
    # tighter batch (for 45s budget, polite_delay=1.5s, 12 reports/date ≈ 18s/date):
    python3 v3/scripts/fetch_nse_reports_history.py --batch 2
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import pandas as pd
import pandas_market_calendars as mcal

from v3.data.nse_reports.fetch import fetch_for_date            # noqa: E402
from v3.data.nse_reports.intel import build_daily_intel, write_intel_json  # noqa: E402

INTEL_DIR = ROOT / "v3" / "cache" / "nse_reports"


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def trading_days(start: date, end: date) -> list[date]:
    nse = mcal.get_calendar("NSE")
    sched = nse.schedule(start_date=str(start), end_date=str(end))
    return [d.date() for d in sched.index]


def intel_path(d: date) -> Path:
    return INTEL_DIR / f"daily_intel_{d.strftime('%Y%m%d')}.json"


def fetch_one_date(d: date, force_refresh: bool, log: logging.Logger) -> bool:
    """Process one date.  Returns True on success.

    Raises on any error so the caller stops the loop (per project rules).
    Skips silently if the daily-intel JSON already exists and force_refresh=False.
    """
    out_path = intel_path(d)
    if out_path.exists() and not force_refresh:
        log.info("history.skip_cached", extra={"date": str(d), "path": str(out_path)})
        return False

    t0 = time.time()
    dfs = fetch_for_date(d, keep_raw=False, keep_parsed=False)
    intel = build_daily_intel(d, dfs)
    written = write_intel_json(intel, ROOT)
    dt = time.time() - t0
    log.info(
        "history.date_done",
        extra={"date": str(d), "n_features": len(intel),
               "elapsed_s": round(dt, 2), "path": str(written)},
    )
    return True


def _last_completed_nse_date() -> date:
    """Most recent NSE trading day strictly before today.

    Avoids fetching today's reports while NSE is mid-publishing (~17:00 IST).
    """
    nse = mcal.get_calendar("NSE")
    today = date.today()
    sched = nse.schedule(start_date=str(today.replace(day=1)), end_date=str(today))
    cutoff = sched.index[sched.index.date < today]
    if len(cutoff) == 0:
        raise SystemExit(f"No NSE trading day before {today} in current month window")
    return cutoff[-1].date()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2025-09-01")
    p.add_argument("--end",   default=str(_last_completed_nse_date()))
    p.add_argument("--batch", type=int, default=10**9,
                   help="max dates to process in one invocation")
    p.add_argument("--max-seconds", type=float, default=38.0,
                   help="stop accepting new dates after this many seconds (sandbox slice safety)")
    p.add_argument("--force", action="store_true",
                   help="re-process even if daily_intel_*.json exists")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log = logging.getLogger("nse_history")

    start = _parse_date(args.start)
    end   = _parse_date(args.end)

    days = trading_days(start, end)
    log.info("history.start",
             extra={"start": str(start), "end": str(end),
                    "trading_days": len(days), "batch": args.batch})

    INTEL_DIR.mkdir(parents=True, exist_ok=True)
    done_already = sum(1 for d in days if intel_path(d).exists())
    log.info("history.already_have", extra={"count": done_already, "of": len(days)})

    processed_this_run = 0
    skipped = 0
    t_start = time.time()
    for d in days:
        elapsed = time.time() - t_start
        if processed_this_run >= args.batch or elapsed >= args.max_seconds:
            log.info("history.budget_hit",
                     extra={"processed": processed_this_run,
                            "elapsed_s": round(elapsed, 1),
                            "max_s": args.max_seconds,
                            "remaining": len(days) - days.index(d)})
            break
        try:
            did = fetch_one_date(d, force_refresh=args.force, log=log)
        except Exception as e:
            log.error("history.date_failed",
                      extra={"date": str(d), "err": repr(e)})
            raise  # hard-fail per project rules
        if did:
            processed_this_run += 1
        else:
            skipped += 1

    remaining = sum(1 for d in days if not intel_path(d).exists())
    log.info("history.batch_summary",
             extra={"processed": processed_this_run, "skipped": skipped,
                    "remaining_in_window": remaining})
    print(f"\nProcessed: {processed_this_run}  Skipped (cached): {skipped}  "
          f"Remaining in window: {remaining}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
