"""
Phase-1 driver: fetch + parse all Tier-A NSE reports for a single trade-date,
build the daily-intel JSON, and write it to v3/cache/nse_reports/.

Default trade-date: 2026-05-04.

Usage:
    cd "Hawala v2/Hawala v2"
    python3 v3/scripts/fetch_nse_reports_one_day.py            # 2026-05-04
    python3 v3/scripts/fetch_nse_reports_one_day.py 2026-05-02
    python3 v3/scripts/fetch_nse_reports_one_day.py --force    # ignore raw cache
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from v3.data.nse_reports.fetch import fetch_for_date          # noqa: E402
from v3.data.nse_reports.intel import build_daily_intel, write_intel_json  # noqa: E402
from v3.data.nse_reports.registry import REGISTRY, tier_a_keys  # noqa: E402


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("trade_date", nargs="?", default="2026-05-04",
                   help="Trade date in YYYY-MM-DD (default 2026-05-04)")
    p.add_argument("--force", action="store_true",
                   help="Re-download even if raw cache exists")
    p.add_argument("--keys", nargs="*", default=None,
                   help="Subset of report keys (default: all Tier-A)")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log = logging.getLogger("nse_oneday")

    td = _parse_date(args.trade_date)
    keys = args.keys or tier_a_keys()

    log.info(
        "phase1.start",
        extra={"trade_date": str(td), "n_keys": len(keys),
               "force": args.force, "root": str(ROOT)},
    )

    dfs = fetch_for_date(td, keys=keys, force=args.force)

    log.info("phase1.fetched", extra={"keys_returned": list(dfs.keys())})

    intel = build_daily_intel(td, dfs)

    out_path = write_intel_json(intel, ROOT)
    log.info("phase1.intel_written", extra={"path": str(out_path),
                                             "n_features": len(intel)})

    # Pretty print to stdout for the caller to inspect.
    print("\n========== DAILY INTEL ==========")
    print(json.dumps(intel, indent=2, default=str))
    print("========== END ==========\n")
    print(f"Total features: {len(intel)}")
    print(f"JSON written:    {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
