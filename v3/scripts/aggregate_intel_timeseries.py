"""
Aggregate every daily_intel_{YYYYMMDD}.json into a single
intel_timeseries.parquet, indexed by trade_date.

- One row per trade-date.
- Scalar columns only (lists/dicts kept as JSON strings if encountered).
- Per project rules: hard-fail on duplicate or unparseable trade_date.

Usage:
    cd "Hawala v2/Hawala v2"
    python3 v3/scripts/aggregate_intel_timeseries.py
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
INTEL_DIR = ROOT / "v3" / "cache" / "nse_reports"
OUT = INTEL_DIR / "intel_timeseries.parquet"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=str(OUT))
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log = logging.getLogger("aggregate")

    files = sorted(INTEL_DIR.glob("daily_intel_*.json"))
    log.info("aggregate.start", extra={"n_files": len(files)})
    if not files:
        raise SystemExit("no daily_intel_*.json files found")

    rows: list[dict] = []
    for fp in files:
        try:
            d = json.loads(fp.read_text())
        except Exception as e:
            raise RuntimeError(f"Unable to parse {fp}: {e}") from e

        # Legacy field-name harmonisation:
        # - 'skipped_optional' (interim policy) -> rename to 'skipped_fetch'
        # - earliest files (pre-policy) had neither -> default to []
        if "skipped_optional" in d and "skipped_fetch" not in d:
            d["skipped_fetch"] = d.pop("skipped_optional")
        elif "skipped_optional" in d:
            d.pop("skipped_optional")
        if "skipped_fetch" not in d:
            d["skipped_fetch"] = []

        # Flatten any lists/dicts to JSON strings; everything else stays scalar.
        flat: dict = {}
        for k, v in d.items():
            if isinstance(v, (list, dict)):
                flat[k] = json.dumps(v, default=str)
            else:
                flat[k] = v
        rows.append(flat)

    df = pd.DataFrame(rows)

    if "trade_date" not in df.columns:
        raise RuntimeError("missing trade_date in some intel JSONs")

    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
    if df["trade_date"].duplicated().any():
        dups = df.loc[df["trade_date"].duplicated(keep=False), "trade_date"].tolist()
        raise RuntimeError(f"duplicate trade_dates: {sorted(set(map(str,dups)))}")

    df = df.sort_values("trade_date").reset_index(drop=True)

    # Ensure column types are parquet-friendly: cast object columns to string
    # where they contain only str/None.
    for c in df.columns:
        if df[c].dtype == "object":
            sample = df[c].dropna().head(5).tolist()
            if all(isinstance(x, str) for x in sample) and sample:
                df[c] = df[c].astype("string")

    df.to_parquet(args.out, index=False)
    log.info(
        "aggregate.done",
        extra={"path": args.out, "rows": len(df), "cols": df.shape[1]},
    )

    # Sanity printout
    print(f"\nWrote {args.out}")
    print(f"Shape: {df.shape}")
    print(f"Date range: {df['trade_date'].min()} -> {df['trade_date'].max()}")
    print(f"\nColumn coverage (% non-null):")
    cov = (df.notna().mean() * 100).round(1).sort_values()
    print(cov.head(15).to_string())
    print("...")
    print(cov.tail(15).to_string())
    print(f"\nFully-populated columns: {(cov == 100.0).sum()}/{df.shape[1]}")
    print(f"Skipped-fetch days     : {(df['skipped_fetch'].astype(str).str.len() > 2).sum()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
