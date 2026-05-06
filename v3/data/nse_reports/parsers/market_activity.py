"""
F&O Market Activity Report (zip with 11 files).

We extract three useful summaries:
- fo_{ddmmyyyy}.csv      : volume summary by product type (idx fut, stk fut, idx opt, ...)
- futidx{ddmmyyyy}.csv   : index futures per-symbol summary (BANKNIFTY, NIFTY, ...)
- optidx{ddmmyyyy}.csv   : index options per-symbol summary

Other members (op{date}.csv contract-level option chain, optstk, ttfut, ttopt,
fohelp.txt, futstk, futivx) are skipped — superseded by UDiFF bhavcopy or
out-of-scope.

Output: dict[str, DataFrame] keyed by "summary" / "fut_idx" / "opt_idx".
"""
from __future__ import annotations
import io
import zipfile
from datetime import date
from typing import Dict

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "FO-MARKET-ACTIVITY-REPORT"


def _read_csv_skip_title(raw: bytes, expected_cols: tuple[str, ...]) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(raw), skiprows=1, skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]
    missing = set(expected_cols) - set(df.columns)
    if missing:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path="<zip-member>",
            reason="market-activity sub-file missing columns",
            diagnostic={"missing": sorted(missing), "have": list(df.columns)},
        )
    return df


def _resolve(names: list[str], target: str) -> str:
    """Find a member whose basename exactly matches `target`.

    Older NSE zips nest files under fo{DDMMYYYY}/, newer ones place
    them at the root.  We accept both.
    """
    for n in names:
        if n.split("/")[-1] == target:
            return n
    raise NSEReportParseError(
        report_key=REPORT_KEY,
        path="<zip>",
        reason=f"no zip member with basename {target}",
        diagnostic={"members": names},
    )


def parse(raw_path: str, trade_date: date) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    dd = trade_date.strftime("%d%m%Y")

    with zipfile.ZipFile(raw_path) as zf:
        names = zf.namelist()
        wanted = {
            "summary": f"fo_{dd}.csv",
            "fut_idx": f"futidx{dd}.csv",
            "opt_idx": f"optidx{dd}.csv",
        }
        for slot, fname in wanted.items():
            member = _resolve(names, fname)
            raw = zf.read(member)

            if slot == "summary":
                df = _read_csv_skip_title(
                    raw, ("Product", "No of Contracts", "Traded Value (Rs. Crs.)")
                )
                df["Product"] = df["Product"].astype(str).str.strip()
                df["No of Contracts"] = pd.to_numeric(
                    df["No of Contracts"], errors="raise"
                ).astype("int64")
                df["Traded Value (Rs. Crs.)"] = pd.to_numeric(
                    df["Traded Value (Rs. Crs.)"], errors="raise"
                ).astype("float64")
                out[slot] = df.set_index("Product")

            else:  # fut_idx / opt_idx
                df = _read_csv_skip_title(
                    raw,
                    (
                        "Symbol",
                        "No of Contracts Traded",
                        "Traded Quantity",
                        "Total Traded Value (Rs. In Crs.)",
                        "Open interest (Qty.) as at end of trading hrs.",
                    ),
                )
                df["Symbol"] = df["Symbol"].astype(str).str.strip()
                df["No of Contracts Traded"] = pd.to_numeric(
                    df["No of Contracts Traded"], errors="raise"
                ).astype("int64")
                df["Traded Quantity"] = pd.to_numeric(
                    df["Traded Quantity"], errors="raise"
                ).astype("int64")
                df["Total Traded Value (Rs. In Crs.)"] = pd.to_numeric(
                    df["Total Traded Value (Rs. In Crs.)"], errors="raise"
                ).astype("float64")
                df["Open interest (Qty.) as at end of trading hrs."] = pd.to_numeric(
                    df["Open interest (Qty.) as at end of trading hrs."],
                    errors="raise",
                ).astype("int64")
                # opt_idx has multiple rows per symbol (one per expiry).
                out[slot] = df

    return out
