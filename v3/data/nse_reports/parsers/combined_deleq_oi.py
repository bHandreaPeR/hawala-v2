"""
F&O Combined Delta-Equivalent Open Interest across exchanges.

Schema (verified 04-May-2026):
    Date, ISIN, Scrip Name, Symbol,
    Notional Open Interest, Portfolio-wise Futures Equivalent Open Interest

Schema variant observed (verified 01-Sep-2025):
    Date, ISIN, Scrip Name, Symbol,
    NotionalOpen Interest, Portfolio-wiseFutures Equivalent Open Interest
        (missing space after the first word in last two columns)

We normalize headers by collapsing all whitespace before matching.
Date format: DD-MM-YYYY.
"""
from __future__ import annotations
import re
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "FO-COMBINE-OI-DELEQ"

# Canonical lookup keyed by whitespace-stripped lowercase header.
_CANON = {
    "date":                                          "date",
    "isin":                                          "isin",
    "scripname":                                     "scrip_name",
    "symbol":                                        "symbol",
    "notionalopeninterest":                          "notional_oi",
    "portfolio-wisefuturesequivalentopeninterest":   "fut_eq_oi",
}

_REQUIRED_OUT = ("date", "isin", "scrip_name", "symbol", "notional_oi", "fut_eq_oi")


def _norm(s: str) -> str:
    return re.sub(r"\s+", "", s.strip()).lower()


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    df = pd.read_csv(raw_path)
    df.columns = [c.strip() for c in df.columns]

    rename: dict[str, str] = {}
    for col in df.columns:
        canon = _CANON.get(_norm(col))
        if canon is not None:
            rename[col] = canon
    df = df.rename(columns=rename)

    missing = set(_REQUIRED_OUT) - set(df.columns)
    if missing:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing columns after normalization",
            diagnostic={"missing": sorted(missing), "have": list(df.columns)},
        )
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["notional_oi"] = pd.to_numeric(df["notional_oi"], errors="raise").astype("int64")
    df["fut_eq_oi"]   = pd.to_numeric(df["fut_eq_oi"],   errors="raise").astype("float64")
    # Date variants observed: '04-05-2026' (DD-MM-YYYY), '01/09/2025' (DD/MM/YYYY).
    sample = str(df["date"].iloc[0])
    if "/" in sample:
        fmt = "%d/%m/%Y"
    elif "-" in sample:
        fmt = "%d-%m-%Y"
    else:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="unrecognized date format",
            diagnostic={"sample": sample},
        )
    df["date"] = pd.to_datetime(df["date"], errors="raise", format=fmt).dt.date

    actual = df["date"].iloc[0]
    if actual != trade_date:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="trade-date mismatch",
            diagnostic={"expected": str(trade_date), "found": str(actual)},
        )

    return df.set_index("symbol")
