"""
Currency Derivatives — Daily Settlement Prices.

Schema (verified 04-May-2026):
    DATE, INSTRUMENT, UNDERLYING, EXPIRY DATE, MTM SETTLEMENT PRICE
"""
from __future__ import annotations
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "CD-SETT-PRICE"

REQUIRED = ("DATE", "INSTRUMENT", "UNDERLYING", "EXPIRY DATE", "MTM SETTLEMENT PRICE")


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    df = pd.read_csv(raw_path)
    df.columns = [c.strip() for c in df.columns]

    missing = set(REQUIRED) - set(df.columns)
    if missing:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing columns",
            diagnostic={"missing": sorted(missing), "have": list(df.columns)},
        )

    df = df.rename(
        columns={
            "DATE": "date",
            "INSTRUMENT": "instrument",
            "UNDERLYING": "underlying",
            "EXPIRY DATE": "expiry",
            "MTM SETTLEMENT PRICE": "settle",
        }
    )
    df["instrument"] = df["instrument"].astype(str).str.strip()
    df["underlying"] = df["underlying"].astype(str).str.strip()
    df["settle"] = pd.to_numeric(df["settle"], errors="raise").astype("float64")
    df["date"]   = pd.to_datetime(df["date"], errors="raise", format="%d-%b-%Y").dt.date
    df["expiry"] = pd.to_datetime(df["expiry"], errors="raise", format="%d-%b-%Y").dt.date

    actual = df["date"].iloc[0]
    if actual != trade_date:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="trade-date mismatch",
            diagnostic={"expected": str(trade_date), "found": str(actual)},
        )
    return df
