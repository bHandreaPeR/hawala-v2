"""
Currency Derivatives — Daily Volatility (X_VOLT_{DDMMYYYY}.csv).

Schema (verified 04-May-2026):
    DATE , SYMBOL,
    UNDERLYING DAILY VOLATILITY, UNDERLYING ANNUALISED VOLATILITY,
     FUTURES VOLATILITY, FUTURES ANNUALISED VOLATILITY,
    APPLICABLE VOLATILITY, APPLICABLE ANNUALISED VOLATILITY

Trailing/leading whitespace in headers and numeric cells.  Values are
already in % (e.g. 0.443383 means 0.443% daily volatility).
"""
from __future__ import annotations
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "CD_VOLATILITY"

_RENAME = {
    "DATE": "date",
    "SYMBOL": "symbol",
    "UNDERLYING DAILY VOLATILITY": "spot_vol_d_pct",
    "UNDERLYING ANNUALISED VOLATILITY": "spot_vol_annual_pct",
    "FUTURES VOLATILITY": "fut_vol_d_pct",
    "FUTURES ANNUALISED VOLATILITY": "fut_vol_annual_pct",
    "APPLICABLE VOLATILITY": "applicable_vol_d_pct",
    "APPLICABLE ANNUALISED VOLATILITY": "applicable_vol_annual_pct",
}


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    df = pd.read_csv(raw_path, skipinitialspace=True)
    df.columns = [c.strip() for c in df.columns]

    missing = set(_RENAME.keys()) - set(df.columns)
    if missing:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing columns",
            diagnostic={"missing": sorted(missing), "have": list(df.columns)},
        )

    df = df.rename(columns=_RENAME)
    df["symbol"] = df["symbol"].astype(str).str.strip()

    for c in (v for v in _RENAME.values() if v not in ("date", "symbol")):
        df[c] = pd.to_numeric(df[c], errors="raise").astype("float64")

    df["date"] = pd.to_datetime(df["date"], errors="raise", format="%d-%b-%y").dt.date

    actual = df["date"].iloc[0]
    if actual != trade_date:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="trade-date mismatch",
            diagnostic={"expected": str(trade_date), "found": str(actual)},
        )

    return df.set_index("symbol")
