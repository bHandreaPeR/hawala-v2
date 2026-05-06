"""
F&O Daily Volatility (FOVOLT_{DDMMYYYY}.csv).

Schema (verified 04-May-2026):
    Date, Symbol, Underlying Close Price (A), Underlying Previous Day Close Price (B),
    Underlying Log Returns (C) = LN(A/B),
    Previous Day Underlying Volatility (D),
    Current Day Underlying Daily Volatility (E) = Sqrt(0.995*D*D + 0.005* C*C),
    Underlying Annualised Volatility (F) = E*sqrt(365),
    Futures Close Price (G), Futures Previous Day Close Price (H),
    Futures Log Returns (I) = LN(G/H),
    Previous Day Futures Volatility (J),
    Current Day Futures Daily Volatility (K) = Sqrt(0.995*J*J + 0.005* I*I),
    Futures Annualised Volatility (L) = K*sqrt(365),
    Applicable Daily Volatility (M) = Max(E or K),
    Applicable Annualised Volatility (N) = Max(F or L)

Numeric cells have leading whitespace (e.g. "  0.02197319").
"""
from __future__ import annotations
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "FO-VOLATILITY"

# Map verbose original headers -> short names we use downstream.
_RENAME = {
    "Date": "date",
    "Symbol": "symbol",
    "Underlying Close Price (A)": "spot_close",
    "Underlying Previous Day Close Price (B)": "spot_prev_close",
    "Underlying Log Returns (C) = LN(A/B)": "spot_log_ret",
    "Previous Day Underlying Volatility (D)": "spot_vol_prev",
    "Current Day Underlying Daily Volatility (E) = Sqrt (0.995*D*D + 0.005* C*C)": "spot_vol_d",
    "Underlying Annualised Volatility (F) = E*sqrt(365)": "spot_vol_annual",
    "Futures Close Price (G)": "fut_close",
    "Futures Previous Day Close Price (H)": "fut_prev_close",
    "Futures Log Returns (I) = LN(G/H)": "fut_log_ret",
    "Previous Day Futures Volatility (J)": "fut_vol_prev",
    "Current Day Futures Daily Volatility (K) = Sqrt (0.995*J*J + 0.005* I*I)": "fut_vol_d",
    "Futures Annualised Volatility (L) = K*sqrt(365)": "fut_vol_annual",
    "Applicable Daily Volatility (M) = Max (E or K)": "applicable_vol_d",
    "Applicable Annualised Volatility (N) = Max (F or L)": "applicable_vol_annual",
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

    num_cols = [v for v in _RENAME.values() if v not in ("date", "symbol")]
    for c in num_cols:
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

    df = df.set_index("symbol")
    return df
