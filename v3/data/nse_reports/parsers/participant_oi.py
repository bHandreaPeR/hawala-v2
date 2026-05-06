"""
F&O Participant-wise Open Interest (CSV).

Sample (04-May-2026):
    Row 0: '"Participant wise Open Interest (no. of contracts) ... May 04, 2026"'
    Row 1: 'Client Type, Future Index Long, Future Index Short, ...'
    Row 2..N: Client/DII/FII/Pro/TOTAL

Quirks:
- Title row uses doubled quotes: ""...""
- Some columns have trailing whitespace ("Future Stock Short       ").
"""
from __future__ import annotations
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "FO-PARTICIPANTWISE-OI"

EXPECTED_CLIENTS = ("Client", "DII", "FII", "Pro", "TOTAL")
NUM_COLS = (
    "Future Index Long", "Future Index Short",
    "Future Stock Long", "Future Stock Short",
    "Option Index Call Long", "Option Index Put Long",
    "Option Index Call Short", "Option Index Put Short",
    "Option Stock Call Long", "Option Stock Put Long",
    "Option Stock Call Short", "Option Stock Put Short",
    "Total Long Contracts", "Total Short Contracts",
)


def _read_table(raw_path: str) -> pd.DataFrame:
    # skip the title row (line 0); header is on line 1.
    df = pd.read_csv(raw_path, skiprows=1)
    # strip whitespace in column names
    df.columns = [c.strip() for c in df.columns]
    # strip whitespace in Client Type values
    if "Client Type" in df.columns:
        df["Client Type"] = df["Client Type"].astype(str).str.strip()
    return df


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    df = _read_table(raw_path)

    if "Client Type" not in df.columns:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing Client Type column",
            diagnostic={"have": list(df.columns)},
        )

    missing_num = set(NUM_COLS) - set(df.columns)
    if missing_num:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing numeric columns",
            diagnostic={"missing": sorted(missing_num), "have": list(df.columns)},
        )

    seen = set(df["Client Type"].tolist())
    missing_clients = set(EXPECTED_CLIENTS) - seen
    if missing_clients:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing client types",
            diagnostic={"missing": sorted(missing_clients), "seen": sorted(seen)},
        )

    for c in NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="raise").astype("int64")

    df = df.set_index("Client Type")[list(NUM_COLS)]
    df.attrs["trade_date"] = str(trade_date)
    return df
