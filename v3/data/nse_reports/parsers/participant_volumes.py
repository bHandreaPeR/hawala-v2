"""
F&O Participant-wise Trading Volume (CSV).
Schema is identical to participant_oi.py — same columns, same client types.
"""
from __future__ import annotations
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError
from . import participant_oi as _poi  # reuse machinery

REPORT_KEY = "FO-PARTICIPANTWISE-TRADING-VOL"


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    df = _poi._read_table(raw_path)

    if "Client Type" not in df.columns:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing Client Type column",
            diagnostic={"have": list(df.columns)},
        )
    missing_num = set(_poi.NUM_COLS) - set(df.columns)
    if missing_num:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing numeric columns",
            diagnostic={"missing": sorted(missing_num), "have": list(df.columns)},
        )

    seen = set(df["Client Type"].tolist())
    missing_clients = set(_poi.EXPECTED_CLIENTS) - seen
    if missing_clients:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing client types",
            diagnostic={"missing": sorted(missing_clients), "seen": sorted(seen)},
        )

    for c in _poi.NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="raise").astype("int64")

    df = df.set_index("Client Type")[list(_poi.NUM_COLS)]
    df.attrs["trade_date"] = str(trade_date)
    return df
