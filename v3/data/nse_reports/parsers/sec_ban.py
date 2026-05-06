"""
F&O Securities in ban period.

Two real formats observed:
- NIL day:
      Securities in Ban For Trade Date {DD-MON-YYYY}: NIL
  (one line, no header, no data rows)
- non-NIL day:
      Securities in Ban For Trade Date {DD-MON-YYYY}:
      1,RBLBANK
      2,VEDL
      ...
  (title line, then 'sr_no,symbol' rows with NO header row)

Returns a DataFrame indexed by symbol; empty when NIL.
"""
from __future__ import annotations
import io
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "FO-SEC-BAN"


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    with open(raw_path, "rb") as f:
        text = f.read().decode("utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()]

    if not lines:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="empty file",
            diagnostic={},
        )

    first = lines[0]

    # NIL day: title line ends with 'NIL' (or contains NIL)
    if "NIL" in first.upper() and "BAN" in first.upper():
        return pd.DataFrame({"symbol": []}).set_index("symbol")

    # non-NIL: first line is a title (no comma OR ends with ':'), then data lines
    title_first = ("," not in first) or first.rstrip().endswith(":")
    data_lines = lines[1:] if title_first else lines

    # rows are '<sr_no>,<symbol>'
    parsed: list[str] = []
    for ln in data_lines:
        parts = ln.split(",", 1)
        if len(parts) != 2:
            raise NSEReportParseError(
                report_key=REPORT_KEY,
                path=raw_path,
                reason="malformed sec-ban data line",
                diagnostic={"line": ln, "head": text[:200]},
            )
        parsed.append(parts[1].strip())

    df = pd.DataFrame({"symbol": parsed}).set_index("symbol")
    return df
