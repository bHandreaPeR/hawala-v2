"""
IRD FPI Combined Long Position.

XLSX disguised as .csv (PK header) — same trick as nbf_fii_long.

Sheet 'Stock Exchange Format' layout (verified 04-May-2026):
    Row 0: ['Date', 'Aggregate Net Long Position of FPIs',
            'Aggregate Permissible Limit', 'Available Limit',
            'Aggregate Net Long Position Percentage',
            'Breach of 90% Threshold', 'Date of Breach']
    Row 1: [NaT, 'INR Crore', 'INR Crore', 'INR Crore', NaN, NaN, NaN]
    Row 2: [NaT, 'A', 'B', 'C = A - B', 'D = (A/B*100)', NaN, NaN]
    Row 3: [date, A, B, C, D, breach_yn, NaT]

Outputs one-row DataFrame with parsed numeric fields.
"""
from __future__ import annotations
import zipfile
from datetime import date

import pandas as pd

from ..errors import NSEReportFetchError, NSEReportParseError

REPORT_KEY = "NBF-FPI-LONG-PSN"


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    try:
        raw = pd.read_excel(raw_path, sheet_name=0, header=None, engine="openpyxl")
    except (zipfile.BadZipFile, OSError) as e:
        # NSE occasionally publishes truncated/corrupt XLSX (missing trailing
        # bytes — observed 2025-11-03).  Reclassify as a fetch issue so the
        # caller's skip-on-fetch-failure policy applies.
        raise NSEReportFetchError(
            report_key=REPORT_KEY,
            url=raw_path,
            status=None,
            body_excerpt=None,
            reason=f"corrupt xlsx from NSE: {type(e).__name__}: {e}",
        )

    if raw.shape[1] < 7:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason=f"expected >=7 cols, got {raw.shape[1]}",
            diagnostic={"shape": list(raw.shape)},
        )

    # find the data row: cell (i, 0) is a real datetime
    data_idx = None
    for i in range(raw.shape[0]):
        cell = raw.iat[i, 0]
        if isinstance(cell, pd.Timestamp) and not pd.isna(cell):
            data_idx = i
            break
        if hasattr(cell, "year") and not pd.isna(cell):
            data_idx = i
            break
    if data_idx is None:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="no data row with parseable date",
            diagnostic={
                "col0_types": [type(raw.iat[i, 0]).__name__ for i in range(min(8, raw.shape[0]))]
            },
        )

    row = raw.iloc[data_idx, :7].tolist()
    file_date = pd.to_datetime(row[0]).date()
    if file_date != trade_date:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="trade-date mismatch",
            diagnostic={"expected": str(trade_date), "found": str(file_date)},
        )

    def _num(v, label):
        try:
            return float(v)
        except (TypeError, ValueError):
            raise NSEReportParseError(
                report_key=REPORT_KEY,
                path=raw_path,
                reason=f"non-numeric {label}",
                diagnostic={"value": repr(v)},
            )

    fpi_long_cr      = _num(row[1], "fpi_long_cr")
    permissible_cr   = _num(row[2], "permissible_cr")
    available_cr     = _num(row[3], "available_cr")
    pct_used         = _num(row[4], "pct_used")
    breach_flag      = str(row[5]).strip()

    df = pd.DataFrame(
        [
            {
                "date": file_date,
                "fpi_long_cr": fpi_long_cr,
                "permissible_cr": permissible_cr,
                "available_cr": available_cr,
                "pct_used": pct_used,
                "breach_90": breach_flag,
            }
        ]
    ).set_index("date")
    return df
