"""
IRD Daily FII Gross Long Position.

File extension is .csv but payload is XLSX (PK header) — verified 04-May-2026.

Sheet 'Stock Exchange Format' layout:
    Row 0:  ['Aggregate Gross Long Position in IRF by all FIIs at the end of Day:',
             NaN, NaN, NaN]
    Row 1:  ['Product Category',
             'Total Value of Aggregate Gross Long Position of FIIs(excluding Long Term Investors)\n(A)',
             'Total Value of Aggregate Gross Long Position of FIIs investors who are registered with SEBI as Long Term Investors viz., Sovereign Wealth Funds, Insurance Funds etc.\n(B)',
             'Total Value of Aggregate Gross Long Position of ALL FIIs\n[C = A+B]']
    Row 2:  [NaN, 'In. Rs', 'In. Rs', 'In. Rs']
    Row 3:  [NaN, NaN, NaN, NaN]
    Row 4:  ['Interest Rate Futures', <A>, <B>, <C>]
    Row 5:  [NaN, NaN, NaN, NaN]
    Row 6:  ['*Settlement Price as on May 04, 2026', NaN, NaN, NaN]

Some files might have multiple product rows (only IRF observed for 04-May).
Output one-row DataFrame: a, b, c (INR).
"""
from __future__ import annotations
import zipfile
from datetime import date

import pandas as pd

from ..errors import NSEReportFetchError, NSEReportParseError

REPORT_KEY = "NBF-FII-GROSS-LONG-POSITION"


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

    if raw.shape[1] < 4:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason=f"expected >=4 cols, got {raw.shape[1]}",
            diagnostic={"shape": list(raw.shape)},
        )

    # locate the row whose first cell starts with 'Interest Rate'
    cat_col = raw.iloc[:, 0].astype(str)
    mask = cat_col.str.contains("Interest Rate", case=False, na=False)
    if not mask.any():
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="no 'Interest Rate Futures' row",
            diagnostic={"col0_sample": cat_col.head(10).tolist()},
        )

    rows = []
    for i in raw.index[mask]:
        cat  = str(raw.iat[i, 0]).strip()
        vals = []
        for j in (1, 2, 3):
            v = raw.iat[i, j]
            try:
                v_num = float(v)
            except (TypeError, ValueError):
                raise NSEReportParseError(
                    report_key=REPORT_KEY,
                    path=raw_path,
                    reason=f"non-numeric in column {j} for row '{cat}'",
                    diagnostic={"value": repr(v)},
                )
            vals.append(v_num)
        rows.append({"category": cat, "fii_excl_lt": vals[0],
                     "fii_lt": vals[1], "fii_total": vals[2]})

    df = pd.DataFrame(rows).set_index("category")
    df.attrs["trade_date"] = str(trade_date)
    return df
