"""
F&O FII Derivatives Statistics (.xls, binary BIFF).

Layout (verified 04-May-2026, sheet 'sheet1'):
    Row 0:  ["FII DERIVATIVES STATISTICS FOR 04-May-2026", NaN x6]   (title)
    Row 1:  [NaN, "BUY", "BUY", "SELL", "SELL", "OPEN INTEREST AT THE END OF THE DAY", NaN]
    Row 2:  [NaN, "No. of contracts", "Amt in Crores", "No. of contracts",
             "Amt in Crores", "No. of contracts", "Amt in Crores"]
    Row 3+: data rows, e.g.
        ["INDEX FUTURES",   11645, 1886.22, 19741, 3157.34, 251188, 40171.74]
        ["BANKNIFTY FUTURES", ...]
        ["NIFTY FUTURES", ...]
        [NaN, NaN, ...]   (blank separator)
        ["INDEX OPTIONS",  ...]
        ["NIFTY OPTIONS",  ...]
        ...
        ["STOCK FUTURES",  ...]
        ["STOCK OPTIONS",  ...]

Output columns (flat):
    category (str), buy_contracts (int), buy_crore (float),
    sell_contracts (int), sell_crore (float),
    eod_oi_contracts (int), eod_oi_crore (float)

We keep all data rows including subcategories like "NIFTY FUTURES",
"BANKNIFTY OPTIONS" etc. so downstream features can pick what they need.
"""
from __future__ import annotations
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "FO-FII-DERIVATIVE-STAT"

OUT_COLS = (
    "buy_contracts", "buy_crore",
    "sell_contracts", "sell_crore",
    "eod_oi_contracts", "eod_oi_crore",
)


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    raw = pd.read_excel(raw_path, sheet_name=0, header=None, engine="xlrd")

    if raw.shape[1] < 7:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason=f"expected >=7 columns, got {raw.shape[1]}",
            diagnostic={"shape": list(raw.shape)},
        )

    # Title in [0,0] should reference the trade-date.
    title = str(raw.iloc[0, 0]) if not pd.isna(raw.iloc[0, 0]) else ""
    if "FII" not in title.upper():
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="title row missing 'FII' marker",
            diagnostic={"title": title[:120]},
        )

    # Data rows: skip first 3 (title + 2 header rows). Drop empty separator rows.
    data = raw.iloc[3:, :7].copy()
    data.columns = ["category"] + list(OUT_COLS)
    data = data[data["category"].notna()].copy()
    data["category"] = data["category"].astype(str).str.strip()
    data = data[data["category"] != ""].copy()

    if data.empty:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="no data rows",
            diagnostic={"shape": list(raw.shape)},
        )

    for c in ("buy_contracts", "sell_contracts", "eod_oi_contracts"):
        data[c] = pd.to_numeric(data[c], errors="coerce").astype("Int64")
    for c in ("buy_crore", "sell_crore", "eod_oi_crore"):
        data[c] = pd.to_numeric(data[c], errors="coerce").astype("float64")

    # Drop rows that were not actual numeric data (e.g. trailing notes).
    data = data.dropna(subset=["buy_contracts", "sell_contracts"]).copy()

    data["net_contracts"] = (
        data["buy_contracts"].astype("Int64") - data["sell_contracts"].astype("Int64")
    )
    data["net_crore"] = data["buy_crore"] - data["sell_crore"]

    data = data.set_index("category")
    data.attrs["trade_date"] = str(trade_date)
    return data
