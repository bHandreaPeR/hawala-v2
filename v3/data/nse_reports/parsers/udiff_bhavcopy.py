"""
F&O UDiFF Common Bhavcopy Final (zip → csv).

Schema (verified 04-May-2026):
    TradDt, BizDt, Sgmt, Src, FinInstrmTp, FinInstrmId, ISIN, TckrSymb,
    SctySrs, XpryDt, FininstrmActlXpryDt, StrkPric, OptnTp, FinInstrmNm,
    OpnPric, HghPric, LwPric, ClsPric, LastPric, PrvsClsgPric,
    UndrlygPric, SttlmPric, OpnIntrst, ChngInOpnIntrst,
    TtlTradgVol, TtlTrfVal, TtlNbOfTxsExctd, SsnId, NewBrdLotQty,
    Rmks, Rsvd1..Rsvd4

FinInstrmTp: STF=stock fut, STO=stock opt, IDF=index fut, IDO=index opt.
"""
from __future__ import annotations
import io
import zipfile
from datetime import date

import pandas as pd

from ..errors import NSEReportParseError

REPORT_KEY = "FO-UDIFF-BHAVCOPY-CSV"

REQUIRED_COLS = {
    "TradDt", "FinInstrmTp", "TckrSymb", "XpryDt", "StrkPric", "OptnTp",
    "ClsPric", "PrvsClsgPric", "UndrlygPric", "SttlmPric",
    "OpnIntrst", "ChngInOpnIntrst", "TtlTradgVol",
}

_FLOAT_COLS = (
    "StrkPric", "OpnPric", "HghPric", "LwPric", "ClsPric", "LastPric",
    "PrvsClsgPric", "UndrlygPric", "SttlmPric", "TtlTrfVal",
)
_INT_COLS = (
    "OpnIntrst", "ChngInOpnIntrst", "TtlTradgVol", "TtlNbOfTxsExctd",
    "NewBrdLotQty",
)


def parse(raw_path: str, trade_date: date) -> pd.DataFrame:
    with zipfile.ZipFile(raw_path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise NSEReportParseError(
                report_key=REPORT_KEY,
                path=raw_path,
                reason="no csv inside zip",
                diagnostic={"members": zf.namelist()},
            )
        if len(names) != 1:
            raise NSEReportParseError(
                report_key=REPORT_KEY,
                path=raw_path,
                reason="expected exactly one csv in zip",
                diagnostic={"members": names},
            )
        with zf.open(names[0]) as f:
            df = pd.read_csv(io.BytesIO(f.read()), low_memory=False)

    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="missing required columns",
            diagnostic={"missing": sorted(missing), "have": list(df.columns)},
        )

    # Type coercion — explicit, no silent fallback.
    for c in _FLOAT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
    for c in _INT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    df["TradDt"]  = pd.to_datetime(df["TradDt"], errors="raise").dt.date
    df["XpryDt"]  = pd.to_datetime(df["XpryDt"], errors="raise").dt.date

    actual = df["TradDt"].iloc[0]
    if actual != trade_date:
        raise NSEReportParseError(
            report_key=REPORT_KEY,
            path=raw_path,
            reason="trade-date mismatch",
            diagnostic={"expected": str(trade_date), "found": str(actual)},
        )

    return df
