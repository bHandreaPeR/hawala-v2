"""
v3/data/nse_reports
===================
Fetch and parse NSE EOD derivatives reports into typed DataFrames + a daily
intelligence JSON suitable for feeding the FII/DII classifier.

Public API:
    from v3.data.nse_reports.fetch import fetch_for_date
    from v3.data.nse_reports.intel import build_daily_intel

Source pages:
    https://www.nseindia.com/all-reports-derivatives
    https://www.nseindia.com/api/daily-reports?key={FO|CD|NBF}
"""
from .errors import (
    NSEReportError,
    NSEReportFetchError,
    NSEReportParseError,
    NSEReportMissingError,
)

__all__ = [
    "NSEReportError",
    "NSEReportFetchError",
    "NSEReportParseError",
    "NSEReportMissingError",
]
