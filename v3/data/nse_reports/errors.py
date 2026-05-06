"""
Specific error types for NSE report pipeline.
Per project rules: no catch-all handlers; every failure carries debug context.
"""
from __future__ import annotations
from typing import Optional


class NSEReportError(Exception):
    """Base class for all NSE-report-pipeline errors."""


class NSEReportFetchError(NSEReportError):
    """HTTP-level failure (non-200, body too small, wrong content-type)."""

    def __init__(
        self,
        report_key: str,
        url: str,
        status: Optional[int],
        body_excerpt: Optional[bytes] = None,
        reason: str = "",
    ):
        self.report_key = report_key
        self.url = url
        self.status = status
        self.body_excerpt = (body_excerpt or b"")[:500]
        self.reason = reason
        super().__init__(
            f"NSE fetch failed | key={report_key} | status={status} | "
            f"reason={reason} | url={url} | body[:500]={self.body_excerpt!r}"
        )


class NSEReportParseError(NSEReportError):
    """Parser found unexpected structure (column missing, wrong types, NIL)."""

    def __init__(
        self,
        report_key: str,
        path: str,
        reason: str,
        diagnostic: Optional[dict] = None,
    ):
        self.report_key = report_key
        self.path = path
        self.reason = reason
        self.diagnostic = diagnostic or {}
        super().__init__(
            f"NSE parse failed | key={report_key} | path={path} | "
            f"reason={reason} | diag={self.diagnostic}"
        )


class NSEReportMissingError(NSEReportError):
    """Report not present in the daily-reports listing for the requested date."""

    def __init__(self, report_key: str, segment: str, trade_date: str):
        self.report_key = report_key
        self.segment = segment
        self.trade_date = trade_date
        super().__init__(
            f"NSE report not listed | segment={segment} | "
            f"key={report_key} | trade_date={trade_date}"
        )
