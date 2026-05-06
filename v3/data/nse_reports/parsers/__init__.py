"""
Per-report parsers.  Each module exposes a single function:
    parse(raw_path: str, trade_date: date) -> pandas.DataFrame
that raises NSEReportParseError on any unexpected structure.
"""
