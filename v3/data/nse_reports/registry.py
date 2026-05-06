"""
Declarative registry of Tier-A and Tier-B NSE derivatives reports.

Each entry maps a `report_key` (matches NSE's daily-reports `fileKey`) to:
- `segment`        : "FO" | "CD" | "NBF"
- `tier`           : "A" (signal-bearing, fed into intel) | "B" (fetched, evaluate later)
- `url_pattern`    : printf-style with placeholders {DDMMYYYY}, {DD-MMM-YYYY}, {YYYYMMDD}
- `parser_module`  : dotted path under v3.data.nse_reports.parsers
- `raw_ext`        : file extension to use when caching raw bytes
- `target_date`    : "T-1" (data is for previous trade date) | "T" (today's snapshot)

Date placeholder semantics (verified against 04-May-2026 listing):
- {DDMMYYYY}     -> 04052026
- {DD-MMM-YYYY}  -> 04-May-2026
- {YYYYMMDD}     -> 20260504
- {DD-MON-YYYY}  -> 04-MAY-2026 (uppercase month)

All Tier-A URLs were probed and confirmed 200 OK on 04-May-2026.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ReportSpec:
    key: str
    segment: str
    tier: str
    url_pattern: str
    parser_module: str
    raw_ext: str
    target_date: str       # "T-1" or "T"
    mandatory: bool = True # if False, a 404 is logged-and-skipped, not fatal.
                           # Used only for NSE-side publication gaps observed
                           # in the IRD/NBF segment (Sep 8 2025, Apr 1 2026).


REGISTRY: dict[str, ReportSpec] = {
    # ---------------- FO Tier-A ----------------
    "FO-UDIFF-BHAVCOPY-CSV": ReportSpec(
        key="FO-UDIFF-BHAVCOPY-CSV",
        segment="FO",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/content/fo/"
            "BhavCopy_NSE_FO_0_0_0_{YYYYMMDD}_F_0000.csv.zip"
        ),
        parser_module="v3.data.nse_reports.parsers.udiff_bhavcopy",
        raw_ext="zip",
        target_date="T-1",
    ),
    "FO-PARTICIPANTWISE-OI": ReportSpec(
        key="FO-PARTICIPANTWISE-OI",
        segment="FO",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/content/nsccl/"
            "fao_participant_oi_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.participant_oi",
        raw_ext="csv",
        target_date="T-1",
    ),
    "FO-PARTICIPANTWISE-TRADING-VOL": ReportSpec(
        key="FO-PARTICIPANTWISE-TRADING-VOL",
        segment="FO",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/content/nsccl/"
            "fao_participant_vol_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.participant_volumes",
        raw_ext="csv",
        target_date="T-1",
    ),
    "FO-FII-DERIVATIVE-STAT": ReportSpec(
        key="FO-FII-DERIVATIVE-STAT",
        segment="FO",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/content/fo/"
            "fii_stats_{DD-MMM-YYYY}.xls"
        ),
        parser_module="v3.data.nse_reports.parsers.fii_stats",
        raw_ext="xls",
        target_date="T-1",
    ),
    "FO-VOLATILITY": ReportSpec(
        key="FO-VOLATILITY",
        segment="FO",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/archives/nsccl/volt/"
            "FOVOLT_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.daily_volatility",
        raw_ext="csv",
        target_date="T-1",
    ),
    "FO-COMBINE-OI-DELEQ": ReportSpec(
        key="FO-COMBINE-OI-DELEQ",
        segment="FO",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/archives/nsccl/mwpl/"
            "combineoi_deleq_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.combined_deleq_oi",
        raw_ext="csv",
        target_date="T-1",
    ),
    "FO-MARKET-ACTIVITY-REPORT": ReportSpec(
        key="FO-MARKET-ACTIVITY-REPORT",
        segment="FO",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/archives/fo/mkt/"
            "fo{DDMMYYYY}.zip"
        ),
        parser_module="v3.data.nse_reports.parsers.market_activity",
        raw_ext="zip",
        target_date="T-1",
    ),
    "FO-SEC-BAN": ReportSpec(
        # Note: sec ban file is dated for the trade-date *T* (today), not T-1.
        key="FO-SEC-BAN",
        segment="FO",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/archives/fo/sec_ban/"
            "fo_secban_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.sec_ban",
        raw_ext="csv",
        target_date="T",
    ),
    # ---------------- NBF (IRD) Tier-A ----------------
    "NBF-FII-GROSS-LONG-POSITION": ReportSpec(
        # Despite .csv extension, payload is XLSX (PK header) — verified 04-May-2026.
        # mandatory=False: NSE publication gaps observed (e.g. 2025-09-08).
        key="NBF-FII-GROSS-LONG-POSITION",
        segment="NBF",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/archives/ird/fii/"
            "fii_longpos_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.nbf_fii_long",
        raw_ext="xlsx",
        target_date="T-1",
        mandatory=False,
    ),
    "NBF-FPI-LONG-PSN": ReportSpec(
        # mandatory=False: same NSE publication gaps as NBF-FII-GROSS-LONG-POSITION.
        key="NBF-FPI-LONG-PSN",
        segment="NBF",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/archives/ird/fii/"
            "Combined_FPI_long_psn_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.nbf_fpi_long",
        raw_ext="xlsx",
        target_date="T-1",
        mandatory=False,
    ),
    # ---------------- CD Tier-A ----------------
    "CD-SETT-PRICE": ReportSpec(
        key="CD-SETT-PRICE",
        segment="CD",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/archives/cd/sett/"
            "CDSett_prce_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.cd_settle",
        raw_ext="csv",
        target_date="T-1",
    ),
    "CD_VOLATILITY": ReportSpec(
        key="CD_VOLATILITY",
        segment="CD",
        tier="A",
        url_pattern=(
            "https://nsearchives.nseindia.com/archives/cd/volt/"
            "X_VOLT_{DDMMYYYY}.csv"
        ),
        parser_module="v3.data.nse_reports.parsers.cd_volatility",
        raw_ext="csv",
        target_date="T-1",
    ),
}


_MONTH_TITLE = ("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")
_MONTH_UPPER = tuple(m.upper() for m in _MONTH_TITLE)


def render_url(spec: ReportSpec, trade_date: date) -> str:
    """
    Substitute date placeholders in the URL pattern.

    For specs with target_date == "T", the placeholder represents *today*
    (the file is for trade-date "T", typically the ban file).  For
    target_date == "T-1", placeholder represents the trade-date itself
    (yesterday relative to the run).

    `trade_date` is always interpreted as the *trade-date the data refers to*.
    Caller is responsible for choosing the right calendar date.
    """
    d = trade_date
    repls = {
        "{DDMMYYYY}":     d.strftime("%d%m%Y"),
        "{YYYYMMDD}":     d.strftime("%Y%m%d"),
        "{DD-MMM-YYYY}":  f"{d.day:02d}-{_MONTH_TITLE[d.month - 1]}-{d.year}",
        "{DD-MON-YYYY}":  f"{d.day:02d}-{_MONTH_UPPER[d.month - 1]}-{d.year}",
    }
    out = spec.url_pattern
    for k, v in repls.items():
        out = out.replace(k, v)
    if "{" in out:
        raise ValueError(
            f"Unresolved placeholder in URL pattern: {out} (key={spec.key})"
        )
    return out


def tier_a_keys() -> list[str]:
    return [k for k, s in REGISTRY.items() if s.tier == "A"]
