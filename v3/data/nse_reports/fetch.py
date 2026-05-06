"""
Top-level fetcher: download all Tier-A reports for a trade-date,
parse each, return {report_key: parsed_object}.

Two modes:
- keep_raw=True, keep_parsed=True (Phase-1 default for the one-day driver):
    Raw bytes cached to v3/cache/nse_reports/raw/{YYYYMMDD}/{key}.{ext}.
    Parsed parquet to v3/cache/nse_reports/parsed/{YYYYMMDD}/{key}.parquet.
- keep_raw=False, keep_parsed=False (Phase-4 history default):
    Raw downloaded into a NamedTemporaryFile, parsed, file deleted in finally.
    Nothing persisted on disk except whatever the caller does with the
    returned object.

Strict mode in both: hard-fail if any registered Tier-A report is
unavailable or fails to parse.

Public:
    fetch_for_date(trade_date, keep_raw=True, keep_parsed=True) -> dict
"""
from __future__ import annotations
import importlib
import logging
import os
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from .errors import NSEReportFetchError, NSEReportParseError
from .registry import REGISTRY, ReportSpec, render_url, tier_a_keys
from .session import client as nse_client

log = logging.getLogger(__name__)

ROOT      = Path(__file__).resolve().parents[3]
RAW_ROOT  = ROOT / "v3" / "cache" / "nse_reports" / "raw"
PRSD_ROOT = ROOT / "v3" / "cache" / "nse_reports" / "parsed"


def _next_nse_trading_day(d: date) -> date:
    """Next NSE trading day strictly after `d`.  Uses pandas_market_calendars 'NSE'.

    Imported lazily so the module remains importable without mcal installed
    in environments that only need the one-day driver.
    """
    import pandas_market_calendars as mcal
    nse = mcal.get_calendar("NSE")
    sched = nse.schedule(
        start_date=str(d + timedelta(days=1)),
        end_date=str(d + timedelta(days=14)),
    )
    if len(sched) == 0:
        raise NSEReportFetchError(
            report_key="__calendar__",
            url="",
            status=None,
            body_excerpt=None,
            reason=f"no NSE trading day within 14 days after {d}",
        )
    return sched.index[0].date()


def _date_for_url(spec: ReportSpec, trade_date: date) -> date:
    """T-1 specs use trade_date; T specs use the *next NSE trading day* after
    trade_date (the sec-ban file is dated for the upcoming session, which is
    not always trade_date + 1 calendar day — could be Mon when trade_date=Fri).
    """
    if spec.target_date == "T":
        return _next_nse_trading_day(trade_date)
    if spec.target_date == "T-1":
        return trade_date
    raise ValueError(f"unknown target_date for spec {spec.key}: {spec.target_date}")


def _raw_path(trade_date: date, key: str, ext: str) -> Path:
    folder = RAW_ROOT / trade_date.strftime("%Y%m%d")
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{key}.{ext}"


def _parsed_path(trade_date: date, key: str, sub: str | None = None) -> Path:
    folder = PRSD_ROOT / trade_date.strftime("%Y%m%d")
    folder.mkdir(parents=True, exist_ok=True)
    name = f"{key}.parquet" if sub is None else f"{key}__{sub}.parquet"
    return folder / name


def _persist_raw(trade_date: date, spec: ReportSpec, body: bytes) -> Path:
    p = _raw_path(trade_date, spec.key, spec.raw_ext)
    p.write_bytes(body)
    return p


def _persist_parsed(trade_date: date, spec: ReportSpec, parsed: Any) -> None:
    if isinstance(parsed, pd.DataFrame):
        parsed.to_parquet(_parsed_path(trade_date, spec.key))
    elif isinstance(parsed, dict):
        for sub, df in parsed.items():
            df.to_parquet(_parsed_path(trade_date, spec.key, sub))


def fetch_one(
    spec: ReportSpec,
    trade_date: date,
    force: bool = False,
    keep_raw: bool = True,
    keep_parsed: bool = True,
) -> Any:
    """Fetch + parse one report.  Returns the parsed object.

    keep_raw=False: write to a tempfile, parse, unlink in finally.
    keep_parsed=False: skip parquet persistence.
    """
    url_date = _date_for_url(spec, trade_date)
    url      = render_url(spec, url_date)

    if keep_raw:
        raw_p = _raw_path(trade_date, spec.key, spec.raw_ext)
        if force or not raw_p.exists() or raw_p.stat().st_size == 0:
            log.info("nse_fetch.start",
                     extra={"key": spec.key, "trade_date": str(trade_date),
                            "url": url})
            body = nse_client().get_bytes(url, report_key=spec.key)
            raw_p.write_bytes(body)
            log.info("nse_fetch.cached",
                     extra={"key": spec.key, "size": len(body),
                            "path": str(raw_p)})
        else:
            log.info("nse_fetch.cache_hit",
                     extra={"key": spec.key, "path": str(raw_p)})
        parser = importlib.import_module(spec.parser_module)
        parsed = parser.parse(str(raw_p), trade_date)
    else:
        # in-memory mode: download to a NamedTemporaryFile, parse, unlink.
        log.info("nse_fetch.start",
                 extra={"key": spec.key, "trade_date": str(trade_date),
                        "url": url, "mode": "in-memory"})
        body = nse_client().get_bytes(url, report_key=spec.key)
        # parsers consume a path; some (xls/xlsx/zip) need real files,
        # not BytesIO with seek limitations.
        tf = tempfile.NamedTemporaryFile(
            suffix=f".{spec.raw_ext}", delete=False
        )
        try:
            tf.write(body)
            tf.flush()
            tf.close()
            parser = importlib.import_module(spec.parser_module)
            parsed = parser.parse(tf.name, trade_date)
        finally:
            try:
                os.unlink(tf.name)
            except FileNotFoundError:
                pass

    if keep_parsed:
        _persist_parsed(trade_date, spec, parsed)

    if not isinstance(parsed, (pd.DataFrame, dict)):
        raise NSEReportParseError(
            report_key=spec.key,
            path="<in-memory>" if not keep_raw else str(raw_p),
            reason="parser returned unexpected type",
            diagnostic={"type": type(parsed).__name__},
        )
    return parsed


def fetch_for_date(
    trade_date: date,
    keys: list[str] | None = None,
    force: bool = False,
    keep_raw: bool = True,
    keep_parsed: bool = True,
) -> Dict[str, Any]:
    """Fetch every Tier-A report (or specified subset). Hard-fails on any error.

    For Phase-4 history runs, callers pass keep_raw=False, keep_parsed=False.
    """
    if keys is None:
        keys = tier_a_keys()

    out: Dict[str, Any] = {}
    parse_failed: list[tuple[str, Exception]] = []
    fetch_skipped: list[tuple[str, str]] = []
    for key in keys:
        spec = REGISTRY[key]
        try:
            out[key] = fetch_one(
                spec, trade_date,
                force=force, keep_raw=keep_raw, keep_parsed=keep_parsed,
            )
        except NSEReportFetchError as e:
            # User-explicit policy: any fetch-level failure is logged and
            # skipped, NOT fatal.  Parse failures remain strict (below).
            log.warning(
                "nse_fetch.skipped",
                extra={"key": key, "trade_date": str(trade_date),
                       "url": e.url, "status": e.status, "reason": e.reason},
            )
            fetch_skipped.append(
                (key, f"http_{e.status}" if e.status else f"err:{e.reason}")
            )
        except NSEReportParseError as e:
            # Parse errors indicate a real code/schema mismatch — surface them.
            log.error("nse_parse.failed", extra={"key": key, "err": repr(e)})
            parse_failed.append((key, e))

    if parse_failed:
        first_key, first_err = parse_failed[0]
        if len(parse_failed) > 1 and hasattr(first_err, "add_note"):
            first_err.add_note(
                "Also parse-failed: " + ", ".join(
                    f"{k}({type(e).__name__})" for k, e in parse_failed[1:]
                )
            )
        raise first_err

    if fetch_skipped:
        # Sentinel so build_daily_intel records what was missing for the date.
        out["__skipped_fetch__"] = [
            {"key": k, "reason": r} for k, r in fetch_skipped
        ]
    return out
