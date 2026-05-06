"""
NSE-aware requests.Session.

Two distinct hosts with different access policies:
- nsearchives.nseindia.com  : public archives, no cookies/warm-up needed
                              (CSV/ZIP/XLS files for daily reports).
- www.nseindia.com/api/*    : Cloudflare-fronted, needs cookie warm-up.

This module focuses on archive fetches — `get_bytes()` hits archives
directly with UA + Referer.  `get_json()` does an opt-in homepage GET first
because the API path is the one that gets gated.

Per project rules:
- Hard-fail on non-2xx with URL+status+body in the error.
- Bounded retries on transient (>=500 / connection error / timeout) only,
  with exponential backoff. Final failure raises the last error.
- No silent fallback; no synthesized data.
"""
from __future__ import annotations
import json
import logging
import time
from typing import Optional

import requests

from .errors import NSEReportFetchError

log = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)
_REFERER = "https://www.nseindia.com/all-reports-derivatives"
_HOMEPAGE = "https://www.nseindia.com/"
_API_HOME = "https://www.nseindia.com/all-reports-derivatives"


class NSEClient:
    """Thread-unsafe singleton-style wrapper around requests.Session."""

    def __init__(self, timeout: float = 30.0, polite_delay: float = 0.6):
        self.timeout = timeout
        self.polite_delay = polite_delay
        self._sess: Optional[requests.Session] = None
        self._warmed: bool = False
        self._last_request_ts: float = 0.0

    # ------------------------------------------------------------------
    # session lifecycle
    # ------------------------------------------------------------------
    def _build_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update(
            {
                "User-Agent": _UA,
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
        )
        return s

    def _session(self) -> requests.Session:
        if self._sess is None:
            self._sess = self._build_session()
        return self._sess

    def _warm_for_api(self) -> None:
        """Cookie warm-up required only for www.nseindia.com/api/* endpoints.

        Idempotent: subsequent calls are no-ops.  Each warm GET is a hard
        prerequisite — failure raises NSEReportFetchError with the body.
        """
        if self._warmed:
            return
        s = self._session()
        for warm in (_HOMEPAGE, _API_HOME):
            r = s.get(warm, timeout=self.timeout)
            if r.status_code != 200:
                raise NSEReportFetchError(
                    report_key="__warmup__",
                    url=warm,
                    status=r.status_code,
                    body_excerpt=r.content[:500],
                    reason="api cookie warm-up failed",
                )
        log.info(
            "nse_session.warmed",
            extra={"cookies": len(s.cookies)},
        )
        self._warmed = True

    def _polite_wait(self) -> None:
        delta = time.time() - self._last_request_ts
        if delta < self.polite_delay:
            time.sleep(self.polite_delay - delta)
        self._last_request_ts = time.time()

    # ------------------------------------------------------------------
    # public fetchers
    # ------------------------------------------------------------------
    def get_bytes(
        self,
        url: str,
        report_key: str,
        accept: Optional[str] = None,
        max_retries: int = 3,
        backoff: float = 2.0,
    ) -> bytes:
        sess = self._session()
        headers = {"Referer": _REFERER}
        if accept:
            headers["Accept"] = accept

        last_err: Optional[Exception] = None
        for attempt in range(max_retries):
            self._polite_wait()
            try:
                r = sess.get(url, headers=headers, timeout=self.timeout)
            except (requests.ConnectionError, requests.Timeout) as e:
                last_err = e
                log.warning(
                    "nse_fetch.transient",
                    extra={
                        "key": report_key,
                        "attempt": attempt + 1,
                        "url": url,
                        "err": repr(e),
                    },
                )
                time.sleep(backoff * (attempt + 1))
                continue

            if r.status_code >= 500:
                last_err = NSEReportFetchError(
                    report_key=report_key,
                    url=url,
                    status=r.status_code,
                    body_excerpt=r.content,
                    reason="server 5xx",
                )
                time.sleep(backoff * (attempt + 1))
                continue

            if r.status_code != 200:
                # 4xx: do not retry, raise immediately with full context.
                raise NSEReportFetchError(
                    report_key=report_key,
                    url=url,
                    status=r.status_code,
                    body_excerpt=r.content,
                    reason="non-200",
                )

            if not r.content:
                raise NSEReportFetchError(
                    report_key=report_key,
                    url=url,
                    status=200,
                    body_excerpt=b"",
                    reason="empty body",
                )

            return r.content

        # exhausted retries
        if last_err is not None:
            raise last_err
        raise NSEReportFetchError(
            report_key=report_key,
            url=url,
            status=None,
            body_excerpt=None,
            reason="exhausted retries with no captured error",
        )

    def get_json(self, url: str, report_key: str) -> dict:
        # API endpoints under www.nseindia.com need the cookie warm-up first.
        if url.startswith("https://www.nseindia.com/api/"):
            self._warm_for_api()
        body = self.get_bytes(url, report_key=report_key, accept="application/json")
        try:
            return json.loads(body)
        except json.JSONDecodeError as e:
            raise NSEReportFetchError(
                report_key=report_key,
                url=url,
                status=200,
                body_excerpt=body[:500],
                reason=f"json decode failed: {e}",
            )


# module-level convenience singleton
_client: Optional[NSEClient] = None


def client() -> NSEClient:
    global _client
    if _client is None:
        _client = NSEClient()
    return _client
