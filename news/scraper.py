"""Concurrent feed scraper. RSS + RBI HTML.

Returns a list of dicts:
  {headline, url, source, tier, ts_seen}

Layer-1 dedup (exact-hash) is applied here so we don't re-emit the same
headline on every poll. Cluster assignment happens later in the pipeline.
"""
from __future__ import annotations

import argparse
import html as _html_lib
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Iterable

import requests

from . import dedup
from .dedup import IST
from .sources import FEEDS


_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
_HDR = {"User-Agent": _UA}

# NSE corporate-filings API needs cookies + Referer to return JSON instead of
# the bot-block page. We warm the session with a homepage GET before each fetch.
_NSE_HDR = {
    **_HDR,
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "en-US,en;q=0.9",
    "Referer":          "https://www.nseindia.com/companies-listing/corporate-filings-announcements",
}
_nse_session_cache = {"sess": None, "ts": 0.0}

_ITEM_RE  = re.compile(r"<item[^>]*>(.*?)</item>", re.DOTALL | re.IGNORECASE)
_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.DOTALL | re.IGNORECASE)
_LINK_RE  = re.compile(r"<link[^>]*>(.*?)</link>", re.DOTALL | re.IGNORECASE)
_CDATA_RE = re.compile(r"^<!\[CDATA\[(.*)\]\]>$", re.DOTALL)
_GN_TAIL  = re.compile(r"\s*[-|]\s*[A-Z][^-|]{3,40}$")


def _clean_title(raw: str, is_google_news: bool) -> str:
    s = _html_lib.unescape(raw).strip()
    s = _CDATA_RE.sub(r"\1", s).strip()
    if is_google_news:
        s = _GN_TAIL.sub("", s).strip()
    return s


def _parse_rss(xml: str, source: str, tier: float, url: str) -> list[dict]:
    is_gn = "news.google.com" in url
    items = _ITEM_RE.findall(xml)
    now = datetime.now(IST)
    out: list[dict] = []
    for block in items[:25]:
        m = _TITLE_RE.search(block)
        if not m:
            continue
        title = _clean_title(m.group(1), is_gn)
        if not title or len(title) < 20:
            continue
        # Skip generic feed boilerplate
        if any(skip in title.lower() for skip in ["google news", "rss feed", "view this email"]):
            continue
        link_m = _LINK_RE.search(block)
        link = ""
        if link_m:
            link = _html_lib.unescape(link_m.group(1)).strip()
        out.append({
            "headline": title,
            "url":      link,
            "source":   source,
            "tier":     float(tier),
            "ts_seen":  now.isoformat(),
        })
    return out


# RBI press release page is plain HTML, not RSS.
_RBI_ROW_RE = re.compile(
    r'<a[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<text>[^<]{20,})</a>',
    re.IGNORECASE,
)


def _parse_rbi(html: str, source: str, tier: float) -> list[dict]:
    now = datetime.now(IST)
    out: list[dict] = []
    seen = set()
    for m in _RBI_ROW_RE.finditer(html):
        text = _html_lib.unescape(m.group("text")).strip()
        text = re.sub(r"\s+", " ", text)
        if not text or text.lower() in seen:
            continue
        # Filter to "press release" style entries — must contain meaningful keywords
        if not any(k in text.lower() for k in [
            "rbi", "repo", "monetary", "policy", "inflation", "rate",
            "press release", "announcement", "circular", "review"
        ]):
            continue
        seen.add(text.lower())
        href = m.group("href")
        if href.startswith("/"):
            href = "https://www.rbi.org.in" + href
        out.append({
            "headline": text,
            "url":      href,
            "source":   source,
            "tier":     float(tier),
            "ts_seen":  now.isoformat(),
        })
        if len(out) >= 15:
            break
    return out


def _nse_session() -> requests.Session:
    """Get (and cache) an NSE session with warmed cookies."""
    import time
    s = _nse_session_cache.get("sess")
    age = time.monotonic() - _nse_session_cache.get("ts", 0.0)
    if s is None or age > 600:  # refresh every 10 min
        s = requests.Session()
        s.headers.update(_NSE_HDR)
        try:
            s.get("https://www.nseindia.com/", timeout=8)
        except Exception:
            pass
        _nse_session_cache["sess"] = s
        _nse_session_cache["ts"]   = time.monotonic()
    return s


def _parse_nse_json(text: str, source: str, tier: float) -> list[dict]:
    """Convert NSE corporate-announcements JSON into our scraper item format.
    NSE row keys: symbol, desc, sm_name, sort_date, attchmntText.
    Headline = `<sm_name>: <desc>` so the keyword classifier sees the company name.
    """
    import json as _json
    now = datetime.now(IST)
    out: list[dict] = []
    try:
        data = _json.loads(text)
    except Exception:
        return out
    if not isinstance(data, list):
        return out
    for row in data[:25]:
        company = (row.get("sm_name") or row.get("symbol") or "").strip()
        desc    = (row.get("desc") or "").strip()
        extra   = (row.get("attchmntText") or "").strip()
        if not company or not desc:
            continue
        # Build a single sentence headline; include extra description if short
        head = f"{company}: {desc}"
        if extra and len(extra) < 120:
            head = f"{head} — {extra}"
        if len(head) < 20:
            continue
        out.append({
            "headline": head,
            "url":      row.get("attchmntFile", ""),
            "source":   source,
            "tier":     float(tier),
            "ts_seen":  now.isoformat(),
        })
    return out


def _fetch_one(feed: dict) -> tuple[str, list[dict], int]:
    """Returns (source_name, items, http_status). status=0 on exception."""
    try:
        if feed["type"] == "nse_json":
            sess = _nse_session()
            r = sess.get(feed["url"], timeout=10)
            if not r.ok:
                return feed["name"], [], r.status_code
            items = _parse_nse_json(r.text, feed["name"], feed["tier"])
            return feed["name"], items, r.status_code
        r = requests.get(feed["url"], timeout=8, headers=_HDR)
        if not r.ok:
            return feed["name"], [], r.status_code
        if feed["type"] == "rss":
            items = _parse_rss(r.text, feed["name"], feed["tier"], feed["url"])
        elif feed["type"] == "html" and feed["name"] == "RBI":
            items = _parse_rbi(r.text, feed["name"], feed["tier"])
        else:
            items = []
        return feed["name"], items, r.status_code
    except Exception as e:
        return feed["name"], [], 0


def fetch_all(feeds: Iterable[dict] = None, *, dedup_layer1: bool = True,
              max_workers: int = 8) -> list[dict]:
    """Fetch all configured feeds concurrently. Returns NEW items only when
    `dedup_layer1` is True (Layer-1 exact-hash dedup applied).
    """
    feeds = list(feeds) if feeds is not None else FEEDS
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_fetch_one, f) for f in feeds]
        for fut in as_completed(futs, timeout=15):
            try:
                _name, items, _status = fut.result()
            except Exception:
                continue
            results.extend(items)

    # Layer-1: exact-hash dedup
    if dedup_layer1:
        new_items: list[dict] = []
        for it in results:
            if dedup.already_seen(it["headline"]):
                continue
            dedup.mark_seen(it["headline"])
            new_items.append(it)
        return new_items
    return results


def healthcheck() -> None:
    """Print HTTP status per feed."""
    print(f"Healthcheck — {len(FEEDS)} feeds")
    print("-" * 60)
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_fetch_one, f): f for f in FEEDS}
        for fut in as_completed(futs, timeout=15):
            f = futs[fut]
            try:
                name, items, status = fut.result()
            except Exception as e:
                name, items, status = f["name"], [], 0
                print(f"  ✗ {f['name']:<22} ERR  {e}")
                continue
            ok = "✓" if status == 200 and items else ("△" if status == 200 else "✗")
            print(f"  {ok} {name:<22} HTTP {status:>3}  items={len(items):>3}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--healthcheck", action="store_true")
    p.add_argument("--dump", action="store_true",
                   help="Print all fetched headlines (no dedup)")
    args = p.parse_args()

    if args.healthcheck:
        healthcheck()
    elif args.dump:
        items = fetch_all(dedup_layer1=False)
        print(f"Fetched {len(items)} headlines")
        for it in items[:50]:
            print(f"  [{it['source']:<14}] {it['headline'][:90]}")
    else:
        items = fetch_all()
        print(f"New (post Layer-1 dedup): {len(items)}")
        for it in items[:30]:
            print(f"  [{it['source']:<14}] {it['headline'][:90]}")
