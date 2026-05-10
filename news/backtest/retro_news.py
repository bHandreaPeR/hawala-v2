"""Retroactive news lookup for historical velocity events.

Limitation: live RSS feeds don't carry archives. We hit Google News RSS with
date-bounded queries (`after:YYYY-MM-DD before:YYYY-MM-DD`) which works for
dates within Google News' search index (~12 months reliably).

For each event:
  - 1-hour window: [event_ts - 60min, event_ts + 30min] (news leads or lags)
  - Query Google News RSS with date-bounded "nifty OR sensex OR india OR <theme>" queries
  - Score returned headlines through news.scorer
  - Report: event_dir vs top news direction, score, lag (seconds from pub→event)
"""
from __future__ import annotations

import argparse
import csv
import html as _html_lib
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from news.scorer import score_headline  # noqa

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc

HDR = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}

ITEM_RE  = re.compile(r"<item[^>]*>(.*?)</item>", re.DOTALL | re.I)
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.DOTALL | re.I)
PUB_RE   = re.compile(r"<pubDate[^>]*>(.*?)</pubDate>", re.DOTALL | re.I)
LINK_RE  = re.compile(r"<link[^>]*>(.*?)</link>", re.DOTALL | re.I)


# Themed queries matching our event taxonomy. Each query string is a Google
# News query — we add `after:` and `before:` date bounds.
THEME_QUERIES = [
    "india nifty OR sensex",
    "rbi rate OR mpc OR repo",
    "iran OR israel OR middle east OR ceasefire",
    "trump tariff OR trade deal OR china",
    "fed rate cut OR rate hike OR fomc",
    "oil OR crude OR opec OR brent",
    "fii OR fpi OR foreign inflow OR foreign outflow",
]


def _gn_search(query: str, after: str, before: str) -> list[dict]:
    """Google News RSS search with date bounds. Returns list of {headline, url, ts_pub}."""
    q = f"{query} after:{after} before:{before}"
    url = f"https://news.google.com/rss/search?q={requests.utils.quote(q)}&hl=en-IN&gl=IN&ceid=IN:en"
    out = []
    try:
        r = requests.get(url, timeout=8, headers=HDR)
        if not r.ok:
            return out
        for blk in ITEM_RE.findall(r.text)[:25]:
            tm = TITLE_RE.search(blk)
            pm = PUB_RE.search(blk)
            lm = LINK_RE.search(blk)
            if not tm:
                continue
            t = _html_lib.unescape(tm.group(1)).strip()
            t = re.sub(r"^<!\[CDATA\[(.*)\]\]>$", r"\1", t, flags=re.DOTALL).strip()
            t = re.sub(r"\s*[-|]\s*[A-Z][^-|]{3,40}$", "", t).strip()
            if len(t) < 20:
                continue
            ts_pub = None
            if pm:
                try:
                    pd_ = parsedate_to_datetime(_html_lib.unescape(pm.group(1)).strip())
                    if pd_.tzinfo is None:
                        pd_ = pd_.replace(tzinfo=UTC)
                    ts_pub = pd_
                except Exception:
                    pass
            out.append({
                "headline": t,
                "url":      lm.group(1).strip() if lm else "",
                "ts_pub":   ts_pub,
                "query":    query,
            })
    except Exception:
        pass
    return out


def lookup_event(ts_event_ist: datetime, window_min: int = 60) -> list[dict]:
    """Return news items published within ±window_min of event timestamp,
    scored, sorted by abs(score)."""
    after  = (ts_event_ist - timedelta(days=1)).strftime("%Y-%m-%d")
    before = (ts_event_ist + timedelta(days=1)).strftime("%Y-%m-%d")
    win_start = ts_event_ist - timedelta(minutes=window_min)
    win_end   = ts_event_ist + timedelta(minutes=window_min)

    found: list[dict] = []
    seen = set()
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(_gn_search, q, after, before) for q in THEME_QUERIES]
        for fut in as_completed(futs, timeout=20):
            try:
                items = fut.result()
            except Exception:
                continue
            for it in items:
                if it["headline"] in seen:
                    continue
                seen.add(it["headline"])
                found.append(it)

    # Filter to window and score
    scored: list[dict] = []
    for it in found:
        ts_pub = it.get("ts_pub")
        if ts_pub is None:
            continue
        ts_pub_ist = ts_pub.astimezone(IST)
        if not (win_start <= ts_pub_ist <= win_end):
            continue
        # Approximate tier for Google News: 0.7 (mainstream coverage).
        # For backtest we want the score AS IF the headline just arrived live —
        # otherwise recency_decay would zero out anything more than a few hours
        # old. Pass `ts_seen=now` so decay=1.0; lag is computed separately.
        s = score_headline(it["headline"], "GoogleNews", 0.7, datetime.now(IST))
        s["ts_pub"] = ts_pub_ist.isoformat()
        s["query"]  = it.get("query")
        s["lag_sec"] = (ts_event_ist - ts_pub_ist).total_seconds()
        scored.append(s)

    scored.sort(key=lambda x: (abs(x.get("score", 0)) > 0, abs(x.get("score", 0))), reverse=True)
    return scored


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--in",  dest="in_csv",  default="news/backtest/velocity_events.csv")
    p.add_argument("--out", dest="out_csv", default="news/backtest/event_news_correlation.csv")
    p.add_argument("--max", type=int, default=40)
    p.add_argument("--window-min", type=int, default=60)
    p.add_argument("--sleep", type=float, default=0.5)
    args = p.parse_args()

    df = pd.read_csv(ROOT / args.in_csv)
    df["ts"] = pd.to_datetime(df["ts"], utc=False)
    if df["ts"].dt.tz is None:
        df["ts"] = df["ts"].dt.tz_localize(IST)
    # Dedupe identical events that hit both indices same minute (keep the one with higher ret_bps)
    df = df.sort_values("ret_bps", ascending=False).drop_duplicates(subset=["ts"], keep="first")
    df = df.sort_values("ts").reset_index(drop=True)
    print(f"Looking up {min(args.max, len(df))} velocity events…")

    out_rows = []
    for i, row in df.head(args.max).iterrows():
        ts = row["ts"].to_pydatetime()
        items = lookup_event(ts, window_min=args.window_min)
        scored_items = [x for x in items if x.get("event_class")]
        top = scored_items[0] if scored_items else None
        sign_event = +1 if row["dir"] == "+" else -1
        sign_news  = (top["direction"] if top else 0)
        match = "MATCH" if top and sign_event == sign_news else (
                "MISMATCH" if top and sign_event != sign_news else "NO_NEWS")

        out_rows.append({
            "event_ts":      ts.strftime("%Y-%m-%d %H:%M"),
            "instrument":    row["instrument"],
            "ret_bps":       round(float(row["ret_bps"]), 1),
            "dir":           row["dir"],
            "atr_ratio":     round(float(row["atr_ratio"]), 1),
            "n_news_in_win": len(items),
            "n_scored":      len(scored_items),
            "top_event_class": top["event_class"] if top else "",
            "top_score":       round(top["score"], 2) if top else 0.0,
            "top_headline":  top["headline"][:100] if top else "",
            "top_lag_sec":   int(top["lag_sec"]) if top else "",
            "match":         match,
        })
        print(f"  [{i+1:>2}/{args.max}] {ts.strftime('%Y-%m-%d %H:%M')} {row['instrument']:<9} "
              f"{row['ret_bps']:>5.1f}bps {row['dir']}  "
              f"news={len(items):>2} scored={len(scored_items):>2}  {match}  "
              f"{(top['headline'][:60] if top else '')}")
        time.sleep(args.sleep)  # be polite to Google

    out = ROOT / args.out_csv
    pd.DataFrame(out_rows).to_csv(out, index=False)
    print(f"\nWrote → {out}")

    # Summary
    df_out = pd.DataFrame(out_rows)
    n = len(df_out)
    matched = (df_out["match"] == "MATCH").sum()
    mismatch = (df_out["match"] == "MISMATCH").sum()
    none = (df_out["match"] == "NO_NEWS").sum()
    print(f"\nSummary over {n} events:")
    print(f"  MATCH   : {matched:>3}  ({100*matched/n:.0f}%) — direction agrees")
    print(f"  MISMATCH: {mismatch:>3}  ({100*mismatch/n:.0f}%) — direction conflicts")
    print(f"  NO_NEWS : {none:>3}  ({100*none/n:.0f}%) — no scored news within window")
    if matched + mismatch > 0:
        avg_lag = df_out[df_out["top_lag_sec"] != ""]["top_lag_sec"].astype(float).abs().mean()
        print(f"  median |lag| (sec): ~{avg_lag:.0f}")


if __name__ == "__main__":
    main()
