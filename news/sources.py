"""Curated news feeds with source-tier weighting.

Tier weights influence per-headline confidence and final score.
- Tier-1 (1.0): wire services + agenda-setters
- Tier-2 (0.7): mainstream business press
- Tier-3 (0.4): topical Google News queries (broad coverage, more noise)
- Direct  (1.0): primary government / central-bank pages
"""
from __future__ import annotations


FEEDS: list[dict] = [
    # ── Tier-1 — agenda-setters ───────────────────────────────────────────────
    {"name": "Axios",       "tier": 1.0, "type": "rss",
     "url":  "https://api.axios.com/feed/"},
    {"name": "Reuters",     "tier": 1.0, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:2h+reuters+markets+OR+economy+OR+geopolitical&hl=en&gl=US&ceid=US:en"},
    {"name": "Bloomberg",   "tier": 1.0, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:2h+bloomberg+markets+OR+fed+OR+economy&hl=en&gl=US&ceid=US:en"},
    {"name": "FT",          "tier": 1.0, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:2h+%22financial+times%22+markets&hl=en&gl=US&ceid=US:en"},
    {"name": "WSJ",         "tier": 1.0, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:2h+%22wall+street+journal%22+markets+OR+fed&hl=en&gl=US&ceid=US:en"},
    {"name": "Moneycontrol", "tier": 1.0, "type": "rss",
     "url":  "https://www.moneycontrol.com/rss/latestnews.xml"},

    # ── Tier-2 — mainstream business press ────────────────────────────────────
    {"name": "CNBC",            "tier": 0.7, "type": "rss",
     "url":  "https://www.cnbc.com/id/10000664/device/rss/rss.html"},
    {"name": "BBC Business",    "tier": 0.7, "type": "rss",
     "url":  "https://feeds.bbci.co.uk/news/business/rss.xml"},
    {"name": "NY Times",        "tier": 0.7, "type": "rss",
     "url":  "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml"},
    {"name": "Economic Times",  "tier": 0.7, "type": "rss",
     "url":  "https://economictimes.indiatimes.com/rssfeedstopstories.cms"},
    {"name": "Livemint",        "tier": 0.7, "type": "rss",
     "url":  "https://www.livemint.com/rss/markets"},
    {"name": "NDTV Profit",     "tier": 0.7, "type": "rss",
     "url":  "https://www.ndtvprofit.com/feed"},

    # ── Tier-3 — topical Google News queries ─────────────────────────────────
    {"name": "GN: rates",       "tier": 0.4, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:1h+federal+reserve+OR+rbi+OR+rate+cut+OR+rate+hike&hl=en&gl=US&ceid=US:en"},
    {"name": "GN: geopolitics", "tier": 0.4, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:1h+iran+OR+israel+OR+russia+ukraine+OR+china+taiwan+OR+ceasefire&hl=en&gl=US&ceid=US:en"},
    {"name": "GN: oil",         "tier": 0.4, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:1h+oil+OR+crude+OR+opec+OR+brent&hl=en&gl=US&ceid=US:en"},
    {"name": "GN: india",       "tier": 0.4, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:1h+india+nifty+OR+sensex+OR+rbi+OR+sebi&hl=en-IN&gl=IN&ceid=IN:en"},

    # ── Direct — primary sources ──────────────────────────────────────────────
    {"name": "RBI",  "tier": 1.0, "type": "html",
     "url":  "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"},
    {"name": "Fed press",   "tier": 1.0, "type": "rss",
     "url":  "https://www.federalreserve.gov/feeds/press_all.xml"},

    # ── Fast wire-style (geopolitics + macro) ────────────────────────────────
    {"name": "ForexLive",   "tier": 1.0, "type": "rss",
     "url":  "https://www.forexlive.com/feed/"},
    {"name": "Investing news", "tier": 0.7, "type": "rss",
     "url":  "https://www.investing.com/rss/news.rss"},
    {"name": "Investing econ", "tier": 0.7, "type": "rss",
     "url":  "https://www.investing.com/rss/news_25.rss"},

    # ── Geopolitical fast feeds ──────────────────────────────────────────────
    {"name": "BBC World",   "tier": 0.7, "type": "rss",
     "url":  "https://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name": "Al Jazeera",  "tier": 0.7, "type": "rss",
     "url":  "https://www.aljazeera.com/xml/rss/all.xml"},

    # ── India fast — extra ───────────────────────────────────────────────────
    {"name": "ET Markets",  "tier": 0.7, "type": "rss",
     "url":  "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms"},
    {"name": "Mint money",  "tier": 0.7, "type": "rss",
     "url":  "https://www.livemint.com/rss/money"},

    # ── Google News — last 30m breaking ──────────────────────────────────────
    {"name": "GN: breaking-30m", "tier": 0.4, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:30m+breaking+OR+exclusive&hl=en&gl=US&ceid=US:en"},
    {"name": "GN: india-30m",    "tier": 0.4, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:30m+nifty+OR+sensex+OR+rbi+OR+india&hl=en-IN&gl=IN&ceid=IN:en"},

    # ── Truth Social — Trump posts (often the literal news catalyst) ────────
    # 3rd-party mirror; official Truth Social does not expose RSS. Tier-1 because
    # Trump's posts move oil/equities directly.
    {"name": "Truth Social",     "tier": 1.0, "type": "rss",
     "url":  "https://trumpstruth.org/feed"},

    # ── X/Twitter via Nitter.net — financial wire accounts ──────────────────
    # Note: nitter mirrors are unreliable. We poll one mirror; if it dies the
    # healthcheck makes it visible and we can swap to another instance.
    {"name": "X: DeItaone",      "tier": 0.7, "type": "rss",
     "url":  "https://nitter.net/DeItaone/rss"},
    {"name": "X: FirstSquawk",   "tier": 0.7, "type": "rss",
     "url":  "https://nitter.net/FirstSquawk/rss"},

    # ── Wire-style market commentary (fast forex/macro) ─────────────────────
    {"name": "FXStreet",         "tier": 0.7, "type": "rss",
     "url":  "https://www.fxstreet.com/rss/news"},
    {"name": "MarketWatch top",  "tier": 0.7, "type": "rss",
     "url":  "https://feeds.marketwatch.com/marketwatch/topstories/"},
    {"name": "Yahoo Finance",    "tier": 0.7, "type": "rss",
     "url":  "https://finance.yahoo.com/news/rssindex"},

    # ── NSE corporate filings (JSON API) ────────────────────────────────────
    # Catches results, board changes, regulatory action, AGM notices, etc.
    # before they hit press. Special parser in scraper.py (type="nse_json").
    {"name": "NSE filings",      "tier": 1.0, "type": "nse_json",
     "url":  "https://www.nseindia.com/api/corporate-announcements?index=equities"},

    # ── Bloomberg / Reuters domain-filtered Google News (last 30m) ──────────
    {"name": "GN: bloomberg-30m", "tier": 1.0, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:30m+site:bloomberg.com&hl=en&gl=US&ceid=US:en"},
    {"name": "GN: reuters-30m",   "tier": 1.0, "type": "rss",
     "url":  "https://news.google.com/rss/search?q=when:30m+site:reuters.com&hl=en&gl=US&ceid=US:en"},
]
