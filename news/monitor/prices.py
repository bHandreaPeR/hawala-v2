"""5-minute price monitor for the assets that anchor our news classifier.

Polls a fixed asset list via yfinance, appends a row to
news/monitor/data/prices_5m.csv, and rotates entries older than 7 days.

Tickers chosen for the news → price correlation:

  Energy:    WTI (CL=F), Brent (BZ=F)
  Metals:    Gold (GC=F), Silver (SI=F)
  Crypto:    BTC (BTC-USD), ETH (ETH-USD)
  FX:        USD/INR (INR=X), DXY (DX-Y.NYB)
  Rates:     US 10Y (^TNX)
  Equities:  SPX e-mini (ES=F), Nifty fut (^NSEI proxy), BankNifty (^NSEBANK)
  Vol:       India VIX (^INDIAVIX)

Each row:  ts, ticker, price, change_pct_5m, change_pct_30m

Usage:
    python -m news.monitor.prices --once
    python -m news.monitor.prices       # daemon: poll every 5 min
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import signal as _signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yfinance as yf  # type: ignore

ROOT = Path(__file__).resolve().parent.parent.parent
IST  = timezone(timedelta(hours=5, minutes=30))

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
PRICES_CSV = DATA_DIR / "prices_5m.csv"
LOG_PATH   = Path(__file__).parent.parent / "monitor.log"
PID_PATH   = Path(__file__).parent.parent / "monitor.pid"

# Comprehensive set per scope decision
TICKERS: list[dict] = [
    # Energy
    {"ticker": "CL=F",        "asset_class": "oil",     "label": "WTI"},
    {"ticker": "BZ=F",        "asset_class": "oil",     "label": "Brent"},
    # Metals
    {"ticker": "GC=F",        "asset_class": "gold",    "label": "Gold"},
    {"ticker": "SI=F",        "asset_class": "silver",  "label": "Silver"},
    # Crypto
    {"ticker": "BTC-USD",     "asset_class": "crypto",  "label": "BTC"},
    {"ticker": "ETH-USD",     "asset_class": "crypto",  "label": "ETH"},
    # FX
    {"ticker": "INR=X",       "asset_class": "fx",      "label": "USDINR"},
    {"ticker": "DX-Y.NYB",    "asset_class": "fx",      "label": "DXY"},
    # Rates
    {"ticker": "^TNX",        "asset_class": "rates",   "label": "US10Y"},
    # Equities
    {"ticker": "ES=F",        "asset_class": "equities","label": "SPX-mini"},
    {"ticker": "^NSEI",       "asset_class": "nifty",   "label": "Nifty"},
    {"ticker": "^NSEBANK",    "asset_class": "nifty",   "label": "BankNifty"},
    # Vol
    {"ticker": "^INDIAVIX",   "asset_class": "vix",     "label": "IndiaVIX"},
]

CYCLE_SEC      = int(os.environ.get("PRICE_MONITOR_CYCLE_SEC", "300"))
PRUNE_DAYS     = int(os.environ.get("PRICE_PRUNE_DAYS", "7"))

log = logging.getLogger("news.monitor.prices")


def _now() -> datetime:
    return datetime.now(IST)


def _setup_logging() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO, format=fmt,
        handlers=[
            logging.FileHandler(LOG_PATH),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _fetch_one(ticker: str) -> float | None:
    """Return the most recent traded price, or None if unavailable.
    Uses fast_info when available; falls back to 1m history."""
    try:
        t = yf.Ticker(ticker)
        # fast_info is much faster than .info but not always populated
        try:
            p = t.fast_info.get("last_price") if hasattr(t, "fast_info") else None
            if p:
                return float(p)
        except Exception:
            pass
        h = t.history(period="1d", interval="1m")
        if h.empty:
            return None
        return float(h["Close"].iloc[-1])
    except Exception as e:
        log.warning("fetch %s failed: %s", ticker, e)
        return None


def _load_recent(minutes: int = 60) -> dict[str, list[tuple[datetime, float]]]:
    """Load last `minutes` of price history from CSV grouped by ticker."""
    out: dict[str, list[tuple[datetime, float]]] = {}
    if not PRICES_CSV.exists():
        return out
    cutoff = _now() - timedelta(minutes=minutes)
    try:
        with open(PRICES_CSV) as f:
            r = csv.DictReader(f)
            for row in r:
                try:
                    ts = datetime.fromisoformat(row["ts"])
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=IST)
                except Exception:
                    continue
                if ts < cutoff:
                    continue
                tk = row.get("ticker", "")
                px = float(row.get("price", 0) or 0)
                if px <= 0:
                    continue
                out.setdefault(tk, []).append((ts, px))
    except Exception as e:
        log.warning("could not parse %s: %s", PRICES_CSV, e)
    for tk in out:
        out[tk].sort(key=lambda x: x[0])
    return out


def _pct_change(history: list[tuple[datetime, float]], current_px: float,
                minutes_back: int) -> float | None:
    """Find the price closest to `minutes_back` min ago and return pct change."""
    if not history:
        return None
    target = _now() - timedelta(minutes=minutes_back)
    # Find closest entry to `target`
    best = min(history, key=lambda x: abs((x[0] - target).total_seconds()))
    if abs((best[0] - target).total_seconds()) > minutes_back * 60 + 60:
        return None
    if best[1] == 0:
        return None
    return (current_px - best[1]) / best[1] * 100.0


def _append_row(row: dict) -> None:
    new_file = not PRICES_CSV.exists()
    with open(PRICES_CSV, "a", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["ts","ticker","label","asset_class","price",
                        "chg_pct_5m","chg_pct_30m"])
        w.writerow([
            row["ts"], row["ticker"], row["label"], row["asset_class"],
            f"{row['price']:.6f}",
            ("" if row["chg_pct_5m"]  is None else f"{row['chg_pct_5m']:+.4f}"),
            ("" if row["chg_pct_30m"] is None else f"{row['chg_pct_30m']:+.4f}"),
        ])


def _prune_csv() -> None:
    """Drop rows older than PRUNE_DAYS days."""
    if not PRICES_CSV.exists():
        return
    cutoff = _now() - timedelta(days=PRUNE_DAYS)
    keep: list[list[str]] = []
    header: list[str] | None = None
    try:
        with open(PRICES_CSV) as f:
            r = csv.reader(f)
            header = next(r, None)
            for row in r:
                if not row:
                    continue
                try:
                    ts = datetime.fromisoformat(row[0])
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=IST)
                except Exception:
                    continue
                if ts >= cutoff:
                    keep.append(row)
    except Exception as e:
        log.warning("prune read failed: %s", e); return
    if header is None:
        return
    tmp = PRICES_CSV.with_suffix(".tmp")
    with open(tmp, "w", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(keep)
    tmp.replace(PRICES_CSV)


def cycle_once() -> dict[str, dict]:
    """Single price poll. Returns {ticker: row_dict}."""
    t0 = time.monotonic()
    history = _load_recent(minutes=60)
    out: dict[str, dict] = {}
    now_iso = _now().isoformat()

    n_ok = 0
    for spec in TICKERS:
        px = _fetch_one(spec["ticker"])
        if px is None:
            continue
        n_ok += 1
        h = history.get(spec["ticker"], [])
        chg5  = _pct_change(h, px, 5)
        chg30 = _pct_change(h, px, 30)
        row = {
            "ts":          now_iso,
            "ticker":      spec["ticker"],
            "label":       spec["label"],
            "asset_class": spec["asset_class"],
            "price":       px,
            "chg_pct_5m":  chg5,
            "chg_pct_30m": chg30,
        }
        _append_row(row)
        out[spec["ticker"]] = row

    elapsed = time.monotonic() - t0
    log.info("price-cycle: %d/%d tickers ok in %.1fs", n_ok, len(TICKERS), elapsed)
    return out


_running = True


def _handle_sigterm(_signum, _frame):
    global _running
    log.info("Received signal — shutting down after current cycle")
    _running = False


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--once", action="store_true")
    args = p.parse_args()
    _setup_logging()

    try:
        PID_PATH.write_text(str(os.getpid()))
    except Exception:
        pass
    _signal.signal(_signal.SIGTERM, _handle_sigterm)
    _signal.signal(_signal.SIGINT,  _handle_sigterm)

    if args.once:
        cycle_once()
        return

    log.info("Price monitor starting (cycle=%ds, %d tickers)",
             CYCLE_SEC, len(TICKERS))
    last_prune = _now()
    while _running:
        try:
            cycle_once()
        except Exception as e:
            log.exception("price cycle failed: %s", e)
        # Prune once an hour
        if (_now() - last_prune).total_seconds() > 3600:
            _prune_csv()
            last_prune = _now()
        slept = 0.0
        while _running and slept < CYCLE_SEC:
            time.sleep(min(5.0, CYCLE_SEC - slept))
            slept += 5.0
    log.info("Price monitor stopped.")


if __name__ == "__main__":
    main()
