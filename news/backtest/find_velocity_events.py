"""Identify high-velocity 1m moves in NIFTY/BANKNIFTY since Sept 2025.

Velocity = abs(1m return) in basis points. We want the largest "shock" bars
that look like news-driven moves (not normal noise). To avoid open/close
hour artifacts:
  - skip first 5 bars (09:15–09:20) — opening auction noise
  - skip last 5 bars  (15:25–15:30) — closing auction
  - require minimum surrounding volatility ratio (move_now / atr_5m_prior > 3)

Outputs CSV with: ts_ist, instrument, return_bps, atr_ratio, dir
"""
from __future__ import annotations

import pickle
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
IST  = timezone(timedelta(hours=5, minutes=30))


def _load(path: str) -> pd.DataFrame:
    df: pd.DataFrame = pickle.load(open(ROOT / path, "rb"))
    df = df.copy()
    df["ts"] = pd.to_datetime(df["ts"])
    if df["ts"].dt.tz is None:
        df["ts"] = df["ts"].dt.tz_localize(IST)
    return df.sort_values("ts").reset_index(drop=True)


def _events(df: pd.DataFrame, instrument: str, top_n: int = 50,
            min_atr_ratio: float = 3.0) -> pd.DataFrame:
    df = df.copy()
    df["ret"]      = df["close"].pct_change()
    df["ret_bps"]  = df["ret"].abs() * 10_000
    # 5-min trailing absolute-return mean (proxy for short-window ATR)
    df["atr_5"]    = df["ret"].abs().rolling(5, min_periods=3).mean()
    df["atr_ratio"] = df["ret"].abs() / df["atr_5"].shift(1)

    # Skip opening 5 and closing 5 bars per day
    df["t"] = df["ts"].dt.time
    skip = (df["t"] <= time(9, 19)) | (df["t"] >= time(15, 25))
    df = df[~skip]

    # Require atr_ratio threshold
    df = df[df["atr_ratio"] >= min_atr_ratio]

    df = df.assign(instrument=instrument,
                   dir=np.where(df["ret"] > 0, "+", "-"))
    df = df[["ts", "instrument", "ret_bps", "atr_ratio", "dir", "close"]]
    df = df.sort_values("ret_bps", ascending=False).head(top_n)
    return df.reset_index(drop=True)


def main():
    n = _load("v3/cache/candles_1m_NIFTY.pkl")
    b = _load("v3/cache/candles_1m_BANKNIFTY.pkl")
    print(f"NIFTY  : {n.shape[0]:>6} bars, {n['ts'].min()} → {n['ts'].max()}")
    print(f"BN     : {b.shape[0]:>6} bars, {b['ts'].min()} → {b['ts'].max()}")

    en = _events(n, "NIFTY",     top_n=40, min_atr_ratio=3.5)
    eb = _events(b, "BANKNIFTY", top_n=40, min_atr_ratio=3.5)

    all_ev = pd.concat([en, eb], ignore_index=True)
    all_ev = all_ev.sort_values(["ts"]).reset_index(drop=True)
    out = ROOT / "news" / "backtest" / "velocity_events.csv"
    all_ev.to_csv(out, index=False)
    print(f"Wrote {len(all_ev)} events → {out}")
    print()
    print(all_ev.to_string(index=False, max_colwidth=20))


if __name__ == "__main__":
    main()
