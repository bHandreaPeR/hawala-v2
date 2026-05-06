"""
v3/signals/cross_index.py
=========================
Cross-index confirmation signal.

Theory: When index A (the "leader") has already bottomed and is recovering
while index B (the "lagger") is still near its intraday low, index B's
options are mispriced relative to A's recovery signal.  Enter in the
direction of A's recovery on B.

The Apr 30 2026 Sensex expiry pattern is the canonical example:
  - BankNifty bottomed at 10:24, recovered 0.7%+ by 10:55
  - Sensex was still at 76,261 (intraday low) at 10:55
  - Sensex 76,500 CE was priced at ~₹62-90 implying ~85% worthless
  - In reality BankNifty's recovery made that probability far lower

Usage:
    direction, conf, note = compute_cross_index_signal(
        leader_candles   = bn_1m,
        lagger_candles   = sensex_1m,
        current_bar_idx  = current_bar,
    )

Confidence scoring:
  Base:  0.40
  +0.20  if leader recovery from bottom ≥ 0.5%
  +0.20  if lag_bars ∈ [5, 35]  (not noise, not stale)
  +0.20  if lagger still within 0.1% of its own low (hasn't started moving)
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger('v3.signals.cross_index')

# ── Thresholds ────────────────────────────────────────────────────────────────
_MIN_RECOVERY_PCT       = 0.3    # leader must have recovered ≥ this from its low
_RECOVERY_BONUS_PCT     = 0.5    # leader ≥0.5% recovery → +0.20 conf
_LAG_BARS_MIN           = 5      # fewer bars = noise (bottoms too close)
_LAG_BARS_MAX           = 35     # more bars = signal too stale
_LAGGER_NEAR_LOW_PCT    = 0.2    # lagger within 0.2% of its low = hasn't moved yet
_LAGGER_NEAR_LOW_BONUS  = 0.1    # within 0.1% → full +0.20 conf bonus


def find_intraday_bottom(
    candles_1m: pd.DataFrame,
    search_from_bar: int = 0,
    search_to_bar: int = 75,
) -> dict:
    """
    Find the lowest candle low within [search_from_bar, search_to_bar).

    Args:
        candles_1m:       1-minute OHLCV DataFrame.  Requires 'low' column.
                          Row 0 = 9:15, row N = 9:15+N minutes.
        search_from_bar:  First bar to include (inclusive).
        search_to_bar:    Last bar to include (exclusive).  Clipped to len(df).

    Returns:
        {
            'bottom_bar_idx':          int,    # 0-based row index in candles_1m
            'bottom_price':            float,
            'bottom_ts':               pd.Timestamp or None,
            'recovery_pct_from_bottom': float, # recovery of close at last bar
        }

    Raises:
        TypeError:  if candles_1m is not a DataFrame.
        ValueError: if 'low' column missing, or slice is empty.
    """
    if not isinstance(candles_1m, pd.DataFrame):
        raise TypeError(
            f"find_intraday_bottom: candles_1m must be pd.DataFrame, "
            f"got {type(candles_1m)}"
        )
    if 'low' not in candles_1m.columns:
        raise ValueError(
            f"find_intraday_bottom: 'low' column missing. "
            f"Available: {list(candles_1m.columns)}"
        )

    to_bar = min(search_to_bar, len(candles_1m))
    if search_from_bar >= to_bar:
        raise ValueError(
            f"find_intraday_bottom: empty search range "
            f"[{search_from_bar}, {to_bar}) in df of length {len(candles_1m)}"
        )

    slice_df = candles_1m.iloc[search_from_bar:to_bar]
    low_arr  = slice_df['low'].values.astype(float)

    rel_idx   = int(np.argmin(low_arr))           # relative to slice
    abs_idx   = search_from_bar + rel_idx          # relative to full df
    bottom_price = float(low_arr[rel_idx])

    # Timestamp (if index is DatetimeIndex)
    try:
        bottom_ts = candles_1m.index[abs_idx]
        if not isinstance(bottom_ts, pd.Timestamp):
            bottom_ts = None
    except (IndexError, TypeError):
        bottom_ts = None

    # Recovery: compare last available close to the bottom
    last_close = float(candles_1m['close'].iloc[-1])
    recovery_pct = (last_close - bottom_price) / bottom_price * 100.0

    return {
        'bottom_bar_idx':           abs_idx,
        'bottom_price':             bottom_price,
        'bottom_ts':                bottom_ts,
        'recovery_pct_from_bottom': round(recovery_pct, 4),
    }


def find_intraday_top(
    candles_1m: pd.DataFrame,
    search_from_bar: int = 0,
    search_to_bar: int = 75,
) -> dict:
    """
    Find the highest candle high within [search_from_bar, search_to_bar).
    Mirror of find_intraday_bottom — used for the SHORT (leader topped) case.

    Returns:
        {
            'top_bar_idx':         int,
            'top_price':           float,
            'top_ts':              pd.Timestamp or None,
            'drop_pct_from_top':   float,   # drop of close at last bar
        }

    Raises:
        TypeError / ValueError: same conditions as find_intraday_bottom.
    """
    if not isinstance(candles_1m, pd.DataFrame):
        raise TypeError(
            f"find_intraday_top: candles_1m must be pd.DataFrame, "
            f"got {type(candles_1m)}"
        )
    if 'high' not in candles_1m.columns:
        raise ValueError(
            f"find_intraday_top: 'high' column missing. "
            f"Available: {list(candles_1m.columns)}"
        )

    to_bar = min(search_to_bar, len(candles_1m))
    if search_from_bar >= to_bar:
        raise ValueError(
            f"find_intraday_top: empty search range "
            f"[{search_from_bar}, {to_bar}) in df of length {len(candles_1m)}"
        )

    slice_df = candles_1m.iloc[search_from_bar:to_bar]
    high_arr = slice_df['high'].values.astype(float)

    rel_idx   = int(np.argmax(high_arr))
    abs_idx   = search_from_bar + rel_idx
    top_price = float(high_arr[rel_idx])

    try:
        top_ts = candles_1m.index[abs_idx]
        if not isinstance(top_ts, pd.Timestamp):
            top_ts = None
    except (IndexError, TypeError):
        top_ts = None

    last_close = float(candles_1m['close'].iloc[-1])
    drop_pct   = (top_price - last_close) / top_price * 100.0

    return {
        'top_bar_idx':       abs_idx,
        'top_price':         top_price,
        'top_ts':            top_ts,
        'drop_pct_from_top': round(drop_pct, 4),
    }


def compute_cross_index_signal(
    leader_candles: pd.DataFrame,
    lagger_candles: pd.DataFrame,
    current_bar_idx: int,
    min_recovery_pct: float = _MIN_RECOVERY_PCT,
    max_lag_bars: int = 45,
) -> tuple[int, float, str]:
    """
    Check if leader has bounced while lagger is still near its intraday low.

    Args:
        leader_candles:    1m OHLCV for the leading index (e.g. BankNifty).
                           Must have [open, high, low, close] columns.
                           Row 0 = 9:15.
        lagger_candles:    1m OHLCV for the lagging index (e.g. Sensex).
                           Same format.  Length must be >= current_bar_idx + 1.
        current_bar_idx:   Most recent completed 1m bar (0-based).
        min_recovery_pct:  Minimum recovery from bottom for leader to qualify.
        max_lag_bars:      Lagger's bottom must be within this many bars of
                           leader's bottom.

    Returns:
        (direction, confidence, note)
        direction  1  = lagger should follow leader UP   (LONG)
        direction -1  = lagger should follow leader DOWN (SHORT)
        direction  0  = no signal

    Raises:
        TypeError:  if either candles arg is not a DataFrame.
        ValueError: if required columns are missing, DataFrames are too short,
                    or current_bar_idx is out of range.
    """
    # ── Input validation ────────────────────────────────────────────────────
    for name, df in [('leader_candles', leader_candles),
                     ('lagger_candles', lagger_candles)]:
        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"compute_cross_index_signal: {name} must be pd.DataFrame, "
                f"got {type(df)}"
            )
        required = {'open', 'high', 'low', 'close'}
        missing  = required - set(df.columns)
        if missing:
            raise ValueError(
                f"compute_cross_index_signal: {name} missing columns: {missing}. "
                f"Available: {list(df.columns)}"
            )
        if len(df) == 0:
            raise ValueError(
                f"compute_cross_index_signal: {name} is empty"
            )

    if current_bar_idx < 0:
        raise ValueError(
            f"compute_cross_index_signal: current_bar_idx={current_bar_idx} < 0"
        )
    if current_bar_idx >= len(leader_candles):
        raise ValueError(
            f"compute_cross_index_signal: current_bar_idx={current_bar_idx} "
            f"out of range for leader_candles length {len(leader_candles)}"
        )
    if current_bar_idx >= len(lagger_candles):
        raise ValueError(
            f"compute_cross_index_signal: current_bar_idx={current_bar_idx} "
            f"out of range for lagger_candles length {len(lagger_candles)}"
        )

    # ── Work on bars up to current_bar_idx ───────────────────────────────────
    leader_bars = leader_candles.iloc[: current_bar_idx + 1]
    lagger_bars = lagger_candles.iloc[: current_bar_idx + 1]

    search_to = min(current_bar_idx + 1, 76)   # don't look past 10:30

    if search_to <= 0:
        return 0, 0.0, "cross_index: current_bar_idx=0, nothing to compare"

    current_leader_close = float(leader_bars['close'].iloc[-1])
    current_lagger_close = float(lagger_bars['close'].iloc[-1])

    # ══════════════════════════════════════════════════════════════════════════
    # CASE 1: LONG — leader bottomed and recovered, lagger still at low
    # ══════════════════════════════════════════════════════════════════════════
    try:
        leader_bottom = find_intraday_bottom(
            leader_candles, search_from_bar=0, search_to_bar=search_to
        )
        lagger_bottom = find_intraday_bottom(
            lagger_candles, search_from_bar=0, search_to_bar=search_to
        )
    except ValueError as exc:
        return 0, 0.0, f"cross_index: bottom search failed — {exc}"

    # Leader recovery at current bar
    leader_recovery_pct = (
        (current_leader_close - leader_bottom['bottom_price'])
        / leader_bottom['bottom_price'] * 100.0
    )

    # How far has the lagger recovered from its own low?
    lagger_recovery_pct = (
        (current_lagger_close - lagger_bottom['bottom_price'])
        / lagger_bottom['bottom_price'] * 100.0
    )

    # Bar gap between the two bottoms (positive = lagger bottomed later)
    lag_bars = lagger_bottom['bottom_bar_idx'] - leader_bottom['bottom_bar_idx']

    if (
        leader_recovery_pct >= min_recovery_pct
        and lagger_recovery_pct < _LAGGER_NEAR_LOW_PCT
    ):
        # Valid LONG setup — leader bounced, lagger hasn't moved yet
        conf = 0.40

        if leader_recovery_pct >= _RECOVERY_BONUS_PCT:
            conf += 0.20

        if _LAG_BARS_MIN <= abs(lag_bars) <= _LAG_BARS_MAX:
            conf += 0.20

        if lagger_recovery_pct <= _LAGGER_NEAR_LOW_BONUS:
            conf += 0.20

        conf = min(1.0, conf)

        note = (
            f"cross_long leader_recovery={leader_recovery_pct:.2f}% "
            f"lagger_recovery={lagger_recovery_pct:.2f}% "
            f"lag_bars={lag_bars} "
            f"leader_low={leader_bottom['bottom_price']:.0f} "
            f"lagger_low={lagger_bottom['bottom_price']:.0f}"
        )
        log.info(
            "cross-index LONG signal fired",
            extra={
                "leader_recovery_pct": round(leader_recovery_pct, 3),
                "lagger_recovery_pct": round(lagger_recovery_pct, 3),
                "lag_bars":            lag_bars,
                "confidence":          round(conf, 3),
                "current_bar":         current_bar_idx,
            },
        )
        return 1, round(conf, 4), note

    # ══════════════════════════════════════════════════════════════════════════
    # CASE 2: SHORT — leader topped and dropped, lagger still near its high
    # ══════════════════════════════════════════════════════════════════════════
    try:
        leader_top = find_intraday_top(
            leader_candles, search_from_bar=0, search_to_bar=search_to
        )
        lagger_top = find_intraday_top(
            lagger_candles, search_from_bar=0, search_to_bar=search_to
        )
    except ValueError as exc:
        return 0, 0.0, f"cross_index: top search failed — {exc}"

    leader_drop_pct = (
        (leader_top['top_price'] - current_leader_close)
        / leader_top['top_price'] * 100.0
    )
    lagger_drop_pct = (
        (lagger_top['top_price'] - current_lagger_close)
        / lagger_top['top_price'] * 100.0
    )

    lag_bars_short = lagger_top['top_bar_idx'] - leader_top['top_bar_idx']

    if (
        leader_drop_pct >= min_recovery_pct
        and lagger_drop_pct < _LAGGER_NEAR_LOW_PCT
    ):
        conf = 0.40

        if leader_drop_pct >= _RECOVERY_BONUS_PCT:
            conf += 0.20

        if _LAG_BARS_MIN <= abs(lag_bars_short) <= _LAG_BARS_MAX:
            conf += 0.20

        if lagger_drop_pct <= _LAGGER_NEAR_LOW_BONUS:
            conf += 0.20

        conf = min(1.0, conf)

        note = (
            f"cross_short leader_drop={leader_drop_pct:.2f}% "
            f"lagger_drop={lagger_drop_pct:.2f}% "
            f"lag_bars={lag_bars_short} "
            f"leader_high={leader_top['top_price']:.0f} "
            f"lagger_high={lagger_top['top_price']:.0f}"
        )
        log.info(
            "cross-index SHORT signal fired",
            extra={
                "leader_drop_pct":  round(leader_drop_pct, 3),
                "lagger_drop_pct":  round(lagger_drop_pct, 3),
                "lag_bars":         lag_bars_short,
                "confidence":       round(conf, 3),
                "current_bar":      current_bar_idx,
            },
        )
        return -1, round(conf, 4), note

    # ── No setup ─────────────────────────────────────────────────────────────
    note = (
        f"cross_no_signal "
        f"leader_rec={leader_recovery_pct:.2f}% "
        f"lagger_rec={lagger_recovery_pct:.2f}% "
        f"leader_drop={leader_drop_pct:.2f}% "
        f"lagger_drop={lagger_drop_pct:.2f}%"
    )
    return 0, 0.0, note
