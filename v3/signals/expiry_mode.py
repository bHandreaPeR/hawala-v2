"""
v3/signals/expiry_mode.py
=========================
Expiry-day detection and intraday reversal signal.

Expiry schedules (NSE/BSE):
  BankNifty  — weekly Wednesday (NSE)
  Nifty      — weekly Thursday (NSE)
  Sensex     — weekly Thursday (BSE)
  FinNifty   — weekly Tuesday  (NSE)
  MidcpNifty — weekly Monday   (NSE)

For each index we check the standard weekly expiry weekday.  If the exact
calendar day falls on a market holiday the exchange typically moves expiry to
the prior trading day, but that edge-case requires a holiday calendar.  We
handle it by accepting the day-before-expiry shift: if the nominated weekday
is a holiday (not in NSE trading calendar) the actual expiry may be the
previous business day — callers can pass a resolved `trade_date` directly.

Intraday reversal signal:
  On expiry day, max pain acts as a gravitational attractor.  The classic
  pattern is an early sell-off below max pain followed by a recovery back
  above it, as short-sellers cover and writers defend.

  Long trigger conditions (all required):
    1. Spot dropped below (max_pain_strike - 200) at some point today
    2. Current spot has since recovered above max_pain_strike
    3. The intraday low occurred between bar 5 and bar 60 (9:20 – 10:15)
    4. Recovery from intraday low ≥ 0.3%

  Short trigger conditions (symmetric, less common):
    1. Spot spiked above max_pain_strike by >1% (≥ 1 bar touched that level)
    2. Current spot has since fallen back below max_pain_strike
    3. Spike occurred between bar 5 and bar 60
    4. Drop from spike peak ≥ 0.3%

Confidence scoring:
  Base:  0.50
  +0.20  if recovery (or drop) from extreme ≥ 0.5%
  +0.15  if extreme was 0.5%–1.5% beyond max pain (sweet spot)
  +0.15  if current price is firmly back on the expected side of max pain
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger('v3.signals.expiry_mode')

# ── Expiry weekday map ────────────────────────────────────────────────────────
# weekday(): Monday=0 … Sunday=6
_EXPIRY_WEEKDAY: dict[str, int] = {
    'BANKNIFTY':  2,   # Wednesday
    'NIFTY':      3,   # Thursday
    'SENSEX':     3,   # Thursday
    'FINNIFTY':   1,   # Tuesday
    'MIDCPNIFTY': 0,   # Monday
}

# ── Signal thresholds ─────────────────────────────────────────────────────────
_PIERCE_BUFFER_PTS   = 200    # spot must have gone >= 200 pts below max pain
_SPIKE_PCT           = 1.0    # short: spot spiked >=1% above max pain
_MIN_RECOVERY_PCT    = 0.3    # minimum move from extreme to signal
_RECOVERY_BONUS_PCT  = 0.5    # ≥0.5% recovery → +0.20 confidence
_SWEET_SPOT_MIN_PCT  = 0.5    # extreme ≥0.5% beyond max pain
_SWEET_SPOT_MAX_PCT  = 1.5    # extreme ≤1.5% beyond max pain
_MIN_BAR             = 5      # low cannot be at open (bars 0-4 = 9:15–9:19)
_MAX_BAR             = 60     # low must appear before bar 60 (10:15)


def is_expiry_day(trade_date: date, index: str = 'BANKNIFTY') -> bool:
    """
    Return True if trade_date is the scheduled weekly expiry day for index.

    Supported index names (case-insensitive):
      BANKNIFTY, NIFTY, SENSEX, FINNIFTY, MIDCPNIFTY

    Note: Does NOT account for NSE/BSE holiday shifts.  For precise expiry
    dates use the bhavcopy cache (if the date has an entry with DTE=0 it IS
    expiry day).  This function gives the calendar approximation.

    Raises:
        ValueError: if index is not in the known list.
    """
    index_upper = index.upper()
    if index_upper not in _EXPIRY_WEEKDAY:
        raise ValueError(
            f"is_expiry_day: unknown index '{index}'. "
            f"Supported: {list(_EXPIRY_WEEKDAY.keys())}"
        )
    expected_weekday = _EXPIRY_WEEKDAY[index_upper]
    return trade_date.weekday() == expected_weekday


def compute_expiry_reversal_signal(
    candles_1m: pd.DataFrame,
    max_pain_strike: int,
    spot_open: float,
    current_bar_idx: int,
) -> tuple[int, float, str]:
    """
    On expiry day, detect intraday reversal at/near max pain support.

    Args:
        candles_1m:      1-minute OHLCV DataFrame with columns
                         [open, high, low, close] and a DatetimeIndex or
                         integer index.  Must contain at least current_bar_idx+1
                         rows.  Row 0 = 9:15 candle.
        max_pain_strike: Max pain strike for today (from compute_max_pain or
                         precomputed from bhavcopy).
        spot_open:       Opening spot price (used as reference for % calcs).
        current_bar_idx: Index of the current (most recent completed) 1m bar.
                         0 = 9:15, 75 = 10:30, 375 = 15:30.

    Returns:
        (direction, confidence, note)
        direction ∈ {-1, 0, 1}

    Raises:
        ValueError: if candles_1m is missing required columns, is empty, or
                    current_bar_idx is out of range.
        TypeError:  if candles_1m is not a DataFrame.
    """
    if not isinstance(candles_1m, pd.DataFrame):
        raise TypeError(
            f"compute_expiry_reversal_signal: candles_1m must be pd.DataFrame, "
            f"got {type(candles_1m)}"
        )
    required_cols = {'open', 'high', 'low', 'close'}
    missing = required_cols - set(candles_1m.columns)
    if missing:
        raise ValueError(
            f"compute_expiry_reversal_signal: candles_1m missing columns: {missing}. "
            f"Available: {list(candles_1m.columns)}"
        )
    if len(candles_1m) == 0:
        raise ValueError("compute_expiry_reversal_signal: candles_1m is empty")
    if current_bar_idx < 0 or current_bar_idx >= len(candles_1m):
        raise ValueError(
            f"compute_expiry_reversal_signal: current_bar_idx={current_bar_idx} "
            f"out of range for candles of length {len(candles_1m)}"
        )
    if spot_open <= 0:
        raise ValueError(
            f"compute_expiry_reversal_signal: spot_open must be positive, "
            f"got {spot_open}"
        )
    if max_pain_strike <= 0:
        raise ValueError(
            f"compute_expiry_reversal_signal: max_pain_strike must be positive, "
            f"got {max_pain_strike}"
        )

    # Work with the bars up to and including current_bar_idx
    bars = candles_1m.iloc[: current_bar_idx + 1]
    current_price = float(bars['close'].iloc[-1])
    mp = float(max_pain_strike)

    # ── LONG SETUP: spot pierced below max pain, has since recovered ─────────
    # Step 1: find intraday low and its bar index within the search window
    search_bars = bars.iloc[_MIN_BAR: min(current_bar_idx + 1, _MAX_BAR + 1)]

    if len(search_bars) == 0:
        return 0, 0.0, f"expiry: bar {current_bar_idx} too early (need >{_MIN_BAR})"

    low_series  = search_bars['low']
    low_bar_rel = int(low_series.idxmin()) if hasattr(low_series.index, '__iter__') else low_series.values.argmin()
    # Handle both integer index and datetime index
    try:
        low_bar_abs = int(low_series.index[low_series.values.argmin()])
    except (TypeError, ValueError):
        low_bar_abs = _MIN_BAR + int(low_series.values.argmin())

    intraday_low = float(low_series.min())

    # Did the low pierce below max pain (or within 200 pts)?
    long_pierce_threshold = mp - _PIERCE_BUFFER_PTS
    pierced_low = intraday_low <= long_pierce_threshold

    # Recovery from intraday low
    recovery_pts = current_price - intraday_low
    recovery_pct = recovery_pts / spot_open * 100.0

    # How far below max pain was the low?
    low_below_mp_pts = mp - intraday_low
    low_below_mp_pct = low_below_mp_pts / spot_open * 100.0

    # Current price recovered above max pain?
    recovered_above_mp = current_price >= mp

    if (
        pierced_low
        and recovery_pct >= _MIN_RECOVERY_PCT
        and recovered_above_mp
    ):
        # Valid LONG setup
        conf = 0.50
        if recovery_pct >= _RECOVERY_BONUS_PCT:
            conf += 0.20
        if _SWEET_SPOT_MIN_PCT <= low_below_mp_pct <= _SWEET_SPOT_MAX_PCT:
            conf += 0.15
        if current_price > mp:
            conf += 0.15
        conf = min(1.0, conf)

        note = (
            f"expiry_reversal_long mp={max_pain_strike} "
            f"low={intraday_low:.0f} ({low_below_mp_pct:+.2f}%_below_mp) "
            f"recovery={recovery_pct:.2f}% curr={current_price:.0f}"
        )
        log.info(
            "expiry reversal LONG signal fired",
            extra={
                "max_pain_strike": max_pain_strike,
                "intraday_low":    round(intraday_low, 1),
                "low_below_mp_pct": round(low_below_mp_pct, 3),
                "recovery_pct":    round(recovery_pct, 3),
                "current_price":   round(current_price, 1),
                "confidence":      round(conf, 3),
                "current_bar":     current_bar_idx,
            },
        )
        return 1, round(conf, 4), note

    # ── SHORT SETUP: spot spiked above max pain, has since reversed ──────────
    high_series  = search_bars['high']
    intraday_high = float(high_series.max())

    spike_threshold_pts = mp * (1.0 + _SPIKE_PCT / 100.0)
    spiked_high = intraday_high >= spike_threshold_pts

    drop_pts = intraday_high - current_price
    drop_pct = drop_pts / spot_open * 100.0

    high_above_mp_pts = intraday_high - mp
    high_above_mp_pct = high_above_mp_pts / spot_open * 100.0

    dropped_below_mp = current_price < mp

    if (
        spiked_high
        and drop_pct >= _MIN_RECOVERY_PCT
        and dropped_below_mp
    ):
        conf = 0.50
        if drop_pct >= _RECOVERY_BONUS_PCT:
            conf += 0.20
        if _SWEET_SPOT_MIN_PCT <= high_above_mp_pct <= _SWEET_SPOT_MAX_PCT:
            conf += 0.15
        if current_price < mp:
            conf += 0.15
        conf = min(1.0, conf)

        note = (
            f"expiry_reversal_short mp={max_pain_strike} "
            f"high={intraday_high:.0f} ({high_above_mp_pct:+.2f}%_above_mp) "
            f"drop={drop_pct:.2f}% curr={current_price:.0f}"
        )
        log.info(
            "expiry reversal SHORT signal fired",
            extra={
                "max_pain_strike":  max_pain_strike,
                "intraday_high":    round(intraday_high, 1),
                "high_above_mp_pct": round(high_above_mp_pct, 3),
                "drop_pct":         round(drop_pct, 3),
                "current_price":    round(current_price, 1),
                "confidence":       round(conf, 3),
                "current_bar":      current_bar_idx,
            },
        )
        return -1, round(conf, 4), note

    # ── No setup ─────────────────────────────────────────────────────────────
    why_parts = []
    if not pierced_low:
        why_parts.append(f"low={intraday_low:.0f}_not_below_mp-{_PIERCE_BUFFER_PTS}")
    elif not recovered_above_mp:
        why_parts.append(f"not_recovered_above_mp({mp:.0f}),curr={current_price:.0f}")
    elif recovery_pct < _MIN_RECOVERY_PCT:
        why_parts.append(f"recovery_too_small({recovery_pct:.2f}%<{_MIN_RECOVERY_PCT}%)")

    if not spiked_high:
        why_parts.append(f"no_spike_above_mp+{_SPIKE_PCT}%")

    note = f"expiry_no_setup [{'; '.join(why_parts) or 'conditions_unmet'}]"
    return 0, 0.0, note
