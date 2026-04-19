# ============================================================
# strategies/patterns.py — Candlestick Pattern Detection Library
# ============================================================
# Pure detection functions. Each detector takes an OHLC DataFrame
# and returns a boolean pd.Series aligned to the bar on which the
# pattern COMPLETES (i.e., the last bar of a 1/2/3-bar pattern).
#
# All thresholds are ATR-relative so the detectors are instrument-
# and timeframe-agnostic (work on 15m/1H/daily bars alike).
#
# Convention:
#   body        = |Close - Open|
#   upper_wick  = High - max(Open, Close)
#   lower_wick  = min(Open, Close) - Low
#   range       = High - Low
#   bullish bar = Close > Open
#   bearish bar = Close < Open
# ============================================================

import warnings

import numpy as np
import pandas as pd

# Pattern detectors combine shifted bool series; pandas 2.x emits a cosmetic
# downcast FutureWarning on .fillna(False) for the resulting object dtype.
# The behaviour is correct — silence the noise module-locally.
warnings.filterwarnings(
    'ignore',
    category=FutureWarning,
    message=r"Downcasting object dtype arrays on .fillna",
)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _body(df):        return (df['Close'] - df['Open']).abs()
def _range(df):       return (df['High']  - df['Low']).clip(lower=1e-9)
def _upper_wick(df):  return df['High'] - df[['Open', 'Close']].max(axis=1)
def _lower_wick(df):  return df[['Open', 'Close']].min(axis=1) - df['Low']
def _is_bull(df):     return (df['Close'] > df['Open']).astype(bool)
def _is_bear(df):     return (df['Close'] < df['Open']).astype(bool)


def _shift_bool(s, n):
    """Shift a bool series by n and fill NaN with False, staying in bool dtype."""
    return s.shift(n).fillna(False).astype(bool)


def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Classic Wilder ATR on OHLC bars."""
    high, low, close = df['High'], df['Low'], df['Close']
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low).abs(),
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()


# ── Single-bar patterns ───────────────────────────────────────────────────────
def is_hammer(df, atr_series=None, body_atr_min=0.3, wick_ratio=2.0):
    """
    Bullish hammer: small body near top, long lower wick (≥ wick_ratio × body),
    minimal upper wick. Bar need not itself be bullish — context makes it a reversal.
    """
    a        = atr_series if atr_series is not None else atr(df)
    body     = _body(df)
    up_wick  = _upper_wick(df)
    lo_wick  = _lower_wick(df)

    cond = (
        (body >= body_atr_min * a * 0.4) &           # not a doji
        (lo_wick >= wick_ratio * body) &
        (up_wick <= 0.5 * body) &
        (lo_wick >= 0.5 * a)                         # meaningful wick vs ATR
    )
    return cond.fillna(False)


def is_shooting_star(df, atr_series=None, body_atr_min=0.3, wick_ratio=2.0):
    """Bearish shooting star: inverse hammer."""
    a        = atr_series if atr_series is not None else atr(df)
    body     = _body(df)
    up_wick  = _upper_wick(df)
    lo_wick  = _lower_wick(df)

    cond = (
        (body >= body_atr_min * a * 0.4) &
        (up_wick >= wick_ratio * body) &
        (lo_wick <= 0.5 * body) &
        (up_wick >= 0.5 * a)
    )
    return cond.fillna(False)


def is_doji(df, atr_series=None, body_frac=0.1):
    """Indecision bar: body < body_frac × range."""
    body  = _body(df)
    rng   = _range(df)
    return ((body / rng) < body_frac).fillna(False)


def is_marubozu(df, atr_series=None, wick_frac=0.05):
    """Full-body bar: negligible wicks, body ≈ range."""
    body    = _body(df)
    rng     = _range(df)
    up_wick = _upper_wick(df)
    lo_wick = _lower_wick(df)
    return (
        (body / rng > (1 - 2 * wick_frac)) &
        (up_wick / rng < wick_frac) &
        (lo_wick / rng < wick_frac)
    ).fillna(False)


# ── Two-bar patterns ──────────────────────────────────────────────────────────
def is_bullish_engulfing(df, atr_series=None, body_atr_min=0.3):
    """Prev bearish bar fully engulfed by current bullish bar."""
    a      = atr_series if atr_series is not None else atr(df)
    body   = _body(df)
    prev_o = df['Open'].shift(1)
    prev_c = df['Close'].shift(1)

    cond = (
        _shift_bool(_is_bear(df), 1) &
        _is_bull(df) &
        (df['Open']  <= prev_c) &
        (df['Close'] >= prev_o) &
        (body >= body_atr_min * a)
    )
    return cond.fillna(False)


def is_bearish_engulfing(df, atr_series=None, body_atr_min=0.3):
    """Prev bullish bar fully engulfed by current bearish bar."""
    a      = atr_series if atr_series is not None else atr(df)
    body   = _body(df)
    prev_o = df['Open'].shift(1)
    prev_c = df['Close'].shift(1)

    cond = (
        _shift_bool(_is_bull(df), 1) &
        _is_bear(df) &
        (df['Open']  >= prev_c) &
        (df['Close'] <= prev_o) &
        (body >= body_atr_min * a)
    )
    return cond.fillna(False)


def is_piercing(df, atr_series=None, body_atr_min=0.3):
    """Bullish reversal: bearish bar, then bullish bar closing above prev midpoint."""
    a       = atr_series if atr_series is not None else atr(df)
    prev_o  = df['Open'].shift(1)
    prev_c  = df['Close'].shift(1)
    prev_mid = (prev_o + prev_c) / 2

    cond = (
        _shift_bool(_is_bear(df), 1) &
        _is_bull(df) &
        (df['Open']  < prev_c) &
        (df['Close'] > prev_mid) &
        (df['Close'] < prev_o) &                # not full engulfing
        (_body(df) >= body_atr_min * a)
    )
    return cond.fillna(False)


def is_dark_cloud(df, atr_series=None, body_atr_min=0.3):
    """Bearish reversal mirror of piercing."""
    a        = atr_series if atr_series is not None else atr(df)
    prev_o   = df['Open'].shift(1)
    prev_c   = df['Close'].shift(1)
    prev_mid = (prev_o + prev_c) / 2

    cond = (
        _shift_bool(_is_bull(df), 1) &
        _is_bear(df) &
        (df['Open']  > prev_c) &
        (df['Close'] < prev_mid) &
        (df['Close'] > prev_o) &
        (_body(df) >= body_atr_min * a)
    )
    return cond.fillna(False)


def is_tweezer_bottom(df, atr_series=None, tol_frac=0.1):
    """Two consecutive bars with matching lows — tolerance = tol_frac × ATR."""
    a      = atr_series if atr_series is not None else atr(df)
    lows   = df['Low']
    prev_l = lows.shift(1)
    cond = (
        (lows - prev_l).abs() <= tol_frac * a
    ) & _shift_bool(_is_bear(df), 1) & _is_bull(df)
    return cond.fillna(False)


def is_tweezer_top(df, atr_series=None, tol_frac=0.1):
    a      = atr_series if atr_series is not None else atr(df)
    highs  = df['High']
    prev_h = highs.shift(1)
    cond = (
        (highs - prev_h).abs() <= tol_frac * a
    ) & _shift_bool(_is_bull(df), 1) & _is_bear(df)
    return cond.fillna(False)


# ── Three-bar patterns ────────────────────────────────────────────────────────
def is_morning_star(df, atr_series=None, body_atr_min=0.3, star_body_frac=0.3):
    """Bearish bar → small-body star → bullish bar closing into bar1 body."""
    a  = atr_series if atr_series is not None else atr(df)
    b  = _body(df)

    b1_bear = _shift_bool(_is_bear(df), 2)
    b1_body = b.shift(2)
    b2_small = (b.shift(1) < star_body_frac * b.shift(2))
    b3_bull = _is_bull(df)
    b3_close = df['Close']
    b1_mid   = (df['Open'].shift(2) + df['Close'].shift(2)) / 2

    cond = (
        b1_bear &
        (b1_body >= body_atr_min * a) &
        b2_small &
        b3_bull &
        (b3_close > b1_mid) &
        (b.ge(body_atr_min * a))            # bar3 meaningful
    )
    return cond.fillna(False)


def is_evening_star(df, atr_series=None, body_atr_min=0.3, star_body_frac=0.3):
    """Bullish → small star → bearish closing into bar1 body."""
    a  = atr_series if atr_series is not None else atr(df)
    b  = _body(df)

    b1_bull  = _shift_bool(_is_bull(df), 2)
    b1_body  = b.shift(2)
    b2_small = (b.shift(1) < star_body_frac * b.shift(2))
    b3_bear  = _is_bear(df)
    b3_close = df['Close']
    b1_mid   = (df['Open'].shift(2) + df['Close'].shift(2)) / 2

    cond = (
        b1_bull &
        (b1_body >= body_atr_min * a) &
        b2_small &
        b3_bear &
        (b3_close < b1_mid) &
        (b.ge(body_atr_min * a))
    )
    return cond.fillna(False)


def is_three_white_soldiers(df, atr_series=None, body_atr_min=0.3):
    """Three consecutive bullish bars, each closing higher."""
    a = atr_series if atr_series is not None else atr(df)
    b = _body(df)
    c = df['Close']

    cond = (
        _is_bull(df) &
        _shift_bool(_is_bull(df), 1) &
        _shift_bool(_is_bull(df), 2) &
        (c > c.shift(1)) & (c.shift(1) > c.shift(2)) &
        (b >= body_atr_min * a) &
        (b.shift(1) >= body_atr_min * a) &
        (b.shift(2) >= body_atr_min * a)
    )
    return cond.fillna(False)


def is_three_black_crows(df, atr_series=None, body_atr_min=0.3):
    """Three consecutive bearish bars, each closing lower."""
    a = atr_series if atr_series is not None else atr(df)
    b = _body(df)
    c = df['Close']

    cond = (
        _is_bear(df) &
        _shift_bool(_is_bear(df), 1) &
        _shift_bool(_is_bear(df), 2) &
        (c < c.shift(1)) & (c.shift(1) < c.shift(2)) &
        (b >= body_atr_min * a) &
        (b.shift(1) >= body_atr_min * a) &
        (b.shift(2) >= body_atr_min * a)
    )
    return cond.fillna(False)


# ── Pattern registry ──────────────────────────────────────────────────────────
# direction: +1 bullish, -1 bearish
BULLISH_PATTERNS = {
    'hammer':                 is_hammer,
    'bullish_engulfing':      is_bullish_engulfing,
    'piercing':               is_piercing,
    'tweezer_bottom':         is_tweezer_bottom,
    'morning_star':           is_morning_star,
    'three_white_soldiers':   is_three_white_soldiers,
}
BEARISH_PATTERNS = {
    'shooting_star':          is_shooting_star,
    'bearish_engulfing':      is_bearish_engulfing,
    'dark_cloud':             is_dark_cloud,
    'tweezer_top':            is_tweezer_top,
    'evening_star':           is_evening_star,
    'three_black_crows':      is_three_black_crows,
}


def detect_all_patterns(df: pd.DataFrame,
                        atr_series: pd.Series = None,
                        body_atr_min: float = 0.3,
                        wick_ratio: float = 2.0) -> pd.DataFrame:
    """
    Run every detector and return a DataFrame aligned to df.index with one
    boolean column per pattern, plus summary columns:
      - bullish_hits  : count of bullish patterns on this bar
      - bearish_hits  : count of bearish patterns on this bar
      - bullish_names : '|'-joined names of bullish patterns that fired
      - bearish_names : '|'-joined names of bearish patterns that fired
    """
    if atr_series is None:
        atr_series = atr(df, period=14)

    out = pd.DataFrame(index=df.index)

    for name, fn in BULLISH_PATTERNS.items():
        try:
            out[name] = fn(df, atr_series=atr_series,
                           body_atr_min=body_atr_min, wick_ratio=wick_ratio) \
                        if name in ('hammer',) else fn(df, atr_series=atr_series,
                                                       body_atr_min=body_atr_min) \
                        if name in ('bullish_engulfing', 'piercing',
                                    'morning_star', 'three_white_soldiers') \
                        else fn(df, atr_series=atr_series)
        except TypeError:
            out[name] = fn(df, atr_series=atr_series)

    for name, fn in BEARISH_PATTERNS.items():
        try:
            out[name] = fn(df, atr_series=atr_series,
                           body_atr_min=body_atr_min, wick_ratio=wick_ratio) \
                        if name in ('shooting_star',) else fn(df, atr_series=atr_series,
                                                               body_atr_min=body_atr_min) \
                        if name in ('bearish_engulfing', 'dark_cloud',
                                    'evening_star', 'three_black_crows') \
                        else fn(df, atr_series=atr_series)
        except TypeError:
            out[name] = fn(df, atr_series=atr_series)

    bull_cols = list(BULLISH_PATTERNS.keys())
    bear_cols = list(BEARISH_PATTERNS.keys())
    out['bullish_hits'] = out[bull_cols].sum(axis=1).astype(int)
    out['bearish_hits'] = out[bear_cols].sum(axis=1).astype(int)
    out['bullish_names'] = out[bull_cols].apply(
        lambda row: '|'.join([c for c in bull_cols if row[c]]), axis=1
    )
    out['bearish_names'] = out[bear_cols].apply(
        lambda row: '|'.join([c for c in bear_cols if row[c]]), axis=1
    )
    return out


# ── Indicators commonly paired with pattern signals ──────────────────────────
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up    = delta.clip(lower=0)
    down  = -delta.clip(upper=0)
    roll_up   = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50.0)
