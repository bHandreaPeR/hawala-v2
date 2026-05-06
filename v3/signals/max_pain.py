"""
v3/signals/max_pain.py
======================
Max Pain calculator from bhavcopy OI data.

Max pain = strike where total payout to all option buyers is minimized
           (i.e., where option writers collectively suffer minimum loss).

Formula:
  For each candidate strike K:
    pain_at_K = Σ max(0, K - strike_i) × CE_OI_i   [all calls above K that expire ITM]
              + Σ max(0, strike_j - K) × PE_OI_j   [all puts below K that expire ITM]
  max_pain_strike = argmin(pain_at_K)

Signal:
  Spot below max pain by >0.5%  → gravity pulls up  → direction=+1
  Spot above max pain by >0.5%  → gravity pulls down → direction=-1
  Within ±0.5%                  → neutral            → direction=0
  Confidence = min(1.0, |distance_pct| / 1.5)
"""
from __future__ import annotations
import logging
import pandas as pd
import numpy as np

log = logging.getLogger('v3.signals.max_pain')

# ── thresholds ────────────────────────────────────────────────────────────────
_NEUTRAL_BAND_PCT   = 0.5    # ±0.5% = neutral zone
_FULL_CONF_PCT      = 1.5    # 1.5% distance = confidence 1.0


def compute_max_pain(
    df_strikes: pd.DataFrame,
    spot: float,
    band: int = 3000,
) -> dict:
    """
    Compute max pain from bhavcopy strike OI data.

    Args:
        df_strikes: DataFrame with columns [strike, ce_oi, pe_oi].
                    Values must be non-negative integers/floats.
        spot:       Current spot price — used for distance calculation and
                    filtering to strikes within `band` points.
        band:       Only consider strikes within this many points of spot.
                    Default 3000 covers ~±4% for BankNifty at 75k, plenty
                    for Sensex at 76k.

    Returns:
        {
            'max_pain_strike': int,
            'distance_pts':    float,   # spot - max_pain (positive = spot above)
            'distance_pct':    float,   # distance as % of spot
            'direction':       int,     # 1=LONG, -1=SHORT, 0=neutral
            'confidence':      float,   # 0-1
            'pain_curve':      dict,    # {strike: pain_value} for debugging
        }

    Raises:
        ValueError: if df_strikes is missing required columns or is empty
                    after filtering, or if spot is non-positive.
        TypeError:  if df_strikes is not a DataFrame.
    """
    if not isinstance(df_strikes, pd.DataFrame):
        raise TypeError(
            f"compute_max_pain: df_strikes must be a pd.DataFrame, got {type(df_strikes)}"
        )
    required_cols = {'strike', 'ce_oi', 'pe_oi'}
    missing = required_cols - set(df_strikes.columns)
    if missing:
        raise ValueError(
            f"compute_max_pain: df_strikes missing required columns: {missing}. "
            f"Available: {list(df_strikes.columns)}"
        )
    if spot <= 0:
        raise ValueError(
            f"compute_max_pain: spot must be positive, got {spot}"
        )

    df = df_strikes[['strike', 'ce_oi', 'pe_oi']].copy()
    df = df.dropna(subset=['strike', 'ce_oi', 'pe_oi'])
    df['strike'] = df['strike'].astype(float)
    df['ce_oi']  = df['ce_oi'].astype(float)
    df['pe_oi']  = df['pe_oi'].astype(float)

    # Filter to strikes within band of spot
    df = df[(df['strike'] >= spot - band) & (df['strike'] <= spot + band)]

    if df.empty:
        raise ValueError(
            f"compute_max_pain: no strikes within band={band} of spot={spot:.0f}. "
            f"Raw strike range was "
            f"[{df_strikes['strike'].min():.0f}, {df_strikes['strike'].max():.0f}]"
        )

    strikes   = df['strike'].values
    ce_oi_arr = df['ce_oi'].values
    pe_oi_arr = df['pe_oi'].values

    # Compute pain at each candidate strike K
    # For K: calls above K that expire ITM → payout = (K - call_strike) × CE_OI
    #        (WAIT — calls are ITM when K > call_strike, i.e., spot > strike)
    # Corrected standard formula:
    #   CE pain at K = Σ_{ strike_i < K } max(0, K - strike_i) × CE_OI_i
    #   PE pain at K = Σ_{ strike_j > K } max(0, strike_j - K) × PE_OI_j
    pain_curve: dict[int, float] = {}

    for K in strikes:
        # Call side: all call strikes BELOW K are ITM if spot=K
        ce_pain = np.sum(np.maximum(0.0, K - strikes) * ce_oi_arr)
        # Put side: all put strikes ABOVE K are ITM if spot=K
        pe_pain = np.sum(np.maximum(0.0, strikes - K) * pe_oi_arr)
        pain_curve[int(K)] = float(ce_pain + pe_pain)

    max_pain_strike = min(pain_curve, key=pain_curve.__getitem__)

    distance_pts  = spot - max_pain_strike                      # positive = above
    distance_pct  = distance_pts / spot * 100.0

    if distance_pct < -_NEUTRAL_BAND_PCT:
        direction = 1    # spot below max pain → gravity up
    elif distance_pct > _NEUTRAL_BAND_PCT:
        direction = -1   # spot above max pain → gravity down
    else:
        direction = 0

    confidence = min(1.0, abs(distance_pct) / _FULL_CONF_PCT)

    log.debug(
        "max_pain computed",
        extra={
            "max_pain_strike": max_pain_strike,
            "spot":            spot,
            "distance_pts":    round(distance_pts, 1),
            "distance_pct":    round(distance_pct, 3),
            "direction":       direction,
            "confidence":      round(confidence, 3),
            "n_strikes":       len(strikes),
        },
    )

    return {
        'max_pain_strike': max_pain_strike,
        'distance_pts':    round(distance_pts, 2),
        'distance_pct':    round(distance_pct, 4),
        'direction':       direction,
        'confidence':      round(confidence, 4),
        'pain_curve':      pain_curve,
    }


def signal_max_pain(
    df_strikes: pd.DataFrame,
    spot: float,
    band: int = 3000,
) -> tuple[int, float, str]:
    """
    Thin wrapper over compute_max_pain() matching the engine.py signal interface.

    Returns (direction, confidence, note) where:
      direction  ∈ {-1, 0, 1}
      confidence ∈ [0, 1]
      note       : human-readable summary string

    Raises:
      ValueError / TypeError from compute_max_pain if inputs are bad.
    """
    result = compute_max_pain(df_strikes, spot, band=band)

    direction  = result['direction']
    confidence = result['confidence']
    mp         = result['max_pain_strike']
    dist_pts   = result['distance_pts']
    dist_pct   = result['distance_pct']

    if direction == 1:
        label = "below_max_pain"
    elif direction == -1:
        label = "above_max_pain"
    else:
        label = "at_max_pain"

    note = (
        f"{label} mp={mp} spot={spot:.0f} "
        f"dist={dist_pts:+.0f}pts ({dist_pct:+.2f}%)"
    )
    return direction, confidence, note
