"""
v3/signals/engine.py
=====================
V3 Signal Engine — 6 core signal modules + 3 optional extra modules.

Core signals (always computed):
  1. OI Quadrant     — Price × OI direction (4-state classifier)
  2. Futures Basis   — Contango/backwardation premium as institutional intent
  3. PCR             — Put-Call Ratio regime
  4. OI Velocity     — Rate of OI build/unwind per strike (live only)
  5. Strike Defense  — Active defense of call/put walls
  6. FII Signature   — FII/DII real-time attribution via FIIDIIClassifier
                       (falls back to lag-1 fii_fut_level when classifier
                        result is not supplied)

Extra signals (computed only when optional inputs are supplied):
  7. Max Pain        — Bhavcopy OI gravity toward max pain strike
  8. Expiry Reversal — Intraday bounce at max pain on expiry day
  9. Cross-Index     — Leader/lagger divergence confirmation

Real-time inputs (new vs old lag-1 design):
  • spot_ltp          — real-time spot price (not lag-1)
  • fii_dii_result    — dict from FIIDIIClassifier.classify() (optional)
                        if None falls back to fii_fut_level / fii_cash_lag1
  • velocity_data     — per-minute OI velocity dict from live chain

Output: SignalState dataclass with per-signal direction + combined score

SignalSmoother:
  EMA smoothing (α=0.4) over the combined score.
  direction fires only if: |smoothed_score| > threshold AND
  same direction for ≥ 2 consecutive bars AND score is not weakening.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import numpy as np

log = logging.getLogger('v3.signals')

# ── Extra signal imports (optional — only imported at call time if needed) ────
# Imported here at module level to surface import errors early.
from v3.signals.max_pain   import signal_max_pain
from v3.signals.expiry_mode import compute_expiry_reversal_signal
from v3.signals.cross_index import compute_cross_index_signal


# ── Signal output structure ───────────────────────────────────────────────────
@dataclass
class SignalState:
    timestamp: pd.Timestamp = None

    # Per-signal: 1=LONG, -1=SHORT, 0=NEUTRAL
    oi_quadrant:      int = 0
    futures_basis:    int = 0
    pcr:              int = 0
    oi_velocity:      int = 0
    strike_defense:   int = 0
    fii_signature:    int = 0

    # Confidence: 0-1 per signal
    oi_quadrant_conf:    float = 0.0
    futures_basis_conf:  float = 0.0
    pcr_conf:            float = 0.0
    oi_velocity_conf:    float = 0.0
    strike_defense_conf: float = 0.0
    fii_signature_conf:  float = 0.0

    # Combined
    score:     float = 0.0   # -1 to +1 weighted
    direction: int   = 0     # final: 1 / -1 / 0
    signal_count: int = 0    # how many signals fired

    # Extra signals (optional — default to neutral if not computed)
    max_pain:              int   = 0
    max_pain_conf:         float = 0.0
    expiry_reversal:       int   = 0
    expiry_reversal_conf:  float = 0.0
    cross_index:           int   = 0
    cross_index_conf:      float = 0.0

    # Context
    ltp:       float = 0.0
    pcr_live:  float = 0.0
    call_wall: Optional[int] = None
    put_wall:  Optional[int] = None
    notes:     list = field(default_factory=list)


# ── Weights ───────────────────────────────────────────────────────────────────
WEIGHTS = {
    'oi_quadrant':   0.20,
    'futures_basis': 0.15,
    'pcr':           0.15,
    'oi_velocity':   0.25,   # highest: fastest, most direct
    'strike_defense':0.15,
    'fii_signature': 0.10,
}

# Extra signal weights — additive, only applied when the signal fires.
# The extra signals extend the weighted pool; they do NOT replace or
# rescale the core WEIGHTS above.
WEIGHTS_EXTRA = {
    'max_pain':        0.20,
    'expiry_reversal': 0.30,   # highest weight — very reliable on expiry day
    'cross_index':     0.25,
}


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 1 — OI Quadrant (Price × OI direction)
# ═══════════════════════════════════════════════════════════════════════════════
def signal_oi_quadrant(df_1m: pd.DataFrame, window: int = 5) -> tuple[int, float, str]:
    """
    Classify last `window` candles into OI quadrant:
      Price↑ + OI↑ → LONG  (long buildup)     score= +1
      Price↓ + OI↑ → SHORT (short buildup)    score= -1
      Price↑ + OI↓ → WEAK LONG (short cover)  score= +0.5
      Price↓ + OI↓ → WEAK SHORT (long unwind) score= -0.5

    Returns (direction, confidence, note)
    """
    if df_1m is None or len(df_1m) < window + 1:
        return 0, 0.0, "insufficient data"

    recent = df_1m.tail(window + 1).copy()
    recent = recent.dropna(subset=['close', 'oi'])

    if len(recent) < 2:
        return 0, 0.0, "oi data missing"

    price_chg = recent['close'].iloc[-1] - recent['close'].iloc[0]
    oi_chg    = recent['oi'].iloc[-1] - recent['oi'].iloc[0]

    # Normalize by std to get confidence
    price_std = recent['close'].diff().std()
    oi_std    = recent['oi'].diff().std()
    price_z   = abs(price_chg) / (price_std * window**0.5 + 1e-9)
    oi_z      = abs(oi_chg)    / (oi_std    * window**0.5 + 1e-9)
    conf      = min(1.0, (price_z + oi_z) / 4)

    if price_chg > 0 and oi_chg > 0:
        return 1, conf, f"long_buildup p={price_chg:+.0f} oi={oi_chg:+.0f}"
    elif price_chg < 0 and oi_chg > 0:
        return -1, conf, f"short_buildup p={price_chg:+.0f} oi={oi_chg:+.0f}"
    elif price_chg > 0 and oi_chg < 0:
        return 1, conf * 0.6, f"short_cover p={price_chg:+.0f} oi={oi_chg:+.0f}"
    else:
        return -1, conf * 0.6, f"long_unwind p={price_chg:+.0f} oi={oi_chg:+.0f}"


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 2 — Futures Basis (Contango/Backwardation)
# ═══════════════════════════════════════════════════════════════════════════════
def signal_futures_basis(futures_ltp: float, spot_ltp: float,
                          days_to_expiry: int,
                          contango_thresh: float = 0.40) -> tuple[int, float, str]:
    """
    Futures premium = (futures - spot) / spot * 100 annualized.
    Fair value premium (no-cost-of-carry) ≈ spot * r * (DTE/365), r≈8%.

    If actual premium >> fair → institutions aggressively long futures → LONG
    If actual premium << fair (backwardation) → aggressive short / hedging → SHORT

    contango_thresh: minimum basis% to fire as contango signal.
      Nifty default=0.40 (cost-of-carry noise sits at ~0.15–0.35%).
      BankNifty=0.15 (BN futures trade closer to fair; 0.15 is already meaningful).

    Returns (direction, confidence, note)
    """
    if spot_ltp <= 0 or days_to_expiry <= 0:
        return 0, 0.0, "missing spot/dte"

    raw_premium_pct = (futures_ltp - spot_ltp) / spot_ltp * 100
    fair_premium_pct = 8.0 * (days_to_expiry / 365) * 100 / 100  # annualized

    # Basis = actual - fair
    basis = raw_premium_pct - fair_premium_pct

    note = f"fut={futures_ltp:.0f} spot={spot_ltp:.0f} basis={basis:+.2f}%"

    if basis > contango_thresh:
        conf = min(1.0, basis / (contango_thresh * 1.5))
        return 1, conf, f"contango {note}"
    elif basis < -0.10:  # backwardation → FII short / aggressive hedge
        conf = min(1.0, abs(basis) / 0.3)
        return -1, conf, f"backwardation {note}"
    else:
        return 0, 0.0, f"neutral {note}"


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 3 — PCR Signal
# ═══════════════════════════════════════════════════════════════════════════════
def signal_pcr(pcr: float, pcr_5d_ma: float = None) -> tuple[int, float, str]:
    """
    PCR interpretation (trend-following, no contrarian logic):
      PCR < 0.7  → extreme call buying → very bullish → LONG
      PCR 0.7-0.9 → mild call-heavy → LONG
      PCR 0.9-1.1 → neutral
      PCR 1.1-1.4 → mild put-heavy → SHORT
      PCR > 1.4  → extreme put buying → very bearish → SHORT (NOT contrarian)

    Contrarian logic (pcr>1.4 = LONG) was removed: it fires on ordinary downtrends,
    not just panic bottoms. Without a VIX/RSI context filter, it adds noise.
    Use a consistent directional interpretation: high PCR = bearish, low PCR = bullish.
    """
    if pcr <= 0:
        return 0, 0.0, "no pcr"

    note = f"pcr={pcr:.2f}"
    if pcr_5d_ma:
        note += f" ma={pcr_5d_ma:.2f}"

    # Extreme ranges: higher confidence, same direction as mild ranges
    if pcr < 0.7:
        conf = min(1.0, (0.7 - pcr) / 0.2)
        return 1, conf, f"extreme_calls_bullish {note}"
    elif pcr > 1.4:
        conf = min(1.0, (pcr - 1.4) / 0.3)
        return -1, conf, f"extreme_puts_bearish {note}"
    # Trend-following middle
    elif 0.7 <= pcr < 0.9:
        return 1, 0.5, f"bullish_pcr {note}"
    elif 1.1 < pcr <= 1.4:
        return -1, 0.5, f"bearish_pcr {note}"
    else:
        return 0, 0.0, f"neutral_pcr {note}"


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 4 — OI Velocity (live, from options chain snapshots)
# ═══════════════════════════════════════════════════════════════════════════════
def signal_oi_velocity(velocity_data: dict, ltp: float,
                        band_pct: float = 0.05,
                        basis_pct: float = None) -> tuple[int, float, str]:
    """
    velocity_data: {strike: {ce_velocity, pe_velocity, net_velocity}} from options_chain.py

    Strategy: Look at strikes within band_pct of LTP.
    If net_velocity strongly positive (put writing > call writing) → LONG
    If net_velocity strongly negative (call writing > put writing) → SHORT

    basis_pct: futures basis as % of spot (positive = contango, negative = backwardation).
    Backwardation filter: suppress SHORT velocity when futures < spot.
    In backwardation, call writing is overwhelmingly covered-call hedging against long
    futures positions — not directional short positioning.  The signal cannot distinguish
    the two, so we discard it rather than fire on noise.
    LONG velocity (put writing) is unaffected: valid regardless of basis.
    """
    if not velocity_data or ltp <= 0:
        return 0, 0.0, "no velocity data"

    band = ltp * band_pct
    atm_strikes = {s: v for s, v in velocity_data.items()
                   if abs(s - ltp) <= band}

    if not atm_strikes:
        return 0, 0.0, "no ATM strikes in band"

    net_vels = [v['net_velocity'] for v in atm_strikes.values()]
    ce_vels  = [v['ce_velocity']  for v in atm_strikes.values()]
    pe_vels  = [v['pe_velocity']  for v in atm_strikes.values()]

    avg_net  = np.mean(net_vels)
    avg_ce   = np.mean(ce_vels)
    avg_pe   = np.mean(pe_vels)
    max_abs  = max(abs(avg_ce), abs(avg_pe)) + 1e-9

    note = f"atm_net_vel={avg_net:.1f} ce={avg_ce:.1f} pe={avg_pe:.1f}"

    # Threshold: meaningful velocity = >5 contracts/min on average.
    # 2.0 fired on ~79% of days, too noisy. 5.0 requires genuine institutional flow.
    threshold = 5.0
    if avg_net > threshold:
        conf = min(1.0, avg_net / (threshold * 3))
        return 1, conf, f"put_writing_dominant {note}"
    elif avg_net < -threshold:
        # Backwardation filter: call writing in backwardation is hedging, not directional.
        if basis_pct is not None and basis_pct < 0:
            return 0, 0.0, f"velocity_short_suppressed_backwardation basis={basis_pct:.2f}% {note}"
        conf = min(1.0, abs(avg_net) / (threshold * 3))
        return -1, conf, f"call_writing_dominant {note}"
    else:
        return 0, 0.0, f"balanced {note}"


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 5 — Strike Defense
# ═══════════════════════════════════════════════════════════════════════════════
def signal_strike_defense(walls: dict, ltp: float) -> tuple[int, float, str]:
    """
    Strike defense: price approaching a wall where OI is NOT unwinding
    (institutions defending the level).

    - Price near call_wall + call OI stable/growing → resistance → SHORT
    - Price near put_wall + put OI stable/growing → support → LONG
    - Price breaking through wall → momentum in that direction

    walls: output from detect_strike_walls()
    """
    if not walls or ltp <= 0:
        return 0, 0.0, "no walls"

    call_wall = walls.get('call_wall')
    put_wall  = walls.get('put_wall')

    if call_wall is None or put_wall is None:
        return 0, 0.0, "walls not detected"

    range_size = call_wall - put_wall
    if range_size <= 0:
        return 0, 0.0, "invalid wall range"

    dist_from_call = (call_wall - ltp) / range_size
    dist_from_put  = (ltp - put_wall) / range_size
    pos_in_range   = (ltp - put_wall) / range_size  # 0=at put wall, 1=at call wall

    note = f"ltp={ltp:.0f} call={call_wall} put={put_wall} pos={pos_in_range:.2f}"

    # Near call wall (within 15% of range) → resistance, SHORT
    if dist_from_call < 0.15:
        conf = min(1.0, (0.15 - dist_from_call) / 0.15)
        return -1, conf, f"near_call_wall {note}"
    # Near put wall (within 15% of range) → support, LONG
    elif dist_from_put < 0.15:
        conf = min(1.0, (0.15 - dist_from_put) / 0.15)
        return 1, conf, f"near_put_wall {note}"
    # Broken above call wall → momentum LONG
    elif ltp > call_wall:
        conf = min(1.0, (ltp - call_wall) / (range_size * 0.1))
        return 1, conf, f"above_call_wall {note}"
    # Broken below put wall → momentum SHORT
    elif ltp < put_wall:
        conf = min(1.0, (put_wall - ltp) / (range_size * 0.1))
        return -1, conf, f"below_put_wall {note}"
    # Middle of range
    else:
        # Slightly bias toward put wall (institutional support typically stronger)
        if pos_in_range < 0.4:
            return 1, 0.3, f"lower_range_bullish {note}"
        elif pos_in_range > 0.6:
            return -1, 0.3, f"upper_range_bearish {note}"
        return 0, 0.0, f"mid_range {note}"


# ═══════════════════════════════════════════════════════════════════════════════
# SIGNAL 6 — FII/DII Signature Detector (real-time classifier path)
# ═══════════════════════════════════════════════════════════════════════════════
def signal_fii_signature(
    df_1m: pd.DataFrame,
    fii_fut_level: int,           # lag-1 fallback: 1/−1/0 from participant OI
    fii_cash_lag1: int,           # lag-1 fallback: 1/−1/0
    velocity_data: dict,
    ltp: float,
    volume_window: int = 10,
    fii_dii_result: Optional[dict] = None,   # real-time: from FIIDIIClassifier
) -> tuple[int, float, str]:
    """
    FII/DII signature signal.

    Priority:
      1. If fii_dii_result is provided (real-time path):
         Use the classifier's attribution + confidence directly.
         Also incorporate volume sweep + OI velocity as confirmations.

      2. Fallback (backtest / no classifier path):
         Use lag-1 fii_fut_level + fii_cash_lag1 + candle velocity analysis.

    FII_BULL attribution  → signal = +1 (institutions adding call OI)
    FII_BEAR attribution  → signal = −1 (institutions adding put OI)
    DII_BULL / DII_BEAR   → signal = ±1 at 0.7× confidence (slower, hedging)
    RETAIL / MIXED        → signal = 0

    OI velocity modulates the confidence:
      If FII_BULL + positive net velocity → confidence ×1.5 (capped at 1.0)
      If RETAIL attribution              → confidence ×0.3
    """
    signals = []
    notes   = []

    # ── Path 1: Real-time FII/DII classifier ─────────────────────────────────
    if fii_dii_result is not None:
        attribution = fii_dii_result.get('attribution', 'UNKNOWN')
        clf_dir     = fii_dii_result.get('direction', 0)
        clf_conf    = float(fii_dii_result.get('confidence', 0.0))
        retail_sc   = float(fii_dii_result.get('retail_score', 0.5))

        if attribution in ('FII_BULL', 'FII_BEAR'):
            signals.append(clf_dir)
            notes.append(f"classifier={attribution} conf={clf_conf:.2f}")
        elif attribution in ('DII_BULL', 'DII_BEAR'):
            # DII moves are real but slower; down-weight confidence
            signals.append(clf_dir)
            clf_conf *= 0.7
            notes.append(f"classifier={attribution} conf={clf_conf:.2f}")
        elif attribution == 'RETAIL' or retail_sc > 0.7:
            # Retail noise — suppress the signal
            return 0, 0.0, f"classifier=RETAIL retail_score={retail_sc:.2f}"
        # MIXED / UNKNOWN — fall through to candle velocity check below

        # OI velocity as confirmation/modifier
        if velocity_data and ltp > 0:
            band = ltp * 0.03
            atm  = {s: v for s, v in velocity_data.items() if abs(s - ltp) <= band}
            if atm:
                net = float(np.mean([v['net_velocity'] for v in atm.values()]))
                if abs(net) > 5:
                    vel_dir = 1 if net > 0 else -1
                    if vel_dir == clf_dir:
                        clf_conf = min(1.0, clf_conf * 1.5)
                        notes.append(f"oi_vel_confirms net={net:.1f}")
                    else:
                        clf_conf *= 0.6
                        notes.append(f"oi_vel_conflicts net={net:.1f}")

        if signals:
            direction = signals[0]
            return direction, min(1.0, clf_conf), f"FII_RT [{', '.join(notes)}]"
        # If MIXED/UNKNOWN from classifier, fall through to candle velocity below

    # ── Path 2: Lag-1 fallback ────────────────────────────────────────────────
    if fii_fut_level != 0:
        signals.append(fii_fut_level)
        notes.append(f"fut_level={'LONG' if fii_fut_level>0 else 'SHORT'}")

    if fii_cash_lag1 != 0:
        signals.append(fii_cash_lag1)
        notes.append(f"cash={'BUY' if fii_cash_lag1>0 else 'SELL'}")

    # Candle velocity signature (large blocks = institutional sweep)
    if df_1m is not None and len(df_1m) >= volume_window:
        recent     = df_1m.tail(volume_window)
        avg_vol    = df_1m['volume'].mean()
        recent_vol = recent['volume'].mean()
        vol_ratio  = recent_vol / (avg_vol + 1e-9)

        price_moves = recent['close'].diff().abs()
        avg_move    = df_1m['close'].diff().abs().mean()
        move_ratio  = price_moves.mean() / (avg_move + 1e-9)

        if vol_ratio > 1.8 and move_ratio > 1.5:
            trend     = recent['close'].iloc[-1] - recent['close'].iloc[0]
            dir_sweep = 1 if trend > 0 else -1
            signals.append(dir_sweep)
            notes.append(
                f"institutional_sweep vol={vol_ratio:.1f}x move={move_ratio:.1f}x"
            )

    # OI velocity convergence
    if velocity_data and ltp > 0:
        band = ltp * 0.03
        atm  = {s: v for s, v in velocity_data.items() if abs(s - ltp) <= band}
        if atm:
            net = np.mean([v['net_velocity'] for v in atm.values()])
            if abs(net) > 3:
                signals.append(1 if net > 0 else -1)
                notes.append(f"oi_velocity={net:.1f}")

    if not signals:
        return 0, 0.0, "no fii signals"

    positive = sum(1 for s in signals if s > 0)
    negative = sum(1 for s in signals if s < 0)
    total    = len(signals)

    if positive > negative and positive >= max(2, total * 0.6):
        return 1, positive / total, f"FII_LONG [{', '.join(notes)}]"
    elif negative > positive and negative >= max(2, total * 0.6):
        return -1, negative / total, f"FII_SHORT [{', '.join(notes)}]"
    else:
        return 0, 0.3, f"FII_mixed [{', '.join(notes)}]"


# ═══════════════════════════════════════════════════════════════════════════════
# COMBINER
# ═══════════════════════════════════════════════════════════════════════════════
def compute_signal_state(
    df_1m: pd.DataFrame,
    futures_ltp: float,
    spot_ltp: float,
    days_to_expiry: int,
    pcr: float,
    pcr_5d_ma: float,
    velocity_data: dict,
    walls: dict,
    fii_fut_level: int,
    fii_cash_lag1: int,
    timestamp: pd.Timestamp = None,
    # ── Real-time FII/DII classifier result (optional) ────────────────────────
    fii_dii_result: Optional[dict] = None,   # from FIIDIIClassifier.classify()
    # ── Optional extra-signal inputs (all default to None / False) ────────────
    df_strikes: pd.DataFrame = None,      # bhavcopy OI table → max pain signal
    leader_candles: pd.DataFrame = None,  # leading index 1m candles → cross-index
    is_expiry: bool = False,              # True on expiry day → expiry reversal
    max_pain_strike: int = None,          # precomputed max pain (skips recompute)
    current_bar_idx: int = None,          # 0-based bar index for intraday signals
    contango_thresh: float = 0.40,        # instrument-specific contango threshold
) -> SignalState:
    """
    Compute all signals and combine into a SignalState.
    All inputs available at call time — no lookahead.

    Core 6 signals are always computed.
    Extra signals (7-9) are computed only when their required inputs are
    provided; if not provided they default to 0 (neutral) and do NOT affect
    existing backtest runs.

    Extra inputs:
        df_strikes:      DataFrame[strike, ce_oi, pe_oi] from bhavcopy.
                         Required for max_pain signal.
        leader_candles:  1m OHLCV for the leading index (e.g. BankNifty).
                         Required for cross_index signal.
        is_expiry:       Flag — True on expiry day.
                         Required (with max_pain_strike) for expiry_reversal.
        max_pain_strike: Precomputed max pain strike.  If None and df_strikes
                         is provided, max pain is computed from df_strikes.
        current_bar_idx: 0-based index of the current 1m bar.  Required for
                         expiry_reversal and cross_index.  If None, these
                         signals are skipped even if other inputs are present.
    """
    state = SignalState(timestamp=timestamp or pd.Timestamp.now())
    state.ltp = futures_ltp
    if walls:
        state.call_wall = walls.get('call_wall')
        state.put_wall  = walls.get('put_wall')
        state.pcr_live  = walls.get('pcr_live', 0)

    # ── Run core signals ─────────────────────────────────────────────────────
    s1, c1, n1 = signal_oi_quadrant(df_1m)
    s2, c2, n2 = signal_futures_basis(futures_ltp, spot_ltp, days_to_expiry, contango_thresh)
    s3, c3, n3 = signal_pcr(pcr, pcr_5d_ma)
    _basis_pct = (futures_ltp - spot_ltp) / spot_ltp * 100.0 if spot_ltp > 0 else None
    s4, c4, n4 = signal_oi_velocity(velocity_data, futures_ltp, basis_pct=_basis_pct)
    s5, c5, n5 = signal_strike_defense(walls, futures_ltp)
    s6, c6, n6 = signal_fii_signature(
        df_1m, fii_fut_level, fii_cash_lag1,
        velocity_data, futures_ltp,
        fii_dii_result=fii_dii_result,
    )

    state.oi_quadrant,   state.oi_quadrant_conf   = s1, c1
    state.futures_basis, state.futures_basis_conf  = s2, c2
    state.pcr,           state.pcr_conf            = s3, c3
    state.oi_velocity,   state.oi_velocity_conf    = s4, c4
    state.strike_defense,state.strike_defense_conf = s5, c5
    state.fii_signature, state.fii_signature_conf  = s6, c6

    state.notes = [n1, n2, n3, n4, n5, n6]

    # ── Run extra signals (only when inputs are provided) ────────────────────
    s7, c7, n7 = 0, 0.0, "max_pain: not computed (no df_strikes)"
    s8, c8, n8 = 0, 0.0, "expiry_reversal: not computed"
    s9, c9, n9 = 0, 0.0, "cross_index: not computed (no leader_candles)"

    # Signal 7: Max Pain
    if df_strikes is not None:
        try:
            s7, c7, n7 = signal_max_pain(df_strikes, spot_ltp)
            # Resolve max_pain_strike for subsequent signals if not precomputed
            if max_pain_strike is None:
                from v3.signals.max_pain import compute_max_pain
                mp_result = compute_max_pain(df_strikes, spot_ltp)
                max_pain_strike = mp_result['max_pain_strike']
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"compute_signal_state: max_pain signal failed — {exc}"
            ) from exc

    state.max_pain      = s7
    state.max_pain_conf = c7
    state.notes.append(n7)

    # Signal 8: Expiry Reversal
    if is_expiry and max_pain_strike is not None and df_1m is not None and current_bar_idx is not None:
        try:
            spot_open = float(df_1m['open'].iloc[0]) if 'open' in df_1m.columns else spot_ltp
            s8, c8, n8 = compute_expiry_reversal_signal(
                candles_1m      = df_1m,
                max_pain_strike = max_pain_strike,
                spot_open       = spot_open,
                current_bar_idx = current_bar_idx,
            )
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"compute_signal_state: expiry_reversal signal failed — {exc}"
            ) from exc

    state.expiry_reversal      = s8
    state.expiry_reversal_conf = c8
    state.notes.append(n8)

    # Signal 9: Cross-Index
    if leader_candles is not None and df_1m is not None and current_bar_idx is not None:
        try:
            s9, c9, n9 = compute_cross_index_signal(
                leader_candles  = leader_candles,
                lagger_candles  = df_1m,
                current_bar_idx = current_bar_idx,
            )
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"compute_signal_state: cross_index signal failed — {exc}"
            ) from exc

    state.cross_index      = s9
    state.cross_index_conf = c9
    state.notes.append(n9)

    # ── Weighted score (core signals) ─────────────────────────────────────────
    core_signals = {
        'oi_quadrant':    (s1, c1),
        'futures_basis':  (s2, c2),
        'pcr':            (s3, c3),
        'oi_velocity':    (s4, c4),
        'strike_defense': (s5, c5),
        'fii_signature':  (s6, c6),
    }

    score   = 0.0
    total_w = 0.0
    fired   = 0
    for name, (direction, confidence) in core_signals.items():
        w = WEIGHTS[name]
        if direction != 0:
            score   += direction * confidence * w
            total_w += w
            fired   += 1

    # ── Add extra signals to the weighted pool ────────────────────────────────
    extra_signals = {
        'max_pain':        (s7, c7),
        'expiry_reversal': (s8, c8),
        'cross_index':     (s9, c9),
    }
    for name, (direction, confidence) in extra_signals.items():
        w = WEIGHTS_EXTRA[name]
        if direction != 0:
            score   += direction * confidence * w
            total_w += w
            fired   += 1

    state.score        = score / total_w if total_w > 0 else 0.0
    state.signal_count = fired

    # Direction threshold: |score| > 0.35 to fire (raised from 0.30)
    if state.score >= 0.35 and fired >= 2:
        state.direction = 1
    elif state.score <= -0.35 and fired >= 2:
        state.direction = -1
    else:
        state.direction = 0

    return state


def state_to_dict(state: SignalState) -> dict:
    """Serialize SignalState to flat dict for logging/CSV."""
    return {
        'ts':                    str(state.timestamp),
        'ltp':                   state.ltp,
        'direction':             state.direction,
        'score':                 round(state.score, 4),
        'signal_count':          state.signal_count,
        # Core signals
        'oi_quadrant':           state.oi_quadrant,
        'futures_basis':         state.futures_basis,
        'pcr_signal':            state.pcr,
        'oi_velocity':           state.oi_velocity,
        'strike_defense':        state.strike_defense,
        'fii_signature':         state.fii_signature,
        # Extra signals
        'max_pain':              state.max_pain,
        'max_pain_conf':         round(state.max_pain_conf, 4),
        'expiry_reversal':       state.expiry_reversal,
        'expiry_reversal_conf':  round(state.expiry_reversal_conf, 4),
        'cross_index':           state.cross_index,
        'cross_index_conf':      round(state.cross_index_conf, 4),
        # Context
        'call_wall':             state.call_wall,
        'put_wall':              state.put_wall,
        'pcr_live':              round(state.pcr_live, 3),
        'notes':                 ' | '.join(state.notes),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SignalSmoother — EMA noise filter for the combined score
# ═══════════════════════════════════════════════════════════════════════════════

class SignalSmoother:
    """
    EMA smoother over the SignalState combined score.

    Fires a directional signal only when:
      1. |smoothed_score| > threshold (default 0.30)
      2. Same direction as the previous bar (persistence ≥ 2 bars)
      3. Score is not weakening (|current| ≥ |previous| × 0.8)

    Usage
    -----
    smoother = SignalSmoother()
    smoothed_dir = smoother.update(state)   # returns +1 / −1 / 0

    The smoother does NOT mutate the SignalState; it returns the smoothed
    direction separately. The caller decides whether to override state.direction.
    """

    def __init__(
        self,
        alpha: float = 0.4,        # EMA decay — higher = faster response
        threshold: float = 0.30,   # |score| must exceed this to fire
        min_persist: int = 2,      # consecutive same-direction bars needed
    ):
        if not (0 < alpha <= 1):
            raise ValueError(f"SignalSmoother: alpha must be in (0,1], got {alpha}")
        if threshold <= 0:
            raise ValueError(f"SignalSmoother: threshold must be > 0, got {threshold}")
        if min_persist < 1:
            raise ValueError(f"SignalSmoother: min_persist must be ≥ 1, got {min_persist}")

        self.alpha       = alpha
        self.threshold   = threshold
        self.min_persist = min_persist

        self._ema:        float = 0.0   # running EMA of score
        self._prev_ema:   float = 0.0
        self._streak:     int   = 0     # consecutive bars in same direction
        self._streak_dir: int   = 0     # direction of current streak

    def update(self, state: SignalState) -> int:
        """
        Feed a new SignalState. Returns smoothed direction (+1 / −1 / 0).

        The raw state.score drives the EMA. Direction fires only when
        persistence and strength thresholds are met.
        """
        raw_score = float(state.score)

        # EMA update
        self._prev_ema = self._ema
        self._ema      = self.alpha * raw_score + (1.0 - self.alpha) * self._ema

        ema  = self._ema
        prev = self._prev_ema

        # Determine raw direction from smoothed score
        if ema >= self.threshold:
            raw_dir = 1
        elif ema <= -self.threshold:
            raw_dir = -1
        else:
            raw_dir = 0

        # Streak tracking
        if raw_dir == 0:
            self._streak     = 0
            self._streak_dir = 0
            return 0

        if raw_dir == self._streak_dir:
            self._streak += 1
        else:
            self._streak     = 1
            self._streak_dir = raw_dir

        # Persistence check
        if self._streak < self.min_persist:
            return 0

        # Weakening filter: don't fire if score is losing momentum
        if abs(ema) < abs(prev) * 0.8:
            log.debug(
                "SignalSmoother: suppressed weakening signal "
                "ema=%.3f prev_ema=%.3f dir=%+d",
                ema, prev, raw_dir,
            )
            return 0

        return raw_dir

    def reset(self) -> None:
        """Reset EMA and streak. Call at start of each trading day."""
        self._ema        = 0.0
        self._prev_ema   = 0.0
        self._streak     = 0
        self._streak_dir = 0
