"""
v3/signals/fii_dii_classifier_BANKNIFTY.py
=====================================
FII/DII Activity Classifier — BankNifty variant
Calibrated on BankNifty OI data only. Use for BN-specific backtest/live.

Two classes:
  FIIDIICalibrator  — offline: learns feature thresholds from 20 days of
                       April 1m option OI data, using bhavcopy net OI delta
                       as weak day-level labels. Saves thresholds to JSON.

  FIIDIIClassifier  — runtime: rolling buffer of OI snapshots, classifies
                       each bar using calibrated thresholds.

Features (15-bar rolling aggregates across FULL option chain):
  1. ce_pe_imbalance     — (ΔTOT_CE - ΔTOT_PE) / (|ΔTOT_CE| + |ΔTOT_PE| + 1)
                           Full chain net OI change ratio. Range -1 to +1.
                           FII_BULL ≫ 0, FII_BEAR ≪ 0.
  2. strike_coverage     — # distinct strikes with nonzero ΔOI / total_strikes.
                           Low = concentrated institutional bet. High = retail scatter.
  3. atm_build_rate      — Total |ΔOI| at ATM±200 / (total |ΔOI| chain + 1).
                           Institutional tends to cluster near ATM.
  4. oi_add_intensity    — Total |ΔOI| chain in window / (running daily avg × 15 + 1).
                           High = above-average activity = institutional sweep.
  5. basis_momentum      — (futures_close_now - futures_close_15_bars_ago) / spot.
                           Futures lead: FII_BULL drives contango.
  6. ce_skew_shift       — Σ(ΔCE_close) OTM CE strikes over window.
                           Premium expansion in OTM calls = call buying.
  7. pe_skew_shift       — Σ(ΔPE_close) OTM PE strikes over window.
                           Premium expansion in OTM puts = put buying / hedging.

Weak labels (day-level, from bhavcopy):
  bhavcopy net = (Σ CE_OI_change) - (Σ PE_OI_change) over near-money band.
  Top quartile  → FII_BULL  (institutions aggressively buying calls)
  Bottom quartile → FII_BEAR  (institutions aggressively buying puts)
  Middle 50%    → DII_MIXED  (DII hedging + retail noise)

Threshold calibration:
  For each feature: compute distribution per label.
  Store label centroids (median) + spread (std) → used for Gaussian log-likelihood
  scoring at runtime.

Cache:
  v3/cache/fii_dii_thresholds_BANKNIFTY.json

Usage:
  # Offline — run once after fetching data
  from v3.signals.fii_dii_classifier import FIIDIICalibrator
  FIIDIICalibrator().calibrate()

  # Runtime — call every minute
  from v3.signals.fii_dii_classifier import FIIDIIClassifier, OISnapshot
  clf = FIIDIIClassifier()
  clf.push(snapshot)
  result = clf.classify()
  # result = {fii_score, dii_score, retail_score, attribution,
  #           direction, confidence, features}
"""
from __future__ import annotations

import json
import logging
import pickle
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger('v3.fii_dii_bn')

ROOT            = Path(__file__).resolve().parents[2]
OI_CACHE        = ROOT / 'v3' / 'cache' / 'option_oi_1m_BANKNIFTY.pkl'
FUTURES_CACHE   = ROOT / 'v3' / 'cache' / 'candles_1m_BANKNIFTY.pkl'
SPOT_CACHE      = ROOT / 'v3' / 'cache' / 'candles_1m_spot_BANKNIFTY.pkl'
BHAVCOPY_CACHE  = ROOT / 'v3' / 'cache' / 'bhavcopy_BN_all.pkl'
THRESHOLDS_FILE = ROOT / 'v3' / 'cache' / 'fii_dii_thresholds_BANKNIFTY.json'

STRIKE_STEP   = 100
ATM_BAND      = 400    # ±400 pts = 4 strikes for ATM build rate (BN 100pt intervals)
OTM_MIN       = 600    # OTM price shift starts at +600 from ATM (BN ~55k)
OTM_MAX       = 1600
WINDOW        = 15     # rolling window in bars for feature extraction
MIN_WINDOW    = 5      # minimum bars needed before classifying
BUFFER_BARS   = 30     # max history kept in runtime buffer


# ── Attribution labels ────────────────────────────────────────────────────────
FII_BULL  = 'FII_BULL'
FII_BEAR  = 'FII_BEAR'
DII_BULL  = 'DII_BULL'
DII_BEAR  = 'DII_BEAR'
RETAIL    = 'RETAIL'
MIXED     = 'MIXED'
UNKNOWN   = 'UNKNOWN'

# Feature names — must match exactly between calibrator and classifier
FEATURE_NAMES = [
    'ce_pe_imbalance',
    'strike_coverage',
    'atm_build_rate',
    'oi_add_intensity',
    'basis_momentum',
    'ce_skew_shift',
    'pe_skew_shift',
    'fii_cash_context',  # lag-1 5-day FII cash flow / 20000 — disambiguates
                         # "put writing in uptrend" (FII_BULL) from
                         # "put writing in downtrend" (defensive, DII_MIXED/BEAR).
                         # Available at day open: no lookahead.
]


# ═══════════════════════════════════════════════════════════════════════════════
# Feature computation
# ═══════════════════════════════════════════════════════════════════════════════

def compute_features(
    ce_oi_window:    dict,   # {strike: [oi_t0, oi_t1, ..., oi_tN]}  length=window
    pe_oi_window:    dict,   # same
    ce_close_window: dict,   # {strike: [close_t0, ..., close_tN]}
    pe_close_window: dict,
    fut_close_window: list,  # [fut_t0, ..., fut_tN]
    atm_strike:      int,
    daily_avg_oi_add: float = 1.0,   # normaliser for oi_add_intensity
    fii_cash_5d_norm: float = 0.0,   # lag-1 sum(FII cash, prior 5 days) / 20000
                                     # +ve = institutional buying, -ve = selling
                                     # clipped to [-3, +3] inside feature
) -> Optional[dict]:
    """
    Compute all 7 features over the provided window.

    All windows must have the same length. Returns None if data is
    insufficient (< MIN_WINDOW bars) or all-NaN.

    Parameters
    ----------
    ce_oi_window / pe_oi_window : dict of strike → list of OI values (ffilled).
    ce_close_window / pe_close_window : dict of strike → list of LTP values.
    fut_close_window : list of futures close prices.
    atm_strike : nearest strike to current spot.
    daily_avg_oi_add : running average of per-bar total-chain OI additions
                       (used to normalise oi_add_intensity).
    """
    strikes   = sorted(set(ce_oi_window.keys()) | set(pe_oi_window.keys()))
    n_strikes = len(strikes)
    if n_strikes == 0:
        return None

    w = len(fut_close_window)
    if w < MIN_WINDOW:
        return None

    # ── Per-strike OI change over the window ─────────────────────────────
    ce_delta: dict = {}
    pe_delta: dict = {}
    for s in strikes:
        ce_series = ce_oi_window.get(s, [])
        pe_series = pe_oi_window.get(s, [])
        if len(ce_series) >= 2:
            first = next((v for v in ce_series if not np.isnan(v)), 0.0)
            last  = next((v for v in reversed(ce_series) if not np.isnan(v)), first)
            ce_delta[s] = float(last - first)
        else:
            ce_delta[s] = 0.0
        if len(pe_series) >= 2:
            first = next((v for v in pe_series if not np.isnan(v)), 0.0)
            last  = next((v for v in reversed(pe_series) if not np.isnan(v)), first)
            pe_delta[s] = float(last - first)
        else:
            pe_delta[s] = 0.0

    tot_ce = sum(ce_delta.values())
    tot_pe = sum(pe_delta.values())
    tot_abs_ce = sum(abs(v) for v in ce_delta.values())
    tot_abs_pe = sum(abs(v) for v in pe_delta.values())

    # ── 1. ce_pe_imbalance ───────────────────────────────────────────────
    denom_cp = tot_abs_ce + tot_abs_pe + 1.0
    ce_pe_imbalance = float((tot_ce - tot_pe) / denom_cp)

    # ── 2. strike_coverage ───────────────────────────────────────────────
    active_strikes = sum(
        1 for s in strikes
        if abs(ce_delta.get(s, 0.0)) + abs(pe_delta.get(s, 0.0)) > 0
    )
    strike_coverage = float(active_strikes / max(n_strikes, 1))

    # ── 3. atm_build_rate ────────────────────────────────────────────────
    atm_strikes = [s for s in strikes if abs(s - atm_strike) <= ATM_BAND]
    atm_oi_abs  = sum(
        abs(ce_delta.get(s, 0.0)) + abs(pe_delta.get(s, 0.0))
        for s in atm_strikes
    )
    total_oi_abs = tot_abs_ce + tot_abs_pe + 1.0
    atm_build_rate = float(atm_oi_abs / total_oi_abs)

    # ── 4. oi_add_intensity ──────────────────────────────────────────────
    # Total absolute OI added in this window vs daily running average
    oi_add_intensity = float(total_oi_abs / (daily_avg_oi_add * w + 1.0))

    # ── 5. basis_momentum ────────────────────────────────────────────────
    fut_clean = [v for v in fut_close_window if v and not np.isnan(v)]
    if len(fut_clean) >= 2:
        basis_momentum = float(
            (fut_clean[-1] - fut_clean[0]) / (fut_clean[0] + 1e-6) * 100
        )
    else:
        basis_momentum = 0.0

    # ── 6/7. ce_skew_shift / pe_skew_shift ───────────────────────────────
    # OTM call/put price change over window
    otm_ce_strikes = [s for s in strikes
                      if OTM_MIN <= (s - atm_strike) <= OTM_MAX]
    otm_pe_strikes = [s for s in strikes
                      if OTM_MIN <= (atm_strike - s) <= OTM_MAX]

    def _price_shift(close_window: dict, ss: list) -> float:
        shifts = []
        for s in ss:
            series = close_window.get(s, [])
            if len(series) < 2:
                continue
            first = next((v for v in series if v and not np.isnan(v)), None)
            last  = next((v for v in reversed(series) if v and not np.isnan(v)), None)
            if first is not None and last is not None and first > 0:
                shifts.append((last - first) / first * 100)
        return float(np.mean(shifts)) if shifts else 0.0

    ce_skew_shift = _price_shift(ce_close_window, otm_ce_strikes)
    pe_skew_shift = _price_shift(pe_close_window, otm_pe_strikes)

    # ── 8. fii_cash_context ──────────────────────────────────────────────
    # Lag-1 5-day FII cash normalised by 20000 crore (rough full-cycle range).
    # Key disambiguator: same intraday OI patterns (put writing, low PCR) appear
    # in both FII_BULL days (FII buying cash) and defensive put-writing days in
    # downtrends (FII selling cash).  Without this feature the classifier cannot
    # tell them apart.  Clipped to [-3, +3] to bound z-score.
    fii_cash_context = float(np.clip(fii_cash_5d_norm, -3.0, 3.0))

    feat = {
        'ce_pe_imbalance':  ce_pe_imbalance,
        'strike_coverage':  strike_coverage,
        'atm_build_rate':   atm_build_rate,
        'oi_add_intensity': oi_add_intensity,
        'basis_momentum':   basis_momentum,
        'ce_skew_shift':    ce_skew_shift,
        'pe_skew_shift':    pe_skew_shift,
        'fii_cash_context': fii_cash_context,
    }

    # Sanity: all finite
    for k, v in feat.items():
        if not np.isfinite(v):
            feat[k] = 0.0

    return feat


# ═══════════════════════════════════════════════════════════════════════════════
# FIIDIICalibrator
# ═══════════════════════════════════════════════════════════════════════════════

class FIIDIICalibrator:
    """
    Offline calibrator. Loads cached 1m option OI + futures + bhavcopy data,
    extracts 15-bar rolling window features per day, assigns weak labels from
    bhavcopy net OI delta, fits per-label Gaussian parameters, writes JSON.
    """

    def __init__(self, training_days: Optional[list] = None):
        """
        Parameters
        ----------
        training_days : list of 'YYYY-MM-DD' strings.
                        If None, uses all days in the OI cache.
        """
        self.training_days = training_days

    # ── Data loading ──────────────────────────────────────────────────────────

    def _load_caches(self) -> tuple:
        for path, name in [
            (OI_CACHE,       'Option OI cache (fetch_option_oi_BANKNIFTY.py)'),
            (FUTURES_CACHE,  'Futures 1m cache (fetch_1m_BANKNIFTY.py)'),
            (BHAVCOPY_CACHE, 'Bhavcopy cache (fetch_bhavcopy_bn.py)'),
        ]:
            if not path.exists():
                raise FileNotFoundError(
                    f"{name} not found: {path}. Run the fetcher first."
                )

        with open(OI_CACHE,       'rb') as f: oi_cache = pickle.load(f)
        with open(FUTURES_CACHE,  'rb') as f: fut_df   = pickle.load(f)
        with open(BHAVCOPY_CACHE, 'rb') as f: bhavcopy = pickle.load(f)

        spot_df = None
        if SPOT_CACHE.exists():
            with open(SPOT_CACHE, 'rb') as f: spot_df = pickle.load(f)

        # FII daily cash flow — optional (degrades gracefully if not present)
        fii_cash_df = None
        fii_cash_file = ROOT / 'fii_data.csv'
        if fii_cash_file.exists():
            fii_cash_df = pd.read_csv(fii_cash_file)
            fii_cash_df['date'] = pd.to_datetime(fii_cash_df['date']).dt.date
            log.info("FII cash data loaded: %d rows", len(fii_cash_df))
        else:
            log.warning(
                "fii_data.csv not found at %s — fii_cash_context feature will be 0.0 "
                "(classifier will have reduced discriminative power for FII selling regimes)",
                fii_cash_file,
            )

        return oi_cache, fut_df, spot_df, bhavcopy, fii_cash_df

    # ── Weak labels ───────────────────────────────────────────────────────────

    def _compute_day_labels(self, bhavcopy: dict, days: list,
                            fut_df: Optional[pd.DataFrame] = None) -> dict:
        """
        Returns {date_str: 'FII_BULL' | 'FII_BEAR' | 'DII_MIXED'}.

        Label strategy (price-direction primary):
          Uses the day's actual futures return (open → close) as the label.
          This directly represents what we are training the classifier to predict.
          Threshold: >+0.3% = FII_BULL, <-0.3% = FII_BEAR, in between = DII_MIXED.

          Rationale: bhavcopy OI-delta labels (old approach) assigned FII_BULL to
          down-days when put writing was active but the market still fell — causing
          the classifier to learn "put writing ≈ bullish", which is wrong in
          trending-down regimes.  Price-outcome labels ensure the classifier learns
          the OI features that ACCOMPANY market direction, not just options activity.

          Falls back to bhavcopy-delta quartile labelling if fut_df is unavailable.
        """
        # ── Price-direction labels (primary) ────────────────────────────────
        if fut_df is not None and not fut_df.empty:
            labels: dict = {}
            BULL_THRESH =  0.003   # > +0.3% = directionally up
            BEAR_THRESH = -0.003   # < -0.3% = directionally down

            for day in days:
                day_dt  = pd.to_datetime(day).date()
                day_fut = fut_df[fut_df['date'] == day_dt].sort_values('ts')
                if day_fut.empty or len(day_fut) < 2:
                    labels[day] = 'DII_MIXED'
                    continue
                open_px  = float(day_fut['open'].iloc[0])
                close_px = float(day_fut['close'].iloc[-1])
                if open_px <= 0:
                    labels[day] = 'DII_MIXED'
                    continue
                ret = (close_px - open_px) / open_px
                if ret > BULL_THRESH:
                    labels[day] = 'FII_BULL'
                elif ret < BEAR_THRESH:
                    labels[day] = 'FII_BEAR'
                else:
                    labels[day] = 'DII_MIXED'

            counts = {l: sum(1 for v in labels.values() if v == l)
                      for l in ['FII_BULL', 'FII_BEAR', 'DII_MIXED']}
            log.info(
                "Price-direction labels (open→close ±0.3%%): %s", counts,
            )
            return labels

        # ── Fallback: bhavcopy OI-delta quartile labelling (legacy) ─────────
        log.warning("fut_df not available — falling back to bhavcopy OI-delta labels")
        deltas: dict = {}
        prev_ce, prev_pe = None, None

        for day in sorted(bhavcopy.keys()):
            df = bhavcopy[day]
            if 'strike' not in df.columns:
                continue
            near = df[(df['strike'] >= 22000) & (df['strike'] <= 28000)]
            tot_ce = float(near['ce_oi'].sum())
            tot_pe = float(near['pe_oi'].sum())

            if prev_ce is not None and day in days:
                deltas[day] = (tot_ce - prev_ce) - (tot_pe - prev_pe)

            prev_ce, prev_pe = tot_ce, tot_pe

        if not deltas:
            log.warning("No bhavcopy deltas computed. Defaulting all to DII_MIXED.")
            return {d: 'DII_MIXED' for d in days}

        vals = np.array(list(deltas.values()))
        q75  = float(np.percentile(vals, 75))
        q25  = float(np.percentile(vals, 25))

        labels = {}
        for d in days:
            if d not in deltas:
                labels[d] = 'DII_MIXED'
            elif deltas[d] >= q75:
                labels[d] = 'FII_BULL'
            elif deltas[d] <= q25:
                labels[d] = 'FII_BEAR'
            else:
                labels[d] = 'DII_MIXED'

        counts = {l: sum(1 for v in labels.values() if v == l)
                  for l in ['FII_BULL', 'FII_BEAR', 'DII_MIXED']}
        log.info(
            "Bhavcopy OI-delta labels: %s  (q25=%.0f q75=%.0f)",
            counts, q25, q75,
        )
        return labels

    # ── Day feature extraction ────────────────────────────────────────────────

    def _extract_day_features(
        self,
        day:              str,
        oi_cache:         dict,
        fut_df:           pd.DataFrame,
        fii_cash_5d_norm: float = 0.0,  # lag-1 normalised FII cash for this day
    ) -> list[dict]:
        """
        Extract one feature vector per WINDOW-bar step across the trading day.
        Uses a sliding window (step=1) for maximum sample count.
        """
        if day not in oi_cache:
            return []

        day_oi  = oi_cache[day]
        strikes = sorted(day_oi.keys())

        day_dt  = pd.to_datetime(day).date()
        day_fut = fut_df[fut_df['date'] == day_dt].sort_values('ts').reset_index(drop=True)
        if day_fut.empty:
            return []

        # Spot proxy from first bar
        spot_proxy = float(day_fut['close'].iloc[0])
        atm_strike = int(round(spot_proxy / STRIKE_STEP) * STRIKE_STEP)

        # Build per-strike OI + close series aligned to futures index
        n_bars = len(day_fut)

        def _align_series(df_s: pd.DataFrame, col: str) -> np.ndarray:
            """Align a per-strike series to the futures bar count."""
            if df_s is None or df_s.empty or col not in df_s.columns:
                return np.full(n_bars, np.nan)
            arr = df_s[col].values.astype(float)
            if len(arr) >= n_bars:
                return arr[:n_bars]
            # Pad with last value
            pad = np.full(n_bars, arr[-1] if len(arr) > 0 else np.nan)
            pad[:len(arr)] = arr
            return pad

        ce_oi_all:    dict = {}
        pe_oi_all:    dict = {}
        ce_close_all: dict = {}
        pe_close_all: dict = {}

        for strike in strikes:
            ce_df = day_oi[strike].get('CE')
            pe_df = day_oi[strike].get('PE')
            ce_oi_all[strike]    = _align_series(ce_df, 'oi')
            pe_oi_all[strike]    = _align_series(pe_df, 'oi')
            ce_close_all[strike] = _align_series(ce_df, 'close')
            pe_close_all[strike] = _align_series(pe_df, 'close')

        fut_close = day_fut['close'].values.astype(float)

        # Pre-compute per-bar total chain |ΔOI| for running-avg normaliser.
        # IMPORTANT: use RUNNING average (bars 0..end-1) not full-day average.
        # Rationale: at runtime the classifier only has bars seen so far;
        # using the full-day average in calibration creates a distribution shift
        # between training features and inference features.
        bar_oi_adds = [0.0]   # index 0 placeholder (no delta for first bar)
        for i in range(1, n_bars):
            bar_add = 0.0
            for s in strikes:
                if i < len(ce_oi_all[s]):
                    v = ce_oi_all[s][i] - ce_oi_all[s][i-1]
                    if np.isfinite(v):
                        bar_add += abs(v)
                if i < len(pe_oi_all[s]):
                    v = pe_oi_all[s][i] - pe_oi_all[s][i-1]
                    if np.isfinite(v):
                        bar_add += abs(v)
            bar_oi_adds.append(bar_add)

        features = []
        for end in range(WINDOW, n_bars):
            start = end - WINDOW

            # Running average: mean of all bars up to (but not including) `end`
            # This matches how the runtime classifier accumulates _daily_oi_adds
            running_avg = float(np.mean(bar_oi_adds[:end])) if end > 0 else 1.0
            running_avg = max(running_avg, 1.0)

            def _window_slice(arr_dict: dict) -> dict:
                return {s: list(arr[start:end])
                        for s, arr in arr_dict.items()}

            feat = compute_features(
                ce_oi_window     = _window_slice(ce_oi_all),
                pe_oi_window     = _window_slice(pe_oi_all),
                ce_close_window  = _window_slice(ce_close_all),
                pe_close_window  = _window_slice(pe_close_all),
                fut_close_window = list(fut_close[start:end]),
                atm_strike       = atm_strike,
                daily_avg_oi_add = running_avg,
                fii_cash_5d_norm = fii_cash_5d_norm,
            )
            if feat is not None:
                features.append(feat)

        return features

    # ── Threshold fitting ─────────────────────────────────────────────────────

    def _fit_thresholds(self, features_by_label: dict) -> dict:
        """
        Fit Gaussian parameters (median, std) per (feature, label).
        Also store global percentiles for diagnostic plots.
        """
        thresholds: dict = {
            'feature_stats':   {},   # {feature: {label: {median, std}}}
            'global_pct':      {},   # {feature: {p10, p25, p50, p75, p90}}
            'label_centroids': {},   # {label: {feature: median}}
            'feature_names':   FEATURE_NAMES,
        }

        all_vals: dict = {f: [] for f in FEATURE_NAMES}
        for label, feat_list in features_by_label.items():
            for feat in feat_list:
                for f in FEATURE_NAMES:
                    v = feat.get(f, np.nan)
                    if np.isfinite(v):
                        all_vals[f].append(v)

        for feat_name in FEATURE_NAMES:
            vals = all_vals[feat_name]
            if not vals:
                log.warning("Feature '%s' has no finite values across all days.", feat_name)
                continue
            arr = np.array(vals)
            thresholds['global_pct'][feat_name] = {
                'p02': float(np.percentile(arr, 2)),
                'p10': float(np.percentile(arr, 10)),
                'p25': float(np.percentile(arr, 25)),
                'p50': float(np.percentile(arr, 50)),
                'p75': float(np.percentile(arr, 75)),
                'p90': float(np.percentile(arr, 90)),
                'p98': float(np.percentile(arr, 98)),
            }

            label_stats: dict = {}
            for label, feat_list in features_by_label.items():
                lv = np.array([f[feat_name] for f in feat_list
                               if np.isfinite(f.get(feat_name, np.nan))])
                if len(lv) == 0:
                    label_stats[label] = {'median': float(np.median(arr)), 'std': 1.0}
                else:
                    label_stats[label] = {
                        'median': float(np.median(lv)),
                        'std':    float(max(np.std(lv), 1e-6)),
                    }
            thresholds['feature_stats'][feat_name] = label_stats

        for label, feat_list in features_by_label.items():
            if not feat_list:
                continue
            centroid: dict = {}
            for feat_name in FEATURE_NAMES:
                lv = np.array([f[feat_name] for f in feat_list
                               if np.isfinite(f.get(feat_name, np.nan))])
                centroid[feat_name] = float(np.median(lv)) if len(lv) > 0 else 0.0
            thresholds['label_centroids'][label] = centroid

        return thresholds

    # ── Main entry ────────────────────────────────────────────────────────────

    def _fii_cash_norm_for_day(
        self, day: str, fii_cash_df: Optional[pd.DataFrame]
    ) -> float:
        """
        Return lag-1 5-day FII cash flow normalised by 20000 crore for `day`.
        Returns 0.0 if data unavailable (safe fallback — feature becomes neutral).
        """
        if fii_cash_df is None or fii_cash_df.empty:
            return 0.0
        td = pd.Timestamp(day).date()
        prior5 = fii_cash_df[fii_cash_df['date'] < td].tail(5)
        if prior5.empty:
            return 0.0
        cash_5d = float(prior5['fpi_net'].sum())
        return float(np.clip(cash_5d / 20_000.0, -3.0, 3.0))

    def calibrate(self) -> dict:
        """Run full calibration. Returns thresholds dict and writes JSON."""
        log.info("FIIDIICalibrator: loading caches")
        oi_cache, fut_df, spot_df, bhavcopy, fii_cash_df = self._load_caches()

        days = sorted(oi_cache.keys()) if self.training_days is None \
               else sorted(self.training_days)

        log.info(
            "Calibrating on %d days: %s … %s", len(days), days[0], days[-1]
        )

        labels = self._compute_day_labels(bhavcopy, days, fut_df=fut_df)

        features_by_label: dict = {
            'FII_BULL':  [],
            'FII_BEAR':  [],
            'DII_MIXED': [],
        }

        for day in days:
            label            = labels.get(day, 'DII_MIXED')
            cash_norm        = self._fii_cash_norm_for_day(day, fii_cash_df)
            feats            = self._extract_day_features(
                day, oi_cache, fut_df, fii_cash_5d_norm=cash_norm,
            )
            features_by_label[label].extend(feats)
            log.info(
                "day=%s  label=%-10s  fii_cash_norm=%+.2f  feature_vecs=%d",
                day, label, cash_norm, len(feats),
            )

        counts = {l: len(v) for l, v in features_by_label.items()}
        log.info("Total feature vectors per label: %s", counts)

        for label, feats in features_by_label.items():
            if len(feats) < 20:
                log.warning(
                    "Label '%s' has only %d feature vectors — "
                    "thresholds will be unreliable.",
                    label, len(feats),
                )

        thresholds = self._fit_thresholds(features_by_label)
        thresholds['training_days'] = days
        thresholds['label_counts']  = counts

        THRESHOLDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(THRESHOLDS_FILE, 'w') as fh:
            json.dump(thresholds, fh, indent=2)

        log.info("Thresholds written to %s", THRESHOLDS_FILE)
        return thresholds


# ═══════════════════════════════════════════════════════════════════════════════
# OI Snapshot — unit fed to runtime classifier
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class OISnapshot:
    """
    One 1-minute bar snapshot fed to FIIDIIClassifier.

    Fields
    ------
    ts           : bar timestamp
    atm_strike   : round(spot / 50) * 50
    strikes      : sorted list of available strikes
    ce_oi        : {strike: float} — current CE OI (may be ffilled)
    pe_oi        : {strike: float} — current PE OI
    ce_close     : {strike: float} — current CE option LTP
    pe_close     : {strike: float} — current PE option LTP
    fut_close    : current futures LTP
    spot_close   : current spot LTP (optional; falls back to fut_close)
    """
    ts:         pd.Timestamp
    atm_strike: int
    strikes:    list
    ce_oi:      dict = field(default_factory=dict)
    pe_oi:      dict = field(default_factory=dict)
    ce_close:   dict = field(default_factory=dict)
    pe_close:   dict = field(default_factory=dict)
    fut_close:  float = 0.0
    spot_close: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# FIIDIIClassifier
# ═══════════════════════════════════════════════════════════════════════════════

class FIIDIIClassifier:
    """
    Runtime classifier. Keeps a rolling buffer of OI snapshots and classifies
    the current bar against calibrated thresholds.

    Usage
    -----
    clf = FIIDIIClassifier()
    clf.push(snapshot)          # call every minute
    result = clf.classify()

    result = {
        'fii_score':    float,   # -1 to +1 (positive = FII_BULL pressure)
        'dii_score':    float,    # 0 to +1 (higher = DII activity)
        'retail_score': float,    # 0 to +1 (high = retail scatter)
        'attribution':  str,
        'direction':    int,      # +1 / -1 / 0
        'confidence':   float,    # 0–1
        'features':     dict,     # raw feature values
    }
    """

    def __init__(self, thresholds_path: Optional[Path] = None):
        path = thresholds_path or THRESHOLDS_FILE
        if not path.exists():
            raise FileNotFoundError(
                f"FII/DII thresholds not found: {path}. "
                "Run FIIDIICalibrator().calibrate() first."
            )
        with open(path) as fh:
            self._thresh = json.load(fh)

        self._buffer: deque = deque(maxlen=BUFFER_BARS)
        self._daily_oi_adds: list = []   # track per-bar total |ΔOI| for intensity normalizer
        self._prev_snap: Optional[OISnapshot] = None

        log.info(
            "FIIDIIClassifier loaded  trained_days=%d  label_counts=%s",
            len(self._thresh.get('training_days', [])),
            self._thresh.get('label_counts', {}),
        )

    # ── Push ─────────────────────────────────────────────────────────────────

    def push(self, snap: OISnapshot) -> None:
        """Accept one 1-minute bar. Must be called in chronological order."""
        # Track per-bar total OI additions for intensity normaliser
        if self._prev_snap is not None:
            bar_add = 0.0
            for s in snap.strikes:
                dce = abs(snap.ce_oi.get(s, 0.0) - self._prev_snap.ce_oi.get(s, 0.0))
                dpe = abs(snap.pe_oi.get(s, 0.0) - self._prev_snap.pe_oi.get(s, 0.0))
                bar_add += dce + dpe
            self._daily_oi_adds.append(bar_add)

        self._buffer.append(snap)
        self._prev_snap = snap

    # ── Build window from buffer ───────────────────────────────────────────────

    def _build_windows(self) -> Optional[tuple]:
        """
        Construct window dicts from the rolling buffer.
        Returns (ce_oi_w, pe_oi_w, ce_close_w, pe_close_w, fut_close_w, atm, daily_avg)
        or None if insufficient data.
        """
        n = len(self._buffer)
        if n < MIN_WINDOW:
            return None

        snaps = list(self._buffer)[-WINDOW:]    # up to WINDOW most recent
        latest = snaps[-1]
        atm    = latest.atm_strike

        ce_oi_w:    dict = {s: [] for s in latest.strikes}
        pe_oi_w:    dict = {s: [] for s in latest.strikes}
        ce_close_w: dict = {s: [] for s in latest.strikes}
        pe_close_w: dict = {s: [] for s in latest.strikes}
        fut_close_w = []

        for snap in snaps:
            fut_close_w.append(snap.fut_close)
            for s in latest.strikes:
                ce_oi_w[s].append(snap.ce_oi.get(s, np.nan))
                pe_oi_w[s].append(snap.pe_oi.get(s, np.nan))
                ce_close_w[s].append(snap.ce_close.get(s, np.nan))
                pe_close_w[s].append(snap.pe_close.get(s, np.nan))

        daily_avg = float(np.mean(self._daily_oi_adds)) if self._daily_oi_adds else 1.0
        return ce_oi_w, pe_oi_w, ce_close_w, pe_close_w, fut_close_w, atm, daily_avg

    # ── Gaussian log-likelihood scoring ───────────────────────────────────────

    def _score_features(self, features: dict) -> dict:
        """
        Compute Gaussian log-likelihood of `features` under each label distribution.
        Returns {label: probability} (normalised via softmax).

        Feature clipping: values outside the training p02-p98 range are clipped
        before scoring.  Without this, out-of-distribution crash-regime values
        (e.g. ce_pe_imbalance=-0.73 when training max is -0.15) produce massive
        z-scores that cause the softmax to pick whichever label centroid is
        'least wrong' rather than reflecting genuine evidence.
        """
        feat_stats  = self._thresh.get('feature_stats', {})
        global_pct  = self._thresh.get('global_pct', {})
        labels      = ['FII_BULL', 'FII_BEAR', 'DII_MIXED']
        log_scores  = {l: 0.0 for l in labels}

        for feat_name in FEATURE_NAMES:
            val = features.get(feat_name, np.nan)
            if not np.isfinite(val) or feat_name not in feat_stats:
                continue

            # Clip to training distribution bounds (p02 / p98)
            pct = global_pct.get(feat_name, {})
            lo  = pct.get('p02', -np.inf)
            hi  = pct.get('p98',  np.inf)
            val = float(np.clip(val, lo, hi))

            for label in labels:
                stats  = feat_stats[feat_name].get(label, {'median': 0.0, 'std': 1.0})
                mu     = stats['median']
                sigma  = max(stats['std'], 1e-6)
                z2     = ((val - mu) / sigma) ** 2
                log_scores[label] -= 0.5 * z2    # Gaussian log-likelihood (unnorm)

        # Softmax
        max_ls = max(log_scores.values())
        exp_s  = {l: np.exp(s - max_ls) for l, s in log_scores.items()}
        tot    = sum(exp_s.values())
        return {l: float(v / tot) for l, v in exp_s.items()}

    # ── Main classify ─────────────────────────────────────────────────────────

    def classify(self, fii_cash_5d_norm: float = 0.0) -> dict:
        """
        Classify the current bar. Returns full attribution dict.

        Parameters
        ----------
        fii_cash_5d_norm : float
            Lag-1 5-day FII cash flow normalised by 20000 crore.
            Pass from backtest / live runner every bar.
            Default 0.0 (neutral) for backward compatibility.
        """
        _null = {
            'fii_score':    0.0,
            'dii_score':    0.0,
            'retail_score': 0.0,
            'attribution':  UNKNOWN,
            'direction':    0,
            'confidence':   0.0,
            'features':     {},
        }

        windows = self._build_windows()
        if windows is None:
            return {**_null, 'attribution': UNKNOWN}

        ce_oi_w, pe_oi_w, ce_close_w, pe_close_w, fut_close_w, atm, daily_avg = windows

        features = compute_features(
            ce_oi_window     = ce_oi_w,
            pe_oi_window     = pe_oi_w,
            ce_close_window  = ce_close_w,
            pe_close_window  = pe_close_w,
            fut_close_window = fut_close_w,
            atm_strike       = atm,
            daily_avg_oi_add = daily_avg,
            fii_cash_5d_norm = fii_cash_5d_norm,
        )
        if features is None:
            return {**_null, 'attribution': UNKNOWN}

        probs = self._score_features(features)

        fii_bull_p = probs.get('FII_BULL', 0.33)
        fii_bear_p = probs.get('FII_BEAR', 0.33)
        dii_p      = probs.get('DII_MIXED', 0.33)

        # Directional FII score: +1 = pure FII_BULL, -1 = pure FII_BEAR
        fii_net     = float(fii_bull_p - fii_bear_p)
        fii_total   = float(fii_bull_p + fii_bear_p)

        # Retail proxy: high strike_coverage + low oi_add_intensity
        coverage = features.get('strike_coverage', 0.5)
        intensity = min(1.0, features.get('oi_add_intensity', 1.0))
        retail_score = float(coverage * 0.6 + (1.0 - min(1.0, intensity)) * 0.4)

        # ── Attribution decision ──────────────────────────────────────────
        ce_pe = features.get('ce_pe_imbalance', 0.0)

        if retail_score > 0.7 and fii_total < 0.5:
            attribution = RETAIL
            direction   = 0
            confidence  = float(retail_score * 0.4)

        elif fii_bull_p > fii_bear_p and fii_bull_p > dii_p:
            attribution = FII_BULL
            direction   = 1
            confidence  = float(fii_bull_p * min(1.0, fii_net + 0.2))

        elif fii_bear_p > fii_bull_p and fii_bear_p > dii_p:
            attribution = FII_BEAR
            direction   = -1
            confidence  = float(fii_bear_p * min(1.0, -fii_net + 0.2))

        elif dii_p >= max(fii_bull_p, fii_bear_p):
            # DII: use ce_pe_imbalance to determine direction
            if ce_pe > 0.05:
                attribution = DII_BULL
                direction   = 1
                confidence  = float(dii_p * 0.5)
            elif ce_pe < -0.05:
                attribution = DII_BEAR
                direction   = -1
                confidence  = float(dii_p * 0.5)
            else:
                attribution = MIXED
                direction   = 0
                confidence  = float(dii_p * 0.25)

        else:
            attribution = MIXED
            direction   = 1 if ce_pe > 0 else (-1 if ce_pe < 0 else 0)
            confidence  = 0.15

        return {
            'fii_score':    fii_net,
            'dii_score':    float(dii_p),
            'retail_score': float(retail_score),
            'attribution':  attribution,
            'direction':    int(direction),
            'confidence':   float(min(1.0, confidence)),
            'features':     features,
        }

    def reset(self) -> None:
        """Clear rolling buffer. Call at the start of each trading day."""
        self._buffer.clear()
        self._daily_oi_adds.clear()
        self._prev_snap = None
        log.debug("FIIDIIClassifier buffer reset")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI: run calibration
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s',
    )

    days_arg = None
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days_arg = arg.split('=', 1)[1].split(',')

    cal    = FIIDIICalibrator(training_days=days_arg)
    thresh = cal.calibrate()

    print(f"\n{'='*60}")
    print(f"Calibration complete — {len(thresh['training_days'])} days")
    print(f"Label counts: {thresh['label_counts']}")
    print(f"\nLabel centroids (feature medians):")
    print(f"{'Feature':<22} {'FII_BULL':>10} {'FII_BEAR':>10} {'DII_MIXED':>10}")
    print('-' * 55)
    for feat in thresh.get('feature_names', FEATURE_NAMES):
        stats = thresh.get('feature_stats', {}).get(feat, {})
        row = {l: stats.get(l, {}).get('median', 0.0) for l in ['FII_BULL', 'FII_BEAR', 'DII_MIXED']}
        print(f"  {feat:<20} {row['FII_BULL']:>+10.4f} {row['FII_BEAR']:>+10.4f} {row['DII_MIXED']:>+10.4f}")
    print(f"\nThresholds saved to: {THRESHOLDS_FILE}")
