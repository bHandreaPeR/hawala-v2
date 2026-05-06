"""
v3/signals/fii_dii_classifier_intel.py
========================================
FII/DII Activity Classifier — INTEL-augmented variant.

Extends the production v3/signals/fii_dii_classifier.py with 3 day-level
features sourced from the NSE-reports intelligence time series.  This module
is a parallel implementation; the existing classifier is NOT modified.

New features (day-level, available pre-market on day T from intel[T-1]):
  9.  roc5_dii_net      — 5-day change in DII total-net (longs - shorts).
                           Phase-5 IC=+0.18, stable across H1/H2.
  10. roc5_usdinr       — 5-day change in USDINR near-month settle (₹).
                           Phase-5 IC=-0.119, cross-asset signal validated.
  11. roc5_fii_ni_fut   — 5-day change in FII Nifty-futures net (₹ crore).
                           Phase-5 IC=-0.114, FII flow signal.

These three came out of the Phase-5 IC + H1/H2 stability test as the only
robust survivors after de-trending.  All other intel-derived features were
trend artifacts or redundant.

Calibration writes to a SEPARATE thresholds file so head-to-head comparison
with the production classifier is clean.
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

# Re-use the existing single-bar feature extractor + window machinery
# (we only need to layer on the 3 day-level features at scoring time).
from .fii_dii_classifier import (
    compute_features as compute_features_base,
    FEATURE_NAMES as BASE_FEATURE_NAMES,
    OISnapshot,
    STRIKE_STEP, ATM_BAND, OTM_MIN, OTM_MAX,
    WINDOW, MIN_WINDOW, BUFFER_BARS,
    FII_BULL, FII_BEAR, DII_BULL, DII_BEAR, RETAIL, MIXED, UNKNOWN,
)

log = logging.getLogger('v3.fii_dii_intel')

ROOT             = Path(__file__).resolve().parents[2]
OI_CACHE         = ROOT / 'v3' / 'cache' / 'option_oi_1m_NIFTY.pkl'
FUTURES_CACHE    = ROOT / 'v3' / 'cache' / 'candles_1m_NIFTY.pkl'
SPOT_CACHE       = ROOT / 'v3' / 'cache' / 'candles_1m_spot_NIFTY.pkl'
BHAVCOPY_CACHE   = ROOT / 'v3' / 'cache' / 'bhavcopy_NIFTY_all.pkl'
INTEL_PARQUET    = ROOT / 'v3' / 'cache' / 'nse_reports' / 'intel_timeseries.parquet'
THRESHOLDS_FILE  = ROOT / 'v3' / 'cache' / 'fii_dii_thresholds_intel.json'

# 3 new features
INTEL_FEATURE_NAMES = ['roc5_dii_net', 'roc5_usdinr', 'roc5_fii_ni_fut']
FEATURE_NAMES = BASE_FEATURE_NAMES + INTEL_FEATURE_NAMES


# ═════════════════════════════════════════════════════════════════════════════
# Day-level intel ROC lookup
# ═════════════════════════════════════════════════════════════════════════════

def load_intel_timeseries(path: Path = INTEL_PARQUET) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"intel_timeseries.parquet not found at {path}. "
            "Run v3/scripts/aggregate_intel_timeseries.py first."
        )
    df = pd.read_parquet(path)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.sort_values('trade_date').reset_index(drop=True)
    # Forward-fill the 3 source columns to bridge NSE outage gaps.
    # Justification: ROC over 5 trade-days is the target; if usdinr is missing
    # on day X due to NSE outage, using the previous-day's value is the best
    # available estimate (rate doesn't move overnight).  This is explicit
    # ffill-after-outage, NOT general data synthesis.
    for c in ('poi_dii_total_net', 'cd_usdinr_near_settle', 'fii_stats_ni_fut_net_crore'):
        if c not in df.columns:
            raise ValueError(f"intel_timeseries missing required column: {c}")
        df[c] = df[c].ffill()
    return df


def intel_roc_for_day(day: str, intel_df: pd.DataFrame) -> dict:
    """
    Return the 3 ROC features for trading day `day` using intel data
    strictly before `day` (NSE EOD for T-1 is published the previous evening).

    Raises if there are <6 prior trading days in the intel set (cannot
    compute a 5-day ROC).
    """
    td = pd.Timestamp(day).normalize()
    prior = intel_df[intel_df['trade_date'] < td]
    if len(prior) < 6:
        raise ValueError(
            f"Insufficient prior intel rows ({len(prior)}) before {day} "
            f"to compute 5-day ROC; need >=6."
        )
    last  = prior.iloc[-1]
    five  = prior.iloc[-6]
    return {
        'roc5_dii_net':     float(last['poi_dii_total_net']      - five['poi_dii_total_net']),
        'roc5_usdinr':      float(last['cd_usdinr_near_settle']  - five['cd_usdinr_near_settle']),
        'roc5_fii_ni_fut':  float(last['fii_stats_ni_fut_net_crore']
                                  - five['fii_stats_ni_fut_net_crore']),
    }


# ═════════════════════════════════════════════════════════════════════════════
# Feature computation (8 base + 3 intel)
# ═════════════════════════════════════════════════════════════════════════════

def compute_features(
    *,
    ce_oi_window:    dict,
    pe_oi_window:    dict,
    ce_close_window: dict,
    pe_close_window: dict,
    fut_close_window: list,
    atm_strike:      int,
    daily_avg_oi_add: float = 1.0,
    fii_cash_5d_norm: float = 0.0,
    roc5_dii_net:     float = 0.0,
    roc5_usdinr:      float = 0.0,
    roc5_fii_ni_fut:  float = 0.0,
) -> Optional[dict]:
    """
    Wrap the 8-feature computer and append 3 day-level intel features.
    """
    feat = compute_features_base(
        ce_oi_window     = ce_oi_window,
        pe_oi_window     = pe_oi_window,
        ce_close_window  = ce_close_window,
        pe_close_window  = pe_close_window,
        fut_close_window = fut_close_window,
        atm_strike       = atm_strike,
        daily_avg_oi_add = daily_avg_oi_add,
        fii_cash_5d_norm = fii_cash_5d_norm,
    )
    if feat is None:
        return None

    # All 3 are clipped to a sane range to keep the Gaussian classifier robust
    # against extreme outliers.  Clip values are loose (5–95th percentile of
    # the 164-day window plus 50% buffer); not synthesised, just bounded.
    feat['roc5_dii_net']    = float(np.clip(roc5_dii_net,    -3_000_000, 3_000_000))
    feat['roc5_usdinr']     = float(np.clip(roc5_usdinr,     -2.5,        2.5))
    feat['roc5_fii_ni_fut'] = float(np.clip(roc5_fii_ni_fut, -10_000,    10_000))
    return feat


# ═════════════════════════════════════════════════════════════════════════════
# Calibrator  (full re-implementation; doesn't mutate baseline state)
# ═════════════════════════════════════════════════════════════════════════════

class FIIDIICalibratorIntel:
    """
    Offline calibrator for the 11-feature intel-augmented classifier.

    Uses the same data-loading path as the baseline calibrator, but
    additionally loads `intel_timeseries.parquet` and injects 3 day-level
    ROC features per day.

    Writes to v3/cache/fii_dii_thresholds_intel.json.
    """

    def __init__(self, training_days: Optional[list] = None,
                 thresholds_out: Optional[Path] = None):
        self.training_days = training_days
        self.thresholds_out = thresholds_out or THRESHOLDS_FILE

    # ── Data ──────────────────────────────────────────────────────────────────
    def _load_caches(self) -> tuple:
        for path, name in [
            (OI_CACHE,       'Option OI cache (fetch_option_oi_NIFTY.py)'),
            (FUTURES_CACHE,  'Futures 1m cache (fetch_1m_NIFTY.py)'),
            (BHAVCOPY_CACHE, 'Bhavcopy cache (fetch_bhavcopy_nifty.py)'),
            (INTEL_PARQUET,  'NSE-reports intel time series'),
        ]:
            if not path.exists():
                raise FileNotFoundError(f"{name} not found: {path}.")

        with open(OI_CACHE,       'rb') as f: oi_cache = pickle.load(f)
        with open(FUTURES_CACHE,  'rb') as f: fut_df   = pickle.load(f)
        with open(BHAVCOPY_CACHE, 'rb') as f: bhavcopy = pickle.load(f)
        intel = load_intel_timeseries()

        fii_cash_df = None
        fii_cash_file = ROOT / 'fii_data.csv'
        if fii_cash_file.exists():
            fii_cash_df = pd.read_csv(fii_cash_file)
            fii_cash_df['date'] = pd.to_datetime(fii_cash_df['date']).dt.date

        return oi_cache, fut_df, bhavcopy, intel, fii_cash_df

    # ── Labels  (price-direction, identical to baseline) ──────────────────────
    @staticmethod
    def _compute_day_labels(days: list, fut_df: pd.DataFrame) -> dict:
        labels: dict = {}
        for day in days:
            day_dt = pd.to_datetime(day).date()
            sub = fut_df[fut_df['date'] == day_dt].sort_values('ts')
            if sub.empty or len(sub) < 2:
                labels[day] = 'DII_MIXED'
                continue
            o, c = float(sub['open'].iloc[0]), float(sub['close'].iloc[-1])
            if o <= 0:
                labels[day] = 'DII_MIXED'
                continue
            ret = (c - o) / o
            labels[day] = 'FII_BULL' if ret > 0.003 else (
                          'FII_BEAR' if ret < -0.003 else 'DII_MIXED')
        return labels

    # ── FII cash lag-1 (matches baseline) ─────────────────────────────────────
    @staticmethod
    def _fii_cash_norm(day: str, fii_cash_df: Optional[pd.DataFrame]) -> float:
        if fii_cash_df is None or fii_cash_df.empty:
            return 0.0
        td = pd.Timestamp(day).date()
        prior5 = fii_cash_df[fii_cash_df['date'] < td].tail(5)
        if prior5.empty:
            return 0.0
        return float(np.clip(prior5['fpi_net'].sum() / 20_000.0, -3.0, 3.0))

    # ── Per-day feature extraction ────────────────────────────────────────────
    def _extract_day_features(
        self, day: str,
        oi_cache: dict, fut_df: pd.DataFrame,
        cash_norm: float, intel_roc: dict,
    ) -> list[dict]:
        if day not in oi_cache:
            return []
        day_oi  = oi_cache[day]
        strikes = sorted(day_oi.keys())
        day_dt  = pd.to_datetime(day).date()
        day_fut = fut_df[fut_df['date'] == day_dt].sort_values('ts').reset_index(drop=True)
        if day_fut.empty:
            return []

        spot_proxy = float(day_fut['close'].iloc[0])
        atm_strike = int(round(spot_proxy / STRIKE_STEP) * STRIKE_STEP)
        n_bars = len(day_fut)

        def _align(df_s, col):
            if df_s is None or df_s.empty or col not in df_s.columns:
                return np.full(n_bars, np.nan)
            arr = df_s[col].values.astype(float)
            if len(arr) >= n_bars:
                return arr[:n_bars]
            pad = np.full(n_bars, arr[-1] if len(arr) > 0 else np.nan)
            pad[:len(arr)] = arr
            return pad

        ce_oi_all, pe_oi_all = {}, {}
        ce_close_all, pe_close_all = {}, {}
        for s in strikes:
            ce_df = day_oi[s].get('CE')
            pe_df = day_oi[s].get('PE')
            ce_oi_all[s]    = _align(ce_df, 'oi')
            pe_oi_all[s]    = _align(pe_df, 'oi')
            ce_close_all[s] = _align(ce_df, 'close')
            pe_close_all[s] = _align(pe_df, 'close')

        fut_close = day_fut['close'].values.astype(float)

        # Per-bar total |ΔOI| for running average
        bar_oi_adds = [0.0]
        for i in range(1, n_bars):
            add = 0.0
            for s in strikes:
                if i < len(ce_oi_all[s]):
                    v = ce_oi_all[s][i] - ce_oi_all[s][i-1]
                    if np.isfinite(v):
                        add += abs(v)
                if i < len(pe_oi_all[s]):
                    v = pe_oi_all[s][i] - pe_oi_all[s][i-1]
                    if np.isfinite(v):
                        add += abs(v)
            bar_oi_adds.append(add)

        feats = []
        for end in range(WINDOW, n_bars):
            start = end - WINDOW
            running_avg = max(float(np.mean(bar_oi_adds[:end])), 1.0)
            def _slice(d):
                return {s: list(arr[start:end]) for s, arr in d.items()}

            f = compute_features(
                ce_oi_window     = _slice(ce_oi_all),
                pe_oi_window     = _slice(pe_oi_all),
                ce_close_window  = _slice(ce_close_all),
                pe_close_window  = _slice(pe_close_all),
                fut_close_window = list(fut_close[start:end]),
                atm_strike       = atm_strike,
                daily_avg_oi_add = running_avg,
                fii_cash_5d_norm = cash_norm,
                roc5_dii_net     = intel_roc['roc5_dii_net'],
                roc5_usdinr      = intel_roc['roc5_usdinr'],
                roc5_fii_ni_fut  = intel_roc['roc5_fii_ni_fut'],
            )
            if f is not None:
                feats.append(f)
        return feats

    # ── Threshold fit ─────────────────────────────────────────────────────────
    @staticmethod
    def _fit_thresholds(features_by_label: dict) -> dict:
        out: dict = {
            'feature_stats':   {},
            'global_pct':      {},
            'label_centroids': {},
            'feature_names':   FEATURE_NAMES,
        }
        all_vals = {f: [] for f in FEATURE_NAMES}
        for label, lst in features_by_label.items():
            for feat in lst:
                for f in FEATURE_NAMES:
                    v = feat.get(f, np.nan)
                    if np.isfinite(v):
                        all_vals[f].append(v)

        for fname in FEATURE_NAMES:
            arr = np.array(all_vals[fname])
            if len(arr) == 0:
                log.warning("Feature '%s' has no finite values.", fname)
                continue
            out['global_pct'][fname] = {
                'p02': float(np.percentile(arr, 2)),
                'p10': float(np.percentile(arr, 10)),
                'p25': float(np.percentile(arr, 25)),
                'p50': float(np.percentile(arr, 50)),
                'p75': float(np.percentile(arr, 75)),
                'p90': float(np.percentile(arr, 90)),
                'p98': float(np.percentile(arr, 98)),
            }
            stats = {}
            for label, lst in features_by_label.items():
                lv = np.array([f[fname] for f in lst
                               if np.isfinite(f.get(fname, np.nan))])
                stats[label] = (
                    {'median': float(np.median(arr)), 'std': 1.0}
                    if len(lv) == 0 else
                    {'median': float(np.median(lv)),
                     'std':    float(max(np.std(lv), 1e-6))}
                )
            out['feature_stats'][fname] = stats

        for label, lst in features_by_label.items():
            if not lst:
                continue
            cent = {}
            for fname in FEATURE_NAMES:
                lv = np.array([f[fname] for f in lst
                               if np.isfinite(f.get(fname, np.nan))])
                cent[fname] = float(np.median(lv)) if len(lv) > 0 else 0.0
            out['label_centroids'][label] = cent
        return out

    # ── Main entry ────────────────────────────────────────────────────────────
    def calibrate(self) -> dict:
        log.info("FIIDIICalibratorIntel: loading caches")
        oi_cache, fut_df, bhavcopy, intel, fii_cash_df = self._load_caches()

        days = sorted(oi_cache.keys()) if self.training_days is None \
               else sorted(self.training_days)
        log.info("Calibrating on %d days: %s … %s", len(days), days[0], days[-1])

        labels = self._compute_day_labels(days, fut_df)

        bucket: dict = {'FII_BULL': [], 'FII_BEAR': [], 'DII_MIXED': []}

        for day in days:
            try:
                intel_roc = intel_roc_for_day(day, intel)
            except ValueError as e:
                log.info("skip day=%s: %s", day, e)
                continue
            cash_norm = self._fii_cash_norm(day, fii_cash_df)
            label     = labels.get(day, 'DII_MIXED')
            feats     = self._extract_day_features(
                day, oi_cache, fut_df, cash_norm, intel_roc,
            )
            bucket[label].extend(feats)
            log.info(
                "day=%s label=%-10s cash=%+.2f roc_dii=%+.0f roc_usd=%+.3f "
                "roc_nifut=%+.0f vecs=%d",
                day, label, cash_norm,
                intel_roc['roc5_dii_net'],
                intel_roc['roc5_usdinr'],
                intel_roc['roc5_fii_ni_fut'],
                len(feats),
            )

        counts = {k: len(v) for k, v in bucket.items()}
        log.info("Total feature vectors per label: %s", counts)

        thresh = self._fit_thresholds(bucket)
        thresh['training_days'] = days
        thresh['label_counts']  = counts

        self.thresholds_out.parent.mkdir(parents=True, exist_ok=True)
        with open(self.thresholds_out, 'w') as fh:
            json.dump(thresh, fh, indent=2)

        log.info("Thresholds written to %s", self.thresholds_out)
        return thresh


# ═════════════════════════════════════════════════════════════════════════════
# Runtime classifier
# ═════════════════════════════════════════════════════════════════════════════

class FIIDIIClassifierIntel:
    """
    11-feature runtime classifier.  Same Gaussian-log-likelihood scoring as the
    baseline; only the feature dimension is bigger.
    """

    def __init__(self, thresholds_path: Optional[Path] = None):
        path = thresholds_path or THRESHOLDS_FILE
        if not path.exists():
            raise FileNotFoundError(
                f"Intel thresholds not found: {path}. "
                "Run FIIDIICalibratorIntel().calibrate() first."
            )
        with open(path) as fh:
            self._thresh = json.load(fh)

        self._buffer: deque = deque(maxlen=BUFFER_BARS)
        self._daily_oi_adds: list = []
        self._prev_snap: Optional[OISnapshot] = None

    def push(self, snap: OISnapshot) -> None:
        if self._prev_snap is not None:
            add = 0.0
            for s in snap.strikes:
                add += abs(snap.ce_oi.get(s, 0.0) - self._prev_snap.ce_oi.get(s, 0.0))
                add += abs(snap.pe_oi.get(s, 0.0) - self._prev_snap.pe_oi.get(s, 0.0))
            self._daily_oi_adds.append(add)
        self._buffer.append(snap)
        self._prev_snap = snap

    def _build_windows(self) -> Optional[tuple]:
        if len(self._buffer) < MIN_WINDOW:
            return None
        snaps = list(self._buffer)[-WINDOW:]
        latest = snaps[-1]
        ce_oi_w = {s: [] for s in latest.strikes}
        pe_oi_w = {s: [] for s in latest.strikes}
        ce_close_w = {s: [] for s in latest.strikes}
        pe_close_w = {s: [] for s in latest.strikes}
        fut_close_w = []
        for snap in snaps:
            fut_close_w.append(snap.fut_close)
            for s in latest.strikes:
                ce_oi_w[s].append(snap.ce_oi.get(s, np.nan))
                pe_oi_w[s].append(snap.pe_oi.get(s, np.nan))
                ce_close_w[s].append(snap.ce_close.get(s, np.nan))
                pe_close_w[s].append(snap.pe_close.get(s, np.nan))
        daily_avg = float(np.mean(self._daily_oi_adds)) if self._daily_oi_adds else 1.0
        return ce_oi_w, pe_oi_w, ce_close_w, pe_close_w, fut_close_w, latest.atm_strike, daily_avg

    def _score(self, features: dict) -> dict:
        feat_stats = self._thresh.get('feature_stats', {})
        global_pct = self._thresh.get('global_pct', {})
        labels = ['FII_BULL', 'FII_BEAR', 'DII_MIXED']
        log_scores = {l: 0.0 for l in labels}
        for fname in FEATURE_NAMES:
            v = features.get(fname, np.nan)
            if not np.isfinite(v) or fname not in feat_stats:
                continue
            pct = global_pct.get(fname, {})
            v = float(np.clip(v, pct.get('p02', -np.inf), pct.get('p98', np.inf)))
            for label in labels:
                st = feat_stats[fname].get(label, {'median': 0.0, 'std': 1.0})
                z2 = ((v - st['median']) / max(st['std'], 1e-6)) ** 2
                log_scores[label] -= 0.5 * z2
        m = max(log_scores.values())
        ex = {l: np.exp(s - m) for l, s in log_scores.items()}
        tot = sum(ex.values())
        return {l: float(v / tot) for l, v in ex.items()}

    def classify(self,
                 fii_cash_5d_norm: float = 0.0,
                 roc5_dii_net:     float = 0.0,
                 roc5_usdinr:      float = 0.0,
                 roc5_fii_ni_fut:  float = 0.0) -> dict:
        null = {'fii_score': 0.0, 'dii_score': 0.0, 'retail_score': 0.0,
                'attribution': UNKNOWN, 'direction': 0,
                'confidence': 0.0, 'features': {}}
        windows = self._build_windows()
        if windows is None:
            return null
        ce_oi_w, pe_oi_w, ce_close_w, pe_close_w, fut_close_w, atm, daily_avg = windows
        feats = compute_features(
            ce_oi_window     = ce_oi_w,
            pe_oi_window     = pe_oi_w,
            ce_close_window  = ce_close_w,
            pe_close_window  = pe_close_w,
            fut_close_window = fut_close_w,
            atm_strike       = atm,
            daily_avg_oi_add = daily_avg,
            fii_cash_5d_norm = fii_cash_5d_norm,
            roc5_dii_net     = roc5_dii_net,
            roc5_usdinr      = roc5_usdinr,
            roc5_fii_ni_fut  = roc5_fii_ni_fut,
        )
        if feats is None:
            return null
        probs = self._score(feats)
        bull, bear, dii = probs['FII_BULL'], probs['FII_BEAR'], probs['DII_MIXED']
        fii_net = bull - bear
        coverage  = feats.get('strike_coverage', 0.5)
        intensity = min(1.0, feats.get('oi_add_intensity', 1.0))
        retail    = coverage * 0.6 + (1.0 - intensity) * 0.4
        ce_pe = feats.get('ce_pe_imbalance', 0.0)

        if retail > 0.7 and (bull + bear) < 0.5:
            attr, dir_, conf = RETAIL, 0, retail * 0.4
        elif bull > bear and bull > dii:
            attr, dir_, conf = FII_BULL, 1, bull * min(1.0, fii_net + 0.2)
        elif bear > bull and bear > dii:
            attr, dir_, conf = FII_BEAR, -1, bear * min(1.0, -fii_net + 0.2)
        elif dii >= max(bull, bear):
            if ce_pe > 0.05:
                attr, dir_, conf = DII_BULL, 1, dii * 0.5
            elif ce_pe < -0.05:
                attr, dir_, conf = DII_BEAR, -1, dii * 0.5
            else:
                attr, dir_, conf = MIXED, 0, dii * 0.25
        else:
            attr, dir_, conf = MIXED, (1 if ce_pe > 0 else (-1 if ce_pe < 0 else 0)), 0.15

        return {'fii_score': float(fii_net), 'dii_score': float(dii),
                'retail_score': float(retail), 'attribution': attr,
                'direction': int(dir_), 'confidence': float(min(1.0, conf)),
                'features': feats, 'probs': probs}

    def reset(self) -> None:
        self._buffer.clear()
        self._daily_oi_adds.clear()
        self._prev_snap = None


# ═════════════════════════════════════════════════════════════════════════════
if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    days_arg = None
    out_arg  = None
    for a in sys.argv[1:]:
        if a.startswith('--days='):
            days_arg = a.split('=', 1)[1].split(',')
        elif a.startswith('--out='):
            out_arg = Path(a.split('=', 1)[1])
    cal = FIIDIICalibratorIntel(training_days=days_arg, thresholds_out=out_arg)
    cal.calibrate()
