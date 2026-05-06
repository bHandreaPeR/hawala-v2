"""
v3/signals/fii_dii_classifier_COMBINED.py
==========================================
FII/DII Activity Classifier — Combined Nifty + BankNifty variant.

Calibrates on BOTH instruments' 1m option OI data simultaneously.
Feature extraction uses per-instrument constants (STRIKE_STEP, ATM_BAND,
OTM ranges) so that "near-ATM" and "OTM" represent proportionally
equivalent strike counts for each instrument.

All 8 features are scale-agnostic (ratios / percentages), so pooling
feature vectors from both instruments is statistically valid:
  ce_pe_imbalance   — OI change ratio            ✓ scale-agnostic
  strike_coverage   — count ratio                ✓ scale-agnostic
  atm_build_rate    — OI proportion at ATM band  ✓ if ATM_BAND covers same #strikes
  oi_add_intensity  — normalised by running avg  ✓ scale-agnostic
  basis_momentum    — % return                   ✓ scale-agnostic
  ce_skew_shift     — % price change OTM calls   ✓ if OTM range covers same #strikes
  pe_skew_shift     — % price change OTM puts    ✓ same
  fii_cash_context  — common FII cash series     ✓ identical for both

Thresholds saved to:  v3/cache/fii_dii_thresholds_COMBINED.json

Runtime FIIDIIClassifier accepts instrument='NIFTY' (default) or
instrument='BANKNIFTY' to apply the right per-instrument ATM/OTM
constants during feature extraction while using the combined thresholds.

Usage:
  # Calibrate (offline, once after data fetch)
  from v3.signals.fii_dii_classifier_COMBINED import FIIDIICalibrator
  FIIDIICalibrator().calibrate()

  # Runtime — BankNifty live/backtest
  from v3.signals.fii_dii_classifier_COMBINED import FIIDIIClassifier, OISnapshot
  clf = FIIDIIClassifier(instrument='BANKNIFTY')
  clf.push(snapshot)
  result = clf.classify()
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

log = logging.getLogger('v3.fii_dii_combined')

ROOT            = Path(__file__).resolve().parents[2]
THRESHOLDS_FILE = ROOT / 'v3' / 'cache' / 'fii_dii_thresholds_COMBINED.json'

# ── Per-instrument constants ──────────────────────────────────────────────────
_INST_CFG = {
    'NIFTY': {
        'oi_cache':      ROOT / 'v3' / 'cache' / 'option_oi_1m_NIFTY.pkl',
        'futures_cache': ROOT / 'v3' / 'cache' / 'candles_1m_NIFTY.pkl',
        'bhavcopy_cache':ROOT / 'v3' / 'cache' / 'bhavcopy_NIFTY_all.pkl',
        'strike_step':   50,
        'atm_band':      200,   # ±200 pts = 4 strikes at 50pt intervals
        'otm_min':       300,
        'otm_max':       800,
    },
    'BANKNIFTY': {
        'oi_cache':      ROOT / 'v3' / 'cache' / 'option_oi_1m_BANKNIFTY.pkl',
        'futures_cache': ROOT / 'v3' / 'cache' / 'candles_1m_BANKNIFTY.pkl',
        'bhavcopy_cache':ROOT / 'v3' / 'cache' / 'bhavcopy_BN_all.pkl',
        'strike_step':   100,
        'atm_band':      400,   # ±400 pts = 4 strikes at 100pt intervals
        'otm_min':       600,
        'otm_max':       1600,
    },
}

WINDOW      = 15   # rolling window bars for feature extraction
MIN_WINDOW  = 5    # minimum bars before classifying
BUFFER_BARS = 30   # max history in runtime buffer

# ── Attribution labels ────────────────────────────────────────────────────────
FII_BULL = 'FII_BULL'
FII_BEAR = 'FII_BEAR'
DII_BULL = 'DII_BULL'
DII_BEAR = 'DII_BEAR'
RETAIL   = 'RETAIL'
MIXED    = 'MIXED'
UNKNOWN  = 'UNKNOWN'

FEATURE_NAMES = [
    'ce_pe_imbalance',
    'strike_coverage',
    'atm_build_rate',
    'oi_add_intensity',
    'basis_momentum',
    'ce_skew_shift',
    'pe_skew_shift',
    'fii_cash_context',
]


# ═══════════════════════════════════════════════════════════════════════════════
# Feature computation  (instrument-parameterised)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_features(
    ce_oi_window:     dict,
    pe_oi_window:     dict,
    ce_close_window:  dict,
    pe_close_window:  dict,
    fut_close_window: list,
    atm_strike:       int,
    daily_avg_oi_add: float = 1.0,
    fii_cash_5d_norm: float = 0.0,
    atm_band:         int   = 200,
    otm_min:          int   = 300,
    otm_max:          int   = 800,
) -> Optional[dict]:
    """
    Compute 8 features over the provided window.
    atm_band / otm_min / otm_max are instrument-specific and must be
    passed by the caller.
    """
    strikes   = sorted(set(ce_oi_window.keys()) | set(pe_oi_window.keys()))
    n_strikes = len(strikes)
    if n_strikes == 0:
        return None

    w = len(fut_close_window)
    if w < MIN_WINDOW:
        return None

    # ── OI delta per strike ───────────────────────────────────────────────────
    ce_delta: dict = {}
    pe_delta: dict = {}
    for s in strikes:
        for delta_d, series_d in [(ce_delta, ce_oi_window), (pe_delta, pe_oi_window)]:
            series = series_d.get(s, [])
            if len(series) >= 2:
                first = next((v for v in series if not np.isnan(v)), 0.0)
                last  = next((v for v in reversed(series) if not np.isnan(v)), first)
                delta_d[s] = float(last - first)
            else:
                delta_d[s] = 0.0

    tot_ce     = sum(ce_delta.values())
    tot_pe     = sum(pe_delta.values())
    tot_abs_ce = sum(abs(v) for v in ce_delta.values())
    tot_abs_pe = sum(abs(v) for v in pe_delta.values())

    # F1 ce_pe_imbalance
    denom_cp        = tot_abs_ce + tot_abs_pe + 1.0
    ce_pe_imbalance = float((tot_ce - tot_pe) / denom_cp)

    # F2 strike_coverage
    active        = sum(1 for s in strikes if abs(ce_delta.get(s,0)) + abs(pe_delta.get(s,0)) > 0)
    strike_coverage = float(active / max(n_strikes, 1))

    # F3 atm_build_rate
    atm_strikes   = [s for s in strikes if abs(s - atm_strike) <= atm_band]
    atm_oi_abs    = sum(abs(ce_delta.get(s,0)) + abs(pe_delta.get(s,0)) for s in atm_strikes)
    total_oi_abs  = tot_abs_ce + tot_abs_pe + 1.0
    atm_build_rate = float(atm_oi_abs / total_oi_abs)

    # F4 oi_add_intensity
    oi_add_intensity = float(total_oi_abs / (daily_avg_oi_add * w + 1.0))

    # F5 basis_momentum
    fut_clean = [v for v in fut_close_window if v and not np.isnan(v)]
    if len(fut_clean) >= 2:
        basis_momentum = float((fut_clean[-1] - fut_clean[0]) / (fut_clean[0] + 1e-6) * 100)
    else:
        basis_momentum = 0.0

    # F6/F7 skew shifts
    otm_ce_strikes = [s for s in strikes if otm_min <= (s - atm_strike) <= otm_max]
    otm_pe_strikes = [s for s in strikes if otm_min <= (atm_strike - s) <= otm_max]

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

    # F8 fii_cash_context
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
    for k, v in feat.items():
        if not np.isfinite(v):
            feat[k] = 0.0
    return feat


# ═══════════════════════════════════════════════════════════════════════════════
# FIIDIICalibrator — Combined
# ═══════════════════════════════════════════════════════════════════════════════

class FIIDIICalibrator:
    """
    Combined calibrator. Loads 1m OI + futures + bhavcopy for BOTH Nifty
    and BankNifty, extracts features using per-instrument constants, pools
    all feature vectors, fits Gaussian thresholds, writes JSON.
    """

    def __init__(self, instruments: list = None, training_days: dict = None):
        """
        Parameters
        ----------
        instruments  : list of instrument names to include, e.g. ['NIFTY', 'BANKNIFTY'].
                       Default: both.
        training_days: {instrument: [date_str, ...]} optional per-instrument day filter.
                       If None, uses all days available in each OI cache.
        """
        self.instruments   = instruments or ['NIFTY', 'BANKNIFTY']
        self.training_days = training_days or {}

    # ── Data loading ──────────────────────────────────────────────────────────

    def _load_instrument(self, inst: str) -> tuple:
        cfg = _INST_CFG[inst]
        for path, name in [
            (cfg['oi_cache'],       f'{inst} Option OI cache'),
            (cfg['futures_cache'],  f'{inst} Futures 1m cache'),
            (cfg['bhavcopy_cache'], f'{inst} Bhavcopy cache'),
        ]:
            if not path.exists():
                raise FileNotFoundError(f"{name} not found: {path}. Run the fetcher first.")

        with open(cfg['oi_cache'],       'rb') as f: oi_cache = pickle.load(f)
        with open(cfg['futures_cache'],  'rb') as f: fut_df   = pickle.load(f)
        with open(cfg['bhavcopy_cache'], 'rb') as f: bhavcopy = pickle.load(f)
        return oi_cache, fut_df, bhavcopy

    def _load_fii_cash(self) -> Optional[pd.DataFrame]:
        fii_cash_file = ROOT / 'fii_data.csv'
        if not fii_cash_file.exists():
            log.warning("fii_data.csv not found — fii_cash_context will be 0.0")
            return None
        df = pd.read_csv(fii_cash_file)
        df['date'] = pd.to_datetime(df['date']).dt.date
        log.info("FII cash data loaded: %d rows", len(df))
        return df

    # ── Price-direction labels ────────────────────────────────────────────────

    def _compute_day_labels(self, days: list, fut_df: pd.DataFrame) -> dict:
        labels: dict = {}
        BULL = 0.003
        BEAR = -0.003
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
            if   ret >  BULL: labels[day] = 'FII_BULL'
            elif ret <  BEAR: labels[day] = 'FII_BEAR'
            else:             labels[day] = 'DII_MIXED'
        counts = {l: sum(1 for v in labels.values() if v == l)
                  for l in ['FII_BULL', 'FII_BEAR', 'DII_MIXED']}
        log.info("Labels: %s", counts)
        return labels

    # ── FII cash normaliser ───────────────────────────────────────────────────

    def _fii_cash_norm_for_day(self, day: str, fii_cash_df: Optional[pd.DataFrame]) -> float:
        if fii_cash_df is None or fii_cash_df.empty:
            return 0.0
        td    = pd.Timestamp(day).date()
        prior = fii_cash_df[fii_cash_df['date'] < td].tail(5)
        if prior.empty:
            return 0.0
        return float(np.clip(prior['fpi_net'].sum() / 20_000.0, -3.0, 3.0))

    # ── Day feature extraction ────────────────────────────────────────────────

    def _extract_day_features(
        self, day: str, oi_cache: dict, fut_df: pd.DataFrame,
        fii_cash_5d_norm: float, cfg: dict,
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
        atm_strike = int(round(spot_proxy / cfg['strike_step']) * cfg['strike_step'])
        n_bars     = len(day_fut)

        def _align(df_s, col):
            if df_s is None or df_s.empty or col not in df_s.columns:
                return np.full(n_bars, np.nan)
            arr = df_s[col].values.astype(float)
            if len(arr) >= n_bars:
                return arr[:n_bars]
            pad = np.full(n_bars, arr[-1] if len(arr) > 0 else np.nan)
            pad[:len(arr)] = arr
            return pad

        ce_oi_all = {}; pe_oi_all = {}
        ce_cl_all = {}; pe_cl_all = {}
        for strike in strikes:
            ce_df = day_oi[strike].get('CE')
            pe_df = day_oi[strike].get('PE')
            ce_oi_all[strike] = _align(ce_df, 'oi')
            pe_oi_all[strike] = _align(pe_df, 'oi')
            ce_cl_all[strike] = _align(ce_df, 'close')
            pe_cl_all[strike] = _align(pe_df, 'close')

        fut_close  = day_fut['close'].values.astype(float)

        # Running per-bar total |ΔOI|
        bar_oi_adds = [0.0]
        for i in range(1, n_bars):
            bar_add = 0.0
            for s in strikes:
                for arr in [ce_oi_all[s], pe_oi_all[s]]:
                    if i < len(arr):
                        v = arr[i] - arr[i-1]
                        if np.isfinite(v):
                            bar_add += abs(v)
            bar_oi_adds.append(bar_add)

        features = []
        for end in range(WINDOW, n_bars):
            start       = end - WINDOW
            running_avg = max(float(np.mean(bar_oi_adds[:end])) if end > 0 else 1.0, 1.0)

            def _win(d):
                return {s: list(arr[start:end]) for s, arr in d.items()}

            feat = compute_features(
                ce_oi_window     = _win(ce_oi_all),
                pe_oi_window     = _win(pe_oi_all),
                ce_close_window  = _win(ce_cl_all),
                pe_close_window  = _win(pe_cl_all),
                fut_close_window = list(fut_close[start:end]),
                atm_strike       = atm_strike,
                daily_avg_oi_add = running_avg,
                fii_cash_5d_norm = fii_cash_5d_norm,
                atm_band         = cfg['atm_band'],
                otm_min          = cfg['otm_min'],
                otm_max          = cfg['otm_max'],
            )
            if feat is not None:
                features.append(feat)
        return features

    # ── Threshold fitting ─────────────────────────────────────────────────────

    def _fit_thresholds(self, features_by_label: dict) -> dict:
        thresholds = {
            'feature_stats':   {},
            'global_pct':      {},
            'label_centroids': {},
            'feature_names':   FEATURE_NAMES,
        }
        all_vals = {f: [] for f in FEATURE_NAMES}
        for label, feat_list in features_by_label.items():
            for feat in feat_list:
                for f in FEATURE_NAMES:
                    v = feat.get(f, np.nan)
                    if np.isfinite(v):
                        all_vals[f].append(v)

        for fn in FEATURE_NAMES:
            vals = all_vals[fn]
            if not vals:
                log.warning("Feature '%s' has no finite values.", fn)
                continue
            arr = np.array(vals)
            thresholds['global_pct'][fn] = {
                p: float(np.percentile(arr, int(p[1:])))
                for p in ['p02','p10','p25','p50','p75','p90','p98']
            }
            label_stats = {}
            for label, feat_list in features_by_label.items():
                lv = np.array([f[fn] for f in feat_list if np.isfinite(f.get(fn, np.nan))])
                if len(lv) == 0:
                    label_stats[label] = {'median': float(np.median(arr)), 'std': 1.0}
                else:
                    label_stats[label] = {'median': float(np.median(lv)),
                                          'std':    float(max(np.std(lv), 1e-6))}
            thresholds['feature_stats'][fn] = label_stats

        for label, feat_list in features_by_label.items():
            if not feat_list:
                continue
            centroid = {}
            for fn in FEATURE_NAMES:
                lv = np.array([f[fn] for f in feat_list if np.isfinite(f.get(fn, np.nan))])
                centroid[fn] = float(np.median(lv)) if len(lv) > 0 else 0.0
            thresholds['label_centroids'][label] = centroid
        return thresholds

    # ── Main calibrate ────────────────────────────────────────────────────────

    def calibrate(self) -> dict:
        log.info("FIIDIICalibrator (COMBINED): loading caches for %s", self.instruments)
        fii_cash_df = self._load_fii_cash()

        features_by_label: dict = {'FII_BULL': [], 'FII_BEAR': [], 'DII_MIXED': []}
        all_training_days: list = []

        for inst in self.instruments:
            cfg                  = _INST_CFG[inst]
            oi_cache, fut_df, _  = self._load_instrument(inst)
            days                 = sorted(self.training_days.get(inst, oi_cache.keys()))
            labels               = self._compute_day_labels(days, fut_df)

            log.info("[%s] Calibrating on %d days: %s … %s",
                     inst, len(days), days[0], days[-1])

            for day in days:
                label    = labels.get(day, 'DII_MIXED')
                cash_n   = self._fii_cash_norm_for_day(day, fii_cash_df)
                feats    = self._extract_day_features(day, oi_cache, fut_df, cash_n, cfg)
                features_by_label[label].extend(feats)
                log.info("[%s] day=%s label=%-10s fii_cash_n=%+.2f vecs=%d",
                         inst, day, label, cash_n, len(feats))
            all_training_days.extend([f"{inst}:{d}" for d in days])

        counts = {l: len(v) for l, v in features_by_label.items()}
        log.info("Combined feature vectors per label: %s", counts)

        for label, feats in features_by_label.items():
            if len(feats) < 20:
                log.warning("Label '%s' only %d vectors — thresholds unreliable.", label, len(feats))

        thresholds = self._fit_thresholds(features_by_label)
        thresholds['training_days'] = all_training_days
        thresholds['label_counts']  = counts
        thresholds['instruments']   = self.instruments

        THRESHOLDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(THRESHOLDS_FILE, 'w') as fh:
            json.dump(thresholds, fh, indent=2)

        log.info("Combined thresholds written to %s", THRESHOLDS_FILE)
        return thresholds


# ═══════════════════════════════════════════════════════════════════════════════
# OI Snapshot
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class OISnapshot:
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
# FIIDIIClassifier — Combined runtime
# ═══════════════════════════════════════════════════════════════════════════════

class FIIDIIClassifier:
    """
    Runtime classifier using combined thresholds.
    Uses per-instrument ATM/OTM constants for feature extraction.

    Usage
    -----
    clf = FIIDIIClassifier(instrument='BANKNIFTY')
    clf.push(snapshot)
    result = clf.classify(fii_cash_5d_norm=...)
    """

    def __init__(self, thresholds_path: Optional[Path] = None,
                 instrument: str = 'NIFTY'):
        path = thresholds_path or THRESHOLDS_FILE
        if not path.exists():
            raise FileNotFoundError(
                f"Combined thresholds not found: {path}. "
                "Run FIIDIICalibrator().calibrate() first."
            )
        with open(path) as fh:
            self._thresh = json.load(fh)

        if instrument not in _INST_CFG:
            raise ValueError(f"Unknown instrument '{instrument}'. Choose from {list(_INST_CFG)}")
        self._cfg = _INST_CFG[instrument]

        self._buffer: deque            = deque(maxlen=BUFFER_BARS)
        self._daily_oi_adds: list      = []
        self._prev_snap: Optional[OISnapshot] = None

        log.info(
            "FIIDIIClassifier (COMBINED) loaded  instrument=%s  trained_days=%d  label_counts=%s",
            instrument,
            len(self._thresh.get('training_days', [])),
            self._thresh.get('label_counts', {}),
        )

    def push(self, snap: OISnapshot) -> None:
        if self._prev_snap is not None:
            bar_add = sum(
                abs(snap.ce_oi.get(s, 0.0) - self._prev_snap.ce_oi.get(s, 0.0)) +
                abs(snap.pe_oi.get(s, 0.0) - self._prev_snap.pe_oi.get(s, 0.0))
                for s in snap.strikes
            )
            self._daily_oi_adds.append(bar_add)
        self._buffer.append(snap)
        self._prev_snap = snap

    def _build_windows(self) -> Optional[tuple]:
        n = len(self._buffer)
        if n < MIN_WINDOW:
            return None
        snaps   = list(self._buffer)[-WINDOW:]
        latest  = snaps[-1]
        atm     = latest.atm_strike

        ce_oi_w = {s: [] for s in latest.strikes}
        pe_oi_w = {s: [] for s in latest.strikes}
        ce_cl_w = {s: [] for s in latest.strikes}
        pe_cl_w = {s: [] for s in latest.strikes}
        fut_w   = []

        for snap in snaps:
            fut_w.append(snap.fut_close)
            for s in latest.strikes:
                ce_oi_w[s].append(snap.ce_oi.get(s, np.nan))
                pe_oi_w[s].append(snap.pe_oi.get(s, np.nan))
                ce_cl_w[s].append(snap.ce_close.get(s, np.nan))
                pe_cl_w[s].append(snap.pe_close.get(s, np.nan))

        daily_avg = float(np.mean(self._daily_oi_adds)) if self._daily_oi_adds else 1.0
        return ce_oi_w, pe_oi_w, ce_cl_w, pe_cl_w, fut_w, atm, daily_avg

    def _score_features(self, features: dict) -> dict:
        feat_stats = self._thresh.get('feature_stats', {})
        global_pct = self._thresh.get('global_pct', {})
        labels     = ['FII_BULL', 'FII_BEAR', 'DII_MIXED']
        log_scores = {l: 0.0 for l in labels}

        for fn in FEATURE_NAMES:
            val = features.get(fn, np.nan)
            if not np.isfinite(val) or fn not in feat_stats:
                continue
            pct = global_pct.get(fn, {})
            val = float(np.clip(val, pct.get('p02', -np.inf), pct.get('p98', np.inf)))
            for label in labels:
                stats = feat_stats[fn].get(label, {'median': 0.0, 'std': 1.0})
                mu    = stats['median']
                sigma = max(stats['std'], 1e-6)
                log_scores[label] -= 0.5 * ((val - mu) / sigma) ** 2

        max_ls = max(log_scores.values())
        exp_s  = {l: np.exp(s - max_ls) for l, s in log_scores.items()}
        tot    = sum(exp_s.values())
        return {l: float(v / tot) for l, v in exp_s.items()}

    def classify(self, fii_cash_5d_norm: float = 0.0) -> dict:
        _null = {'fii_score': 0.0, 'dii_score': 0.0, 'retail_score': 0.0,
                 'attribution': UNKNOWN, 'direction': 0, 'confidence': 0.0, 'features': {}}

        windows = self._build_windows()
        if windows is None:
            return {**_null, 'attribution': UNKNOWN}

        ce_oi_w, pe_oi_w, ce_cl_w, pe_cl_w, fut_w, atm, daily_avg = windows

        features = compute_features(
            ce_oi_window     = ce_oi_w,
            pe_oi_window     = pe_oi_w,
            ce_close_window  = ce_cl_w,
            pe_close_window  = pe_cl_w,
            fut_close_window = fut_w,
            atm_strike       = atm,
            daily_avg_oi_add = daily_avg,
            fii_cash_5d_norm = fii_cash_5d_norm,
            atm_band         = self._cfg['atm_band'],
            otm_min          = self._cfg['otm_min'],
            otm_max          = self._cfg['otm_max'],
        )
        if features is None:
            return {**_null, 'attribution': UNKNOWN}

        probs      = self._score_features(features)
        fii_bull_p = probs.get('FII_BULL', 0.33)
        fii_bear_p = probs.get('FII_BEAR', 0.33)
        dii_p      = probs.get('DII_MIXED', 0.33)

        fii_net      = float(fii_bull_p - fii_bear_p)
        fii_total    = float(fii_bull_p + fii_bear_p)
        coverage     = features.get('strike_coverage', 0.5)
        intensity    = min(1.0, features.get('oi_add_intensity', 1.0))
        retail_score = float(coverage * 0.6 + (1.0 - min(1.0, intensity)) * 0.4)
        ce_pe        = features.get('ce_pe_imbalance', 0.0)

        if retail_score > 0.7 and fii_total < 0.5:
            attribution, direction, confidence = RETAIL, 0, float(retail_score * 0.4)
        elif fii_bull_p > fii_bear_p and fii_bull_p > dii_p:
            attribution, direction = FII_BULL, 1
            confidence = float(fii_bull_p * min(1.0, fii_net + 0.2))
        elif fii_bear_p > fii_bull_p and fii_bear_p > dii_p:
            attribution, direction = FII_BEAR, -1
            confidence = float(fii_bear_p * min(1.0, -fii_net + 0.2))
        elif dii_p >= max(fii_bull_p, fii_bear_p):
            if ce_pe > 0.05:
                attribution, direction, confidence = DII_BULL, 1, float(dii_p * 0.5)
            elif ce_pe < -0.05:
                attribution, direction, confidence = DII_BEAR, -1, float(dii_p * 0.5)
            else:
                attribution, direction, confidence = MIXED, 0, float(dii_p * 0.25)
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
        self._buffer.clear()
        self._daily_oi_adds.clear()
        self._prev_snap = None
        log.debug("FIIDIIClassifier (COMBINED) buffer reset")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    instruments = ['NIFTY', 'BANKNIFTY']
    for arg in sys.argv[1:]:
        if arg.startswith('--instruments='):
            instruments = arg.split('=', 1)[1].split(',')

    cal    = FIIDIICalibrator(instruments=instruments)
    thresh = cal.calibrate()

    print(f"\n{'='*65}")
    print(f"Combined calibration complete — instruments: {thresh['instruments']}")
    print(f"Training day refs: {len(thresh['training_days'])}")
    print(f"Label counts: {thresh['label_counts']}")
    print(f"\nLabel centroids:")
    print(f"{'Feature':<22} {'FII_BULL':>10} {'FII_BEAR':>10} {'DII_MIXED':>10}")
    print('-' * 55)
    for fn in thresh.get('feature_names', FEATURE_NAMES):
        stats = thresh.get('feature_stats', {}).get(fn, {})
        row = {l: stats.get(l, {}).get('median', 0.0) for l in ['FII_BULL','FII_BEAR','DII_MIXED']}
        print(f"  {fn:<20} {row['FII_BULL']:>+10.4f} {row['FII_BEAR']:>+10.4f} {row['DII_MIXED']:>+10.4f}")
    print(f"\nThresholds saved to: {THRESHOLDS_FILE}")
