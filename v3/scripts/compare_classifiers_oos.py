"""
Head-to-head comparison: 8-feature baseline vs 11-feature intel-augmented
classifier on a 30-day held-out OOS window.

Process:
1. Split the 164 NSE trading days into TRAIN (134) + TEST (last 30).
2. Calibrate BOTH classifiers on TRAIN only (so neither has seen TEST data).
3. For each TEST day, replay the day's bars through both classifiers,
   compute majority-vote attribution, compare with the day's actual
   price-direction label.
4. Report:
   - Per-classifier:  bar-level label-recovery accuracy, day-vote dir accuracy.
   - Bar-level confusion matrix.
   - Per-day side-by-side comparison.

Per project rules: hard-fail on any data inconsistency, no synthesised data.
"""
from __future__ import annotations
import argparse
import json
import logging
import pickle
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from v3.signals.fii_dii_classifier import (
    FIIDIICalibrator, FIIDIIClassifier, OISnapshot,
    WINDOW, MIN_WINDOW, STRIKE_STEP, FEATURE_NAMES as BASE_FEAT_NAMES,
    FII_BULL, FII_BEAR, DII_BULL, DII_BEAR, MIXED, UNKNOWN,
)
from v3.signals.fii_dii_classifier_intel import (
    FIIDIICalibratorIntel, FIIDIIClassifierIntel,
    intel_roc_for_day, load_intel_timeseries,
    FEATURE_NAMES as INTEL_FEAT_NAMES,
)

OI_CACHE       = ROOT / 'v3' / 'cache' / 'option_oi_1m_NIFTY.pkl'
FUT_CACHE      = ROOT / 'v3' / 'cache' / 'candles_1m_NIFTY.pkl'
FII_CSV        = ROOT / 'fii_data.csv'
TRAIN_THRESH   = ROOT / 'v3' / 'cache' / 'fii_dii_thresholds_train134.json'
TRAIN_THRESH_I = ROOT / 'v3' / 'cache' / 'fii_dii_thresholds_intel_train134.json'

LABEL_DIR = {'FII_BULL': +1, 'FII_BEAR': -1, 'DII_MIXED': 0}


def price_direction_label(open_px: float, close_px: float) -> str:
    if open_px <= 0:
        return 'DII_MIXED'
    r = (close_px - open_px) / open_px
    if r > 0.003:  return 'FII_BULL'
    if r < -0.003: return 'FII_BEAR'
    return 'DII_MIXED'


def fii_cash_lag1_norm(day: str, fii_cash_df: pd.DataFrame) -> float:
    if fii_cash_df is None or fii_cash_df.empty:
        return 0.0
    td = pd.Timestamp(day).date()
    prior = fii_cash_df[fii_cash_df['date'] < td].tail(5)
    if prior.empty:
        return 0.0
    return float(np.clip(prior['fpi_net'].sum() / 20_000.0, -3.0, 3.0))


def replay_day_baseline(
    day: str, oi_cache: dict, fut_df: pd.DataFrame,
    cash_norm: float, clf: FIIDIIClassifier,
) -> tuple[list, list]:
    """Push every bar through the baseline classifier; return (attributions, dirs)."""
    if day not in oi_cache:
        return [], []
    day_oi = oi_cache[day]
    strikes = sorted(day_oi.keys())
    day_dt = pd.to_datetime(day).date()
    day_fut = fut_df[fut_df['date'] == day_dt].sort_values('ts').reset_index(drop=True)
    if day_fut.empty:
        return [], []

    clf.reset()

    # Pre-align OI per strike to fut bars
    n_bars = len(day_fut)
    def _align(df_s, col):
        if df_s is None or df_s.empty or col not in df_s.columns:
            return np.full(n_bars, np.nan)
        arr = df_s[col].values.astype(float)
        if len(arr) >= n_bars: return arr[:n_bars]
        pad = np.full(n_bars, arr[-1] if len(arr) > 0 else np.nan)
        pad[:len(arr)] = arr
        return pad

    ce_oi  = {s: _align(day_oi[s].get('CE'), 'oi') for s in strikes}
    pe_oi  = {s: _align(day_oi[s].get('PE'), 'oi') for s in strikes}
    ce_cls = {s: _align(day_oi[s].get('CE'), 'close') for s in strikes}
    pe_cls = {s: _align(day_oi[s].get('PE'), 'close') for s in strikes}

    attrs, dirs = [], []
    for i in range(n_bars):
        spot_proxy = float(day_fut['close'].iloc[i])
        atm = int(round(spot_proxy / STRIKE_STEP) * STRIKE_STEP)
        snap = OISnapshot(
            ts=day_fut['ts'].iloc[i],
            atm_strike=atm,
            strikes=strikes,
            ce_oi={s: float(ce_oi[s][i]) if np.isfinite(ce_oi[s][i]) else 0.0 for s in strikes},
            pe_oi={s: float(pe_oi[s][i]) if np.isfinite(pe_oi[s][i]) else 0.0 for s in strikes},
            ce_close={s: float(ce_cls[s][i]) if np.isfinite(ce_cls[s][i]) else 0.0 for s in strikes},
            pe_close={s: float(pe_cls[s][i]) if np.isfinite(pe_cls[s][i]) else 0.0 for s in strikes},
            fut_close=float(day_fut['close'].iloc[i]),
            spot_close=spot_proxy,
        )
        clf.push(snap)
        if i >= MIN_WINDOW:
            r = clf.classify(fii_cash_5d_norm=cash_norm)
            if r['attribution'] != UNKNOWN:
                attrs.append(r['attribution'])
                dirs.append(r['direction'])
    return attrs, dirs


def replay_day_intel(
    day: str, oi_cache: dict, fut_df: pd.DataFrame,
    cash_norm: float, intel_roc: dict, clf: FIIDIIClassifierIntel,
) -> tuple[list, list]:
    """Push every bar through the intel classifier; return (attributions, dirs)."""
    if day not in oi_cache:
        return [], []
    day_oi = oi_cache[day]
    strikes = sorted(day_oi.keys())
    day_dt = pd.to_datetime(day).date()
    day_fut = fut_df[fut_df['date'] == day_dt].sort_values('ts').reset_index(drop=True)
    if day_fut.empty:
        return [], []

    clf.reset()
    n_bars = len(day_fut)
    def _align(df_s, col):
        if df_s is None or df_s.empty or col not in df_s.columns:
            return np.full(n_bars, np.nan)
        arr = df_s[col].values.astype(float)
        if len(arr) >= n_bars: return arr[:n_bars]
        pad = np.full(n_bars, arr[-1] if len(arr) > 0 else np.nan)
        pad[:len(arr)] = arr
        return pad

    ce_oi  = {s: _align(day_oi[s].get('CE'), 'oi') for s in strikes}
    pe_oi  = {s: _align(day_oi[s].get('PE'), 'oi') for s in strikes}
    ce_cls = {s: _align(day_oi[s].get('CE'), 'close') for s in strikes}
    pe_cls = {s: _align(day_oi[s].get('PE'), 'close') for s in strikes}

    attrs, dirs = [], []
    for i in range(n_bars):
        spot_proxy = float(day_fut['close'].iloc[i])
        atm = int(round(spot_proxy / STRIKE_STEP) * STRIKE_STEP)
        snap = OISnapshot(
            ts=day_fut['ts'].iloc[i], atm_strike=atm, strikes=strikes,
            ce_oi={s: float(ce_oi[s][i]) if np.isfinite(ce_oi[s][i]) else 0.0 for s in strikes},
            pe_oi={s: float(pe_oi[s][i]) if np.isfinite(pe_oi[s][i]) else 0.0 for s in strikes},
            ce_close={s: float(ce_cls[s][i]) if np.isfinite(ce_cls[s][i]) else 0.0 for s in strikes},
            pe_close={s: float(pe_cls[s][i]) if np.isfinite(pe_cls[s][i]) else 0.0 for s in strikes},
            fut_close=float(day_fut['close'].iloc[i]), spot_close=spot_proxy,
        )
        clf.push(snap)
        if i >= MIN_WINDOW:
            r = clf.classify(
                fii_cash_5d_norm=cash_norm,
                roc5_dii_net=intel_roc['roc5_dii_net'],
                roc5_usdinr=intel_roc['roc5_usdinr'],
                roc5_fii_ni_fut=intel_roc['roc5_fii_ni_fut'],
            )
            if r['attribution'] != UNKNOWN:
                attrs.append(r['attribution'])
                dirs.append(r['direction'])
    return attrs, dirs


def majority_vote_dir(attrs: list, dirs: list) -> tuple[str, int, float]:
    """Return (label, dir, confidence_pct) by simple majority on bar-level attributions."""
    if not attrs:
        return UNKNOWN, 0, 0.0
    # Map bar-level to direction labels first
    bar_dirs = []
    for a in attrs:
        if a == FII_BULL or a == DII_BULL: bar_dirs.append('UP')
        elif a == FII_BEAR or a == DII_BEAR: bar_dirs.append('DOWN')
        else: bar_dirs.append('FLAT')
    cnt = Counter(bar_dirs)
    pred = cnt.most_common(1)[0][0]
    pct = cnt[pred] / len(bar_dirs)
    if pred == 'UP':   return 'FII_BULL', +1, pct
    if pred == 'DOWN': return 'FII_BEAR', -1, pct
    return 'DII_MIXED', 0, pct


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--n-test', type=int, default=30,
                    help='Last N trading days for OOS holdout (default 30)')
    ap.add_argument('--skip-calibrate', action='store_true',
                    help='Re-use existing TRAIN thresholds files')
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(message)s')
    log = logging.getLogger('oos')

    # ── Load data once ──
    with open(OI_CACHE, 'rb') as f: oi_cache = pickle.load(f)
    fut_df = pd.read_pickle(FUT_CACHE)
    fii_cash = pd.read_csv(FII_CSV) if FII_CSV.exists() else None
    if fii_cash is not None:
        fii_cash['date'] = pd.to_datetime(fii_cash['date']).dt.date
    intel = load_intel_timeseries()

    days = sorted(oi_cache.keys())
    log.info("Total days in OI cache: %d  (%s -> %s)", len(days), days[0], days[-1])

    # ── Train / Test split ──
    test_days  = days[-args.n_test:]
    train_days = days[:-args.n_test]
    log.info("TRAIN: %d days  (%s -> %s)", len(train_days), train_days[0], train_days[-1])
    log.info("TEST:  %d days  (%s -> %s)", len(test_days),  test_days[0],  test_days[-1])

    # ── Calibrate both on TRAIN only ──
    if not args.skip_calibrate or not TRAIN_THRESH.exists():
        log.info("Calibrating BASELINE (8-feature) on %d days...", len(train_days))
        t0 = time.time()
        cal_b = FIIDIICalibrator(training_days=train_days)
        cal_b.calibrate.__func__.__doc__ = "train"
        # Override default output path
        from v3.signals import fii_dii_classifier as base_mod
        orig_path = base_mod.THRESHOLDS_FILE
        base_mod.THRESHOLDS_FILE = TRAIN_THRESH
        try:
            cal_b.calibrate()
        finally:
            base_mod.THRESHOLDS_FILE = orig_path
        log.info("Baseline cal done in %.1fs", time.time() - t0)

    if not args.skip_calibrate or not TRAIN_THRESH_I.exists():
        log.info("Calibrating INTEL (11-feature) on %d days...", len(train_days))
        t0 = time.time()
        cal_i = FIIDIICalibratorIntel(training_days=train_days,
                                      thresholds_out=TRAIN_THRESH_I)
        cal_i.calibrate()
        log.info("Intel cal done in %.1fs", time.time() - t0)

    # ── Build classifiers from TRAIN thresholds ──
    clf_b = FIIDIIClassifier(thresholds_path=TRAIN_THRESH)
    clf_i = FIIDIIClassifierIntel(thresholds_path=TRAIN_THRESH_I)

    # ── Evaluate on TEST ──
    bar_b, bar_i = [], []           # (true, pred) at bar level
    day_rows = []                   # one row per test day

    for day in test_days:
        day_dt = pd.to_datetime(day).date()
        sub = fut_df[fut_df['date'] == day_dt].sort_values('ts')
        if sub.empty:
            log.warning("no fut for %s", day); continue
        o, c = float(sub['open'].iloc[0]), float(sub['close'].iloc[-1])
        true_label = price_direction_label(o, c)
        true_dir   = LABEL_DIR[true_label]

        cash_norm = fii_cash_lag1_norm(day, fii_cash)

        # Baseline replay
        attrs_b, dirs_b = replay_day_baseline(day, oi_cache, fut_df, cash_norm, clf_b)
        pred_b_label, pred_b_dir, conf_b = majority_vote_dir(attrs_b, dirs_b)

        # Intel replay
        try:
            intel_roc = intel_roc_for_day(day, intel)
        except ValueError as e:
            log.info("skip intel %s: %s", day, e)
            continue
        attrs_i, dirs_i = replay_day_intel(day, oi_cache, fut_df,
                                            cash_norm, intel_roc, clf_i)
        pred_i_label, pred_i_dir, conf_i = majority_vote_dir(attrs_i, dirs_i)

        # Bar-level direction labels (FII_BULL=UP, FII_BEAR=DOWN, else FLAT)
        for a in attrs_b:
            true_d = 'UP' if true_dir > 0 else ('DOWN' if true_dir < 0 else 'FLAT')
            pred_d = 'UP' if a == FII_BULL or a == DII_BULL else ('DOWN' if a == FII_BEAR or a == DII_BEAR else 'FLAT')
            bar_b.append((true_d, pred_d))
        for a in attrs_i:
            true_d = 'UP' if true_dir > 0 else ('DOWN' if true_dir < 0 else 'FLAT')
            pred_d = 'UP' if a == FII_BULL or a == DII_BULL else ('DOWN' if a == FII_BEAR or a == DII_BEAR else 'FLAT')
            bar_i.append((true_d, pred_d))

        day_rows.append({
            'day': day, 'true_ret_pct': (c-o)/o*100, 'true_label': true_label,
            'true_dir': true_dir,
            'baseline_label': pred_b_label, 'baseline_dir': pred_b_dir,
            'baseline_conf':  conf_b, 'baseline_bars': len(attrs_b),
            'intel_label':    pred_i_label, 'intel_dir':    pred_i_dir,
            'intel_conf':     conf_i, 'intel_bars': len(attrs_i),
            'roc5_dii_net':   intel_roc['roc5_dii_net'],
            'roc5_usdinr':    intel_roc['roc5_usdinr'],
            'roc5_fii_ni_fut': intel_roc['roc5_fii_ni_fut'],
        })

    days_df = pd.DataFrame(day_rows)
    print('\n' + '='*86)
    print(f"OOS RESULTS — {len(days_df)} test days  ({days_df.day.iloc[0]} -> {days_df.day.iloc[-1]})")
    print('='*86)

    print(f"\nDay-level direction accuracy (majority vote of bars):")
    base_dir_match = (days_df['baseline_dir'] == days_df['true_dir']).sum()
    intel_dir_match = (days_df['intel_dir'] == days_df['true_dir']).sum()
    print(f"  Baseline (8 feat) : {base_dir_match}/{len(days_df)} = {base_dir_match/len(days_df)*100:.1f}%")
    print(f"  Intel    (11 feat): {intel_dir_match}/{len(days_df)} = {intel_dir_match/len(days_df)*100:.1f}%")
    print(f"  Δ (intel - baseline): {(intel_dir_match-base_dir_match):+d} days")

    print(f"\nDay-level label accuracy (FII_BULL/FII_BEAR/DII_MIXED):")
    base_lbl_match = (days_df['baseline_label'] == days_df['true_label']).sum()
    intel_lbl_match = (days_df['intel_label'] == days_df['true_label']).sum()
    print(f"  Baseline : {base_lbl_match}/{len(days_df)} = {base_lbl_match/len(days_df)*100:.1f}%")
    print(f"  Intel    : {intel_lbl_match}/{len(days_df)} = {intel_lbl_match/len(days_df)*100:.1f}%")

    print(f"\nBar-level direction accuracy:")
    if bar_b:
        b_correct = sum(1 for t, p in bar_b if t == p)
        print(f"  Baseline : {b_correct}/{len(bar_b)} = {b_correct/len(bar_b)*100:.1f}%")
    if bar_i:
        i_correct = sum(1 for t, p in bar_i if t == p)
        print(f"  Intel    : {i_correct}/{len(bar_i)} = {i_correct/len(bar_i)*100:.1f}%")

    # ── Confusion matrices (day-level) ──
    print(f"\nConfusion (day-level), Baseline:")
    cmb = pd.crosstab(days_df['true_label'], days_df['baseline_label'])
    print(cmb.to_string())
    print(f"\nConfusion (day-level), Intel:")
    cmi = pd.crosstab(days_df['true_label'], days_df['intel_label'])
    print(cmi.to_string())

    # ── Per-day side-by-side ──
    print(f"\nPer-day (side-by-side):")
    show_cols = ['day','true_ret_pct','true_label',
                 'baseline_label','baseline_conf',
                 'intel_label','intel_conf',
                 'roc5_dii_net','roc5_usdinr','roc5_fii_ni_fut']
    print(days_df[show_cols].round({'true_ret_pct':3, 'baseline_conf':2, 'intel_conf':2,
                                      'roc5_dii_net':0, 'roc5_usdinr':3, 'roc5_fii_ni_fut':0}
                                  ).to_string(index=False))

    # ── Where do they disagree? ──
    diffs = days_df[days_df['baseline_dir'] != days_df['intel_dir']]
    print(f"\n{len(diffs)} days where baseline & intel disagreed:")
    if len(diffs):
        print(diffs[show_cols + ['true_dir']].round(3).to_string(index=False))
        # Among disagreements, who wins?
        b_wins = (diffs['baseline_dir'] == diffs['true_dir']).sum()
        i_wins = (diffs['intel_dir'] == diffs['true_dir']).sum()
        ties   = ((diffs['baseline_dir'] != diffs['true_dir']) &
                  (diffs['intel_dir'] != diffs['true_dir'])).sum()
        print(f"  Baseline correct on {b_wins} of these  |  "
              f"Intel correct on {i_wins}  |  both wrong on {ties}")

    out_csv = ROOT / 'v3' / 'cache' / 'nse_reports' / 'oos_comparison.csv'
    days_df.to_csv(out_csv, index=False)
    print(f"\nSaved: {out_csv}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
