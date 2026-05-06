"""
Walk-forward cross-validation: 4 sequential folds.

For each fold:
  fold 1: train = days[0:80],   test = days[80:100]
  fold 2: train = days[0:100],  test = days[100:120]
  fold 3: train = days[0:120],  test = days[120:140]
  fold 4: train = days[0:140],  test = days[140:164]

Per fold: calibrate baseline (8-feat) and intel (11-feat) on train,
replay test bars through both, compute day-level dir accuracy.

Checkpointed: each fold saves a JSON result so the script can resume
across the bash 45s budget.

Usage (run repeatedly until all 4 folds done):
    python3 v3/scripts/walk_forward_cv.py            # process next pending fold
    python3 v3/scripts/walk_forward_cv.py --fold 2   # specific fold
    python3 v3/scripts/walk_forward_cv.py --summary  # aggregate results
"""
from __future__ import annotations
import argparse
import json
import logging
import pickle
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from v3.signals.fii_dii_classifier import (
    FIIDIICalibrator, FIIDIIClassifier, OISnapshot,
    MIN_WINDOW, STRIKE_STEP, FII_BULL, FII_BEAR, DII_BULL, DII_BEAR, MIXED, UNKNOWN,
)
from v3.signals.fii_dii_classifier_intel import (
    FIIDIICalibratorIntel, FIIDIIClassifierIntel,
    intel_roc_for_day, load_intel_timeseries,
)

OI_CACHE   = ROOT / 'v3' / 'cache' / 'option_oi_1m_NIFTY.pkl'
FUT_CACHE  = ROOT / 'v3' / 'cache' / 'candles_1m_NIFTY.pkl'
FII_CSV    = ROOT / 'fii_data.csv'
CV_DIR     = ROOT / 'v3' / 'cache' / 'nse_reports' / 'walk_forward_cv'

# Same fold splits across all calls
FOLDS = [
    {'fold': 1, 'train_end': 80,  'test_end': 100},
    {'fold': 2, 'train_end': 100, 'test_end': 120},
    {'fold': 3, 'train_end': 120, 'test_end': 140},
    {'fold': 4, 'train_end': 140, 'test_end': 164},
]

LABEL_DIR = {'FII_BULL': +1, 'FII_BEAR': -1, 'DII_MIXED': 0}


def price_dir_label(o, c):
    if o <= 0: return 'DII_MIXED'
    r = (c - o) / o
    return 'FII_BULL' if r > 0.003 else ('FII_BEAR' if r < -0.003 else 'DII_MIXED')


def fii_cash_lag1_norm(day, fii_cash_df):
    if fii_cash_df is None or fii_cash_df.empty: return 0.0
    td = pd.Timestamp(day).date()
    prior = fii_cash_df[fii_cash_df['date'] < td].tail(5)
    if prior.empty: return 0.0
    return float(np.clip(prior['fpi_net'].sum() / 20_000.0, -3.0, 3.0))


def replay_day_baseline(day, oi_cache, fut_df, cash_norm, clf):
    if day not in oi_cache: return [], []
    day_oi = oi_cache[day]
    strikes = sorted(day_oi.keys())
    day_dt = pd.to_datetime(day).date()
    day_fut = fut_df[fut_df['date'] == day_dt].sort_values('ts').reset_index(drop=True)
    if day_fut.empty: return [], []
    clf.reset()
    n = len(day_fut)
    def _al(df_s, col):
        if df_s is None or df_s.empty or col not in df_s.columns:
            return np.full(n, np.nan)
        a = df_s[col].values.astype(float)
        if len(a) >= n: return a[:n]
        p = np.full(n, a[-1] if len(a) > 0 else np.nan); p[:len(a)] = a; return p
    ce_oi  = {s: _al(day_oi[s].get('CE'), 'oi')    for s in strikes}
    pe_oi  = {s: _al(day_oi[s].get('PE'), 'oi')    for s in strikes}
    ce_cls = {s: _al(day_oi[s].get('CE'), 'close') for s in strikes}
    pe_cls = {s: _al(day_oi[s].get('PE'), 'close') for s in strikes}
    attrs = []
    for i in range(n):
        spot = float(day_fut['close'].iloc[i])
        atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)
        snap = OISnapshot(
            ts=day_fut['ts'].iloc[i], atm_strike=atm, strikes=strikes,
            ce_oi={s: float(ce_oi[s][i]) if np.isfinite(ce_oi[s][i]) else 0.0 for s in strikes},
            pe_oi={s: float(pe_oi[s][i]) if np.isfinite(pe_oi[s][i]) else 0.0 for s in strikes},
            ce_close={s: float(ce_cls[s][i]) if np.isfinite(ce_cls[s][i]) else 0.0 for s in strikes},
            pe_close={s: float(pe_cls[s][i]) if np.isfinite(pe_cls[s][i]) else 0.0 for s in strikes},
            fut_close=float(day_fut['close'].iloc[i]), spot_close=spot,
        )
        clf.push(snap)
        if i >= MIN_WINDOW:
            r = clf.classify(fii_cash_5d_norm=cash_norm)
            if r['attribution'] != UNKNOWN:
                attrs.append(r['attribution'])
    return attrs


def replay_day_intel(day, oi_cache, fut_df, cash_norm, intel_roc, clf):
    if day not in oi_cache: return []
    day_oi = oi_cache[day]
    strikes = sorted(day_oi.keys())
    day_dt = pd.to_datetime(day).date()
    day_fut = fut_df[fut_df['date'] == day_dt].sort_values('ts').reset_index(drop=True)
    if day_fut.empty: return []
    clf.reset()
    n = len(day_fut)
    def _al(df_s, col):
        if df_s is None or df_s.empty or col not in df_s.columns:
            return np.full(n, np.nan)
        a = df_s[col].values.astype(float)
        if len(a) >= n: return a[:n]
        p = np.full(n, a[-1] if len(a) > 0 else np.nan); p[:len(a)] = a; return p
    ce_oi  = {s: _al(day_oi[s].get('CE'), 'oi')    for s in strikes}
    pe_oi  = {s: _al(day_oi[s].get('PE'), 'oi')    for s in strikes}
    ce_cls = {s: _al(day_oi[s].get('CE'), 'close') for s in strikes}
    pe_cls = {s: _al(day_oi[s].get('PE'), 'close') for s in strikes}
    attrs = []
    for i in range(n):
        spot = float(day_fut['close'].iloc[i])
        atm = int(round(spot / STRIKE_STEP) * STRIKE_STEP)
        snap = OISnapshot(
            ts=day_fut['ts'].iloc[i], atm_strike=atm, strikes=strikes,
            ce_oi={s: float(ce_oi[s][i]) if np.isfinite(ce_oi[s][i]) else 0.0 for s in strikes},
            pe_oi={s: float(pe_oi[s][i]) if np.isfinite(pe_oi[s][i]) else 0.0 for s in strikes},
            ce_close={s: float(ce_cls[s][i]) if np.isfinite(ce_cls[s][i]) else 0.0 for s in strikes},
            pe_close={s: float(pe_cls[s][i]) if np.isfinite(pe_cls[s][i]) else 0.0 for s in strikes},
            fut_close=float(day_fut['close'].iloc[i]), spot_close=spot,
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
    return attrs


def majority_dir(attrs):
    if not attrs: return 0
    bar_dirs = []
    for a in attrs:
        if a in (FII_BULL, DII_BULL): bar_dirs.append('UP')
        elif a in (FII_BEAR, DII_BEAR): bar_dirs.append('DOWN')
        else: bar_dirs.append('FLAT')
    cnt = Counter(bar_dirs)
    pred = cnt.most_common(1)[0][0]
    return +1 if pred == 'UP' else (-1 if pred == 'DOWN' else 0)


def run_fold(fold_def, all_days, oi_cache, fut_df, fii_cash, intel,
             do_baseline=True, do_intel=True):
    f = fold_def['fold']
    train_days = all_days[:fold_def['train_end']]
    test_days  = all_days[fold_def['train_end']:fold_def['test_end']]
    log = logging.getLogger(f'fold{f}')
    log.info("fold=%d train=%d days (%s..%s) test=%d days (%s..%s)",
             f, len(train_days), train_days[0], train_days[-1],
             len(test_days), test_days[0], test_days[-1])

    fold_dir = CV_DIR / f"fold_{f:02d}"
    fold_dir.mkdir(parents=True, exist_ok=True)
    base_thresh = fold_dir / 'baseline.json'
    intel_thresh = fold_dir / 'intel.json'
    result_json  = fold_dir / 'result.json'

    # ── Calibrate ────────────────────────────────────────────────────────────
    if do_baseline:
        if base_thresh.exists():
            log.info("baseline thresh exists — reusing")
        else:
            log.info("calibrating baseline...")
            t0 = time.time()
            cal = FIIDIICalibrator(training_days=train_days)
            from v3.signals import fii_dii_classifier as base_mod
            orig = base_mod.THRESHOLDS_FILE
            base_mod.THRESHOLDS_FILE = base_thresh
            try:
                cal.calibrate()
            finally:
                base_mod.THRESHOLDS_FILE = orig
            log.info("baseline cal done in %.1fs", time.time() - t0)

    if do_intel:
        if intel_thresh.exists():
            log.info("intel thresh exists — reusing")
        else:
            log.info("calibrating intel...")
            t0 = time.time()
            cal = FIIDIICalibratorIntel(training_days=train_days,
                                          thresholds_out=intel_thresh)
            cal.calibrate()
            log.info("intel cal done in %.1fs", time.time() - t0)

    # ── Replay test days ─────────────────────────────────────────────────────
    if result_json.exists():
        log.info("results already saved — skipping replay (fold %d)", f)
        return json.loads(result_json.read_text())

    # If either threshold file is missing (calibration not yet run for this
    # fold's classifier), skip replay — caller will rerun with the missing
    # phase.  This makes the script safely resumable across the 45s budget.
    if not base_thresh.exists() or not intel_thresh.exists():
        log.info("replay skipped: baseline.json=%s intel.json=%s",
                 base_thresh.exists(), intel_thresh.exists())
        return None

    log.info("replaying %d test days", len(test_days))
    clf_b = FIIDIIClassifier(thresholds_path=base_thresh)
    clf_i = FIIDIIClassifierIntel(thresholds_path=intel_thresh)
    rows = []
    for day in test_days:
        day_dt = pd.to_datetime(day).date()
        sub = fut_df[fut_df['date'] == day_dt].sort_values('ts')
        if sub.empty: continue
        o, c = float(sub['open'].iloc[0]), float(sub['close'].iloc[-1])
        true_lbl = price_dir_label(o, c)
        true_dir = LABEL_DIR[true_lbl]
        cash = fii_cash_lag1_norm(day, fii_cash)
        try:
            iroc = intel_roc_for_day(day, intel)
        except ValueError:
            continue
        a_b = replay_day_baseline(day, oi_cache, fut_df, cash, clf_b)
        a_i = replay_day_intel(day, oi_cache, fut_df, cash, iroc, clf_i)
        rows.append({
            'day': day, 'true_ret_pct': (c - o) / o * 100,
            'true_dir': true_dir,
            'baseline_dir': majority_dir(a_b),
            'intel_dir':    majority_dir(a_i),
        })

    df = pd.DataFrame(rows)
    n = len(df)
    base_match = int((df['baseline_dir'] == df['true_dir']).sum())
    intel_match = int((df['intel_dir'] == df['true_dir']).sum())
    res = {
        'fold': f, 'n_test_days': n,
        'baseline_correct': base_match, 'baseline_acc': base_match / n if n else 0,
        'intel_correct':    intel_match, 'intel_acc':    intel_match / n if n else 0,
        'delta':            (intel_match - base_match) / n if n else 0,
        'rows': rows,
    }
    result_json.write_text(json.dumps(res, indent=2, default=str))
    log.info("fold=%d  N=%d  baseline=%.1f%%  intel=%.1f%%  Δ=%+.1f pp",
             f, n, res['baseline_acc']*100, res['intel_acc']*100, res['delta']*100)
    return res


def summary():
    print("\n" + "="*70)
    print("WALK-FORWARD CV SUMMARY")
    print("="*70)
    rows = []
    for fd in FOLDS:
        result_json = CV_DIR / f"fold_{fd['fold']:02d}" / 'result.json'
        if not result_json.exists():
            print(f"  fold {fd['fold']}: PENDING")
            continue
        r = json.loads(result_json.read_text())
        rows.append(r)
        print(f"  fold {r['fold']}  N={r['n_test_days']:3d}  "
              f"baseline={r['baseline_acc']*100:5.1f}%  "
              f"intel={r['intel_acc']*100:5.1f}%  "
              f"Δ={r['delta']*100:+5.1f} pp  "
              f"({'INTEL' if r['intel_acc']>r['baseline_acc'] else 'BASE'} wins)")
    if rows:
        n_total = sum(r['n_test_days'] for r in rows)
        b_total = sum(r['baseline_correct'] for r in rows)
        i_total = sum(r['intel_correct']    for r in rows)
        print(f"\n  TOTAL N={n_total}  baseline={b_total/n_total*100:.1f}%  "
              f"intel={i_total/n_total*100:.1f}%  "
              f"Δ={((i_total - b_total)/n_total)*100:+.1f} pp")
        intel_wins = sum(1 for r in rows if r['intel_acc'] > r['baseline_acc'])
        print(f"  Folds intel ≥ baseline: {intel_wins}/{len(rows)}")
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--fold', type=int, default=None,
                    help='Run a specific fold; default = next pending')
    ap.add_argument('--summary', action='store_true')
    ap.add_argument('--baseline-only', action='store_true')
    ap.add_argument('--intel-only', action='store_true')
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s %(message)s')

    if args.summary:
        summary()
        return 0

    with open(OI_CACHE, 'rb') as f: oi_cache = pickle.load(f)
    fut_df = pd.read_pickle(FUT_CACHE)
    fii_cash = pd.read_csv(FII_CSV) if FII_CSV.exists() else None
    if fii_cash is not None:
        fii_cash['date'] = pd.to_datetime(fii_cash['date']).dt.date
    intel = load_intel_timeseries()
    all_days = sorted(oi_cache.keys())
    if len(all_days) != 164:
        raise RuntimeError(f"Expected 164 days, got {len(all_days)}")

    if args.fold is not None:
        chosen = next(f for f in FOLDS if f['fold'] == args.fold)
    else:
        # Pick next fold whose result.json doesn't exist
        chosen = None
        for fd in FOLDS:
            if not (CV_DIR / f"fold_{fd['fold']:02d}" / 'result.json').exists():
                chosen = fd; break
        if chosen is None:
            print("All 4 folds done. Use --summary.")
            summary()
            return 0

    do_b = not args.intel_only
    do_i = not args.baseline_only
    run_fold(chosen, all_days, oi_cache, fut_df, fii_cash, intel,
             do_baseline=do_b, do_intel=do_i)

    summary()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
