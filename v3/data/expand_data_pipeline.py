"""
v3/data/expand_data_pipeline.py
================================
Orchestrates full backtest data expansion after probe_groww_history.py
identifies how far back Groww 1m data is available.

Steps:
  1. Fetch BankNifty futures 1m candles from --from date to today
  2. Fetch BankNifty option OI 1m from --from date (incremental, skips cached days)
  3. Fetch Nifty futures 1m candles from --from date to today
  4. Fetch Nifty option OI 1m from --from date
  5. Validate bhavcopy + FII cash coverage — warn if gaps found
  6. Re-run classifier recalibration on full dataset
  7. Re-run backtest (BankNifty)

Usage:
    python v3/data/expand_data_pipeline.py --from 2025-09-01
    python v3/data/expand_data_pipeline.py --from 2025-09-01 --skip-bn-futures
    python v3/data/expand_data_pipeline.py --from 2025-09-01 --skip-bn-futures --skip-nifty-futures

Flags:
    --from YYYY-MM-DD       start date for data fetch (required)
    --skip-bn-futures       skip BN futures fetch (already done)
    --skip-bn-options       skip BN options OI fetch
    --skip-nifty-futures    skip Nifty futures fetch
    --skip-nifty-options    skip Nifty options OI fetch
    --skip-backtest         stop after fetch + validation
    --dry-run               show what would be done, no API calls
"""
import sys, pickle, subprocess, logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('expand_pipeline')


def _parse_args():
    args = sys.argv[1:]
    start_date       = None
    skip_bn_fut      = '--skip-bn-futures'    in args or '--skip-futures' in args
    skip_bn_opt      = '--skip-bn-options'    in args or '--skip-options' in args
    skip_nifty_fut   = '--skip-nifty-futures' in args
    skip_nifty_opt   = '--skip-nifty-options' in args
    skip_bt          = '--skip-backtest'      in args
    dry_run          = '--dry-run'            in args

    for a in args:
        if a.startswith('--from'):
            val = a.split('=')[1] if '=' in a else args[args.index(a) + 1]
            start_date = datetime.strptime(val, '%Y-%m-%d').date()

    if start_date is None:
        print("ERROR: --from YYYY-MM-DD is required")
        print("  Example: python v3/data/expand_data_pipeline.py --from 2025-09-01")
        sys.exit(1)

    return start_date, skip_bn_fut, skip_bn_opt, skip_nifty_fut, skip_nifty_opt, skip_bt, dry_run


# ── Step helpers ──────────────────────────────────────────────────────────────

def _run(cmd: list[str], dry_run: bool):
    log.info("RUN: %s", ' '.join(cmd))
    if dry_run:
        print(f"  [DRY-RUN] would run: {' '.join(cmd)}")
        return 0
    result = subprocess.run(cmd, cwd=str(ROOT))
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {' '.join(cmd)}"
        )
    return result.returncode


def _validate_bhavcopy(start_date: date) -> tuple[bool, list[str]]:
    """Check bhavcopy coverage from start_date onward."""
    cache_file = ROOT / 'v3/cache/bhavcopy_BN_all.pkl'
    if not cache_file.exists():
        return False, ["bhavcopy_BN_all.pkl not found"]

    with open(cache_file, 'rb') as f:
        bhav = pickle.load(f)

    bhav_dates = sorted(datetime.strptime(d, '%Y-%m-%d').date()
                        for d in bhav.keys() if isinstance(d, str))
    if not bhav_dates:
        return False, ["bhavcopy is empty"]

    coverage_start = bhav_dates[0]
    coverage_end   = bhav_dates[-1]

    issues = []
    if coverage_start > start_date:
        issues.append(
            f"Bhavcopy starts {coverage_start} but we need from {start_date}. "
            f"Run: python v3/data/fetch_bhavcopy_nifty.py --from {start_date}"
        )
    if coverage_end < date(2026, 4, 30):
        issues.append(f"Bhavcopy ends {coverage_end} — expected 2026-04-30")

    log.info("Bhavcopy coverage: %s → %s (%d dates)", coverage_start, coverage_end, len(bhav_dates))
    return len(issues) == 0, issues


def _validate_fii_cash(start_date: date) -> tuple[bool, list[str]]:
    """Check FII cash data coverage."""
    fii_file = ROOT / 'fii_data.csv'
    if not fii_file.exists():
        return False, ["fii_data.csv not found"]

    df = pd.read_csv(fii_file)
    df['date'] = pd.to_datetime(df['date']).dt.date
    fii_start = df['date'].min()
    fii_end   = df['date'].max()

    issues = []
    if fii_start > start_date:
        issues.append(
            f"FII cash data starts {fii_start} but need from {start_date}. "
            "Manually extend fii_data.csv or fetch from NSE website."
        )
    if fii_end < date(2026, 4, 30):
        issues.append(f"FII cash data ends {fii_end} — expected 2026-04-30")

    log.info("FII cash coverage: %s → %s (%d rows)", fii_start, fii_end, len(df))
    return len(issues) == 0, issues


def _validate_futures_cache(start_date: date) -> tuple[int, date | None, date | None]:
    """Return (n_days, first_date, last_date) from cached futures candles."""
    cache_file = ROOT / 'v3/cache/candles_1m_BANKNIFTY.pkl'
    if not cache_file.exists():
        return 0, None, None
    with open(cache_file, 'rb') as f:
        df = pickle.load(f)
    if df.empty or 'date' not in df.columns:
        return 0, None, None
    dates = sorted(df['date'].unique())
    return len(dates), dates[0], dates[-1]


def _validate_options_cache(start_date: date) -> tuple[int, str | None, str | None]:
    """Return (n_days, first_date, last_date) from options OI cache."""
    cache_file = ROOT / 'v3/cache/option_oi_1m_BANKNIFTY.pkl'
    if not cache_file.exists():
        return 0, None, None
    with open(cache_file, 'rb') as f:
        cache = pickle.load(f)
    if not cache:
        return 0, None, None
    keys = sorted(cache.keys())
    return len(keys), keys[0], keys[-1]


# ── Main ──────────────────────────────────────────────────────────────────────

def _validate_nifty_futures_cache(start_date: date) -> tuple[int, date | None, date | None]:
    cache_file = ROOT / 'v3/cache/candles_1m_NIFTY.pkl'
    if not cache_file.exists():
        return 0, None, None
    with open(cache_file, 'rb') as f:
        df = pickle.load(f)
    if df.empty or 'date' not in df.columns:
        return 0, None, None
    dates = sorted(df['date'].unique())
    return len(dates), dates[0], dates[-1]


def _validate_nifty_options_cache(start_date: date) -> tuple[int, str | None, str | None]:
    cache_file = ROOT / 'v3/cache/option_oi_1m_NIFTY.pkl'
    if not cache_file.exists():
        return 0, None, None
    with open(cache_file, 'rb') as f:
        cache = pickle.load(f)
    if not cache:
        return 0, None, None
    keys = sorted(cache.keys())
    return len(keys), keys[0], keys[-1]


def main():
    start_date, skip_bn_fut, skip_bn_opt, skip_nifty_fut, skip_nifty_opt, skip_bt, dry_run \
        = _parse_args()

    print(f"\n{'='*70}")
    print(f"Data Expansion Pipeline — BankNifty + Nifty")
    print(f"  Start date       : {start_date}")
    print(f"  Skip BN futures  : {skip_bn_fut}   Skip BN options : {skip_bn_opt}")
    print(f"  Skip Nifty fut   : {skip_nifty_fut}  Skip Nifty opts: {skip_nifty_opt}")
    print(f"  Skip backtest    : {skip_bt}   Dry run: {dry_run}")
    print(f"{'='*70}\n")

    # ── Step 0: Current cache state ───────────────────────────────────────────
    n_bn_fut,  bn_fut_first,  bn_fut_last  = _validate_futures_cache(start_date)
    n_bn_opt,  bn_opt_first,  bn_opt_last  = _validate_options_cache(start_date)
    n_nf_fut,  nf_fut_first,  nf_fut_last  = _validate_nifty_futures_cache(start_date)
    n_nf_opt,  nf_opt_first,  nf_opt_last  = _validate_nifty_options_cache(start_date)
    log.info("BN Futures cache  : %d days (%s → %s)", n_bn_fut, bn_fut_first, bn_fut_last)
    log.info("BN Options cache  : %d days (%s → %s)", n_bn_opt, bn_opt_first, bn_opt_last)
    log.info("Nifty Futures cache: %d days (%s → %s)", n_nf_fut, nf_fut_first, nf_fut_last)
    log.info("Nifty Options cache: %d days (%s → %s)", n_nf_opt, nf_opt_first, nf_opt_last)

    # ── Step 1: BankNifty futures 1m ─────────────────────────────────────────
    if not skip_bn_fut:
        print(f"\n[Step 1/4] Fetching BankNifty futures 1m from {start_date} ...")
        _run([sys.executable,
              str(ROOT / 'v3/data/fetch_1m_BANKNIFTY.py'),
              f'--from={start_date}'], dry_run)
        n_bn_fut, bn_fut_first, bn_fut_last = _validate_futures_cache(start_date)
        print(f"  BN Futures: {n_bn_fut} days ({bn_fut_first} → {bn_fut_last})")
    else:
        print(f"[Step 1/4] BN Futures skipped — {n_bn_fut} days ({bn_fut_first} → {bn_fut_last})")

    # ── Step 2: BankNifty options OI 1m ──────────────────────────────────────
    if not skip_bn_opt:
        print(f"\n[Step 2/4] Fetching BankNifty options OI 1m from {start_date} ...")
        print(f"  Incremental — safe to interrupt and restart.")
        est_days = max(0, n_bn_fut - n_bn_opt)
        print(f"  ~{est_days} new days to fetch (~{est_days * 40 * 2 * 0.4 / 60:.0f} min)")
        _run([sys.executable,
              str(ROOT / 'v3/data/fetch_option_oi_BANKNIFTY.py'),
              f'--from={start_date}'], dry_run)
        n_bn_opt, bn_opt_first, bn_opt_last = _validate_options_cache(start_date)
        print(f"  BN Options: {n_bn_opt} days ({bn_opt_first} → {bn_opt_last})")
    else:
        print(f"[Step 2/4] BN Options skipped — {n_bn_opt} days ({bn_opt_first} → {bn_opt_last})")

    # ── Step 3: Nifty futures 1m ──────────────────────────────────────────────
    if not skip_nifty_fut:
        print(f"\n[Step 3/4] Fetching Nifty futures 1m from {start_date} ...")
        _run([sys.executable,
              str(ROOT / 'v3/data/fetch_1m_NIFTY.py'),
              f'--from={start_date}'], dry_run)
        n_nf_fut, nf_fut_first, nf_fut_last = _validate_nifty_futures_cache(start_date)
        print(f"  Nifty Futures: {n_nf_fut} days ({nf_fut_first} → {nf_fut_last})")
    else:
        print(f"[Step 3/4] Nifty Futures skipped — {n_nf_fut} days ({nf_fut_first} → {nf_fut_last})")

    # ── Step 4: Nifty options OI 1m ───────────────────────────────────────────
    if not skip_nifty_opt:
        print(f"\n[Step 4/4] Fetching Nifty options OI 1m from {start_date} ...")
        print(f"  Incremental — safe to interrupt and restart.")
        _run([sys.executable,
              str(ROOT / 'v3/data/fetch_option_oi_NIFTY.py'),
              f'--from={start_date}'], dry_run)
        n_nf_opt, nf_opt_first, nf_opt_last = _validate_nifty_options_cache(start_date)
        print(f"  Nifty Options: {n_nf_opt} days ({nf_opt_first} → {nf_opt_last})")
    else:
        print(f"[Step 4/4] Nifty Options skipped — {n_nf_opt} days ({nf_opt_first} → {nf_opt_last})")

    # ── Step 5: Validate bhavcopy + FII cash ──────────────────────────────────
    print(f"\n[Validate] Checking bhavcopy + FII cash coverage ...")
    bhav_ok, bhav_issues = _validate_bhavcopy(start_date)
    fii_ok,  fii_issues  = _validate_fii_cash(start_date)

    for issue in bhav_issues:
        print(f"  WARNING: {issue}")
    if not bhav_issues:
        print(f"  Bhavcopy: OK")

    for issue in fii_issues:
        print(f"  WARNING: {issue}")
    if not fii_issues:
        print(f"  FII cash: OK")

    if not bhav_ok:
        print(f"\n  STOPPING: Bhavcopy gaps will cause PCR/walls to fail.")
        sys.exit(1)

    if not fii_ok:
        print(f"\n  NOTE: FII gaps → classifier uses fii_cash_context=0 fallback. Continuing.")

    # ── Step 6: Recalibrate classifiers ───────────────────────────────────────
    if not skip_bt:
        print(f"\n[Calibrate] Recalibrating all classifiers on full dataset ...")
        clf_scripts = [
            ROOT / 'v3/signals/fii_dii_classifier.py',
            ROOT / 'v3/signals/fii_dii_classifier_BANKNIFTY.py',
            ROOT / 'v3/signals/fii_dii_classifier_COMBINED.py',
        ]
        for script in clf_scripts:
            if script.exists():
                print(f"  {script.name} ...")
                _run([sys.executable, str(script), '--calibrate'], dry_run)
            else:
                print(f"  WARNING: {script.name} not found — skipping")

        # ── Step 7: Re-run backtest ────────────────────────────────────────────
        print(f"\n[Backtest] Re-running BankNifty backtest ...")
        _run([sys.executable, str(ROOT / 'v3/backtest/run_backtest_banknifty.py')], dry_run)

    print(f"\n{'='*70}")
    print("Pipeline complete.")
    print(f"{'='*70}\n")


if __name__ == '__main__':
    main()
