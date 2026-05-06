#!/bin/bash
# =============================================================================
# v3/scripts/daily_fetch.sh
# =============================================================================
# Daily end-of-day data pipeline for Hawala v3.
# Run this after market close (~16:00 IST) every trading day.
#
# What it does (in order):
#   1. Fetch NIFTY futures 1m candles          (always available post-close)
#   2. Fetch NIFTY option OI 1m candles         (must run within same session day)
#   3. Fetch BankNifty futures 1m candles
#   4. Fetch BankNifty option OI 1m candles
#   5. Fetch NSE bhavcopy → update NIFTY PCR cache     (available ~15:30 IST)
#   6. Fetch NSE bhavcopy → update BANKNIFTY PCR cache (same file, different filter)
#
# Usage:
#   cd "Hawala v2/Hawala v2"
#   bash v3/scripts/daily_fetch.sh
#
#   Or via cron (runs Mon-Fri at 16:05 IST = 10:35 UTC):
#   35 10 * * 1-5 cd "/path/to/Hawala v2/Hawala v2" && bash v3/scripts/daily_fetch.sh >> v3/logs/daily_fetch.log 2>&1
#
# To install the cron job:
#   crontab -e
#   Add the line above (edit path first).
#
# Requirements: token.env must be valid (refresh TOTP before running if needed).
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/v3/logs"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

mkdir -p "$LOG_DIR"

echo "============================================================"
echo "  daily_fetch.sh  |  $TIMESTAMP"
echo "  project: $PROJECT_ROOT"
echo "============================================================"

cd "$PROJECT_ROOT"

# ── Helper ────────────────────────────────────────────────────────────────────
run_step() {
    local name="$1"
    local script="$2"
    echo ""
    echo "▶  $name"
    echo "   script: $script"
    if python3 "$script"; then
        echo "   ✓  $name — OK"
    else
        echo "   ✗  $name — FAILED (exit code $?)"
        # Do NOT abort on individual step failures — continue with remaining steps.
        # Each step is independent; a BankNifty fetch failure shouldn't stop PCR fetch.
    fi
}

# ── Step 1: NIFTY futures candles ─────────────────────────────────────────────
run_step "NIFTY futures 1m candles" "v3/data/fetch_1m_NIFTY.py"

# ── Step 2: NIFTY option OI ──────────────────────────────────────────────────
# IMPORTANT: Groww stores intraday option OI in historical candles,
# so this CAN be run end-of-day to capture the full session's data.
# However, run it the SAME CALENDAR DAY as the session (before midnight).
run_step "NIFTY option OI 1m" "v3/data/fetch_option_oi_NIFTY.py"

# ── Step 3: BankNifty futures candles ────────────────────────────────────────
run_step "BankNifty futures 1m candles" "v3/data/fetch_1m_BANKNIFTY.py"

# ── Step 4: BankNifty option OI ──────────────────────────────────────────────
run_step "BankNifty option OI 1m" "v3/data/fetch_option_oi_BANKNIFTY.py"

# ── Step 5: Bhavcopy + PCR (NIFTY) ──────────────────────────────────────────
# NSE publishes bhavcopy ~15:30 IST. This step fetches and updates NIFTY PCR cache.
run_step "NSE bhavcopy (NIFTY PCR)" "v3/data/fetch_bhavcopy_nifty.py"

# ── Step 6: Bhavcopy + PCR (BANKNIFTY) ──────────────────────────────────────
# Same bhavcopy file, filtered for BANKNIFTY. Updates bhavcopy_BN_all.pkl.
# runner_banknifty.py PCR reads exclusively from this cache.
run_step "NSE bhavcopy (BANKNIFTY PCR)" "v3/data/fetch_bhavcopy_banknifty.py"

# ── Step 7: FII Cash ──────────────────────────────────────────────────────────
# NSE publishes FII cash net buy/sell data EOD. Updates fii_data.csv.
# Used by signal_fii_signature (lag-1 FII cash direction).
run_step "FII Cash (fii_data.csv)" "v3/data/fetch_fii_cash.py"

# ── Step 8: FII F&O ───────────────────────────────────────────────────────────
# NSE F&O participant-wise OI data. Updates trade_logs/_fii_fo_cache.pkl.
# Used by signal_fii_signature (fii_fut_level: long/short/neutral).
run_step "FII F&O (_fii_fo_cache.pkl)" "v3/data/fetch_fii_fo.py"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  daily_fetch.sh COMPLETE  |  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

# Verify cache freshness
echo "Cache freshness check:"
python3 - <<'PYEOF'
import pickle, pandas as pd
from pathlib import Path
from datetime import date

ROOT = Path('.').resolve()

def check(label, path_str):
    p = Path(path_str)
    if not p.exists():
        print(f"  {label}: NOT FOUND at {path_str}")
        return
    try:
        if path_str.endswith('.pkl'):
            with open(p, 'rb') as f:
                data = pickle.load(f)
            if isinstance(data, pd.DataFrame):
                last = str(data['date'].max()) if 'date' in data.columns else 'unknown'
            elif isinstance(data, dict):
                last = sorted(data.keys())[-1] if data else 'empty'
            else:
                last = 'loaded'
        elif path_str.endswith('.csv'):
            df = pd.read_csv(p)
            last = df['date'].max() if 'date' in df.columns else 'unknown'
        else:
            last = 'unknown format'
        today = str(date.today())
        status = '✓ UP TO DATE' if str(last)[:10] == today else f'⚠  last={last}'
        print(f"  {label}: {status}")
    except Exception as e:
        print(f"  {label}: ERROR reading — {e}")

check("NIFTY candles  ", "v3/cache/candles_1m_NIFTY.pkl")
check("NIFTY option OI", "v3/cache/option_oi_1m_NIFTY.pkl")
check("BN candles     ", "v3/cache/candles_1m_BANKNIFTY.pkl")
check("BN option OI   ", "v3/cache/option_oi_1m_BANKNIFTY.pkl")
check("PCR daily      ", "v3/cache/pcr_daily.csv")
check("BN bhavcopy    ", "v3/cache/bhavcopy_BN_all.pkl")
PYEOF
