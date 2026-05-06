#!/bin/bash
# =============================================================================
# v3/scripts/morning_fetch.sh
# =============================================================================
# Pre-market data pipeline for Hawala v3.
# Run this BEFORE market opens each trading day (~08:30 IST).
#
# What it does (in order):
#   1. Fetch FII F&O participant OI  → trade_logs/_fii_fo_cache.pkl
#   2. Fetch FII / FPI cash data     → fii_data.csv
#   3. Validate all caches and print freshness report
#
# Why morning (not EOD)?
#   NSE publishes FII F&O and FII cash data for the PREVIOUS day overnight.
#   Both are lag-1 inputs: used today to predict today's direction.
#   They must be refreshed BEFORE market opens — not after close.
#
# EOD data (candles, option OI, bhavcopy) is handled by daily_fetch.sh
# which runs AFTER market close (~16:05 IST).
#
# Usage:
#   cd "Hawala v2/Hawala v2"
#   bash v3/scripts/morning_fetch.sh
#
#   Via cron (runs Mon-Fri at 08:30 IST = 03:00 UTC):
#   0 3 * * 1-5 cd "/path/to/Hawala v2/Hawala v2" && bash v3/scripts/morning_fetch.sh >> v3/logs/morning_fetch.log 2>&1
#
# To install cron jobs (run both morning + EOD):
#   crontab -e
#   Add these two lines (edit paths first):
#     0  3 * * 1-5  cd "/path/to/Hawala v2/Hawala v2" && bash v3/scripts/morning_fetch.sh >> v3/logs/morning_fetch.log 2>&1
#     35 10 * * 1-5  cd "/path/to/Hawala v2/Hawala v2" && bash v3/scripts/daily_fetch.sh >> v3/logs/daily_fetch.log 2>&1
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/v3/logs"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

mkdir -p "$LOG_DIR"

echo "============================================================"
echo "  morning_fetch.sh  |  $TIMESTAMP"
echo "  project: $PROJECT_ROOT"
echo "============================================================"

cd "$PROJECT_ROOT"

# ── Helper ────────────────────────────────────────────────────────────────────
run_step() {
    local name="$1"
    local script="$2"
    local extra_args="${3:-}"
    echo ""
    echo "▶  $name"
    echo "   script: $script"
    if python3 "$script" $extra_args; then
        echo "   ✓  $name — OK"
    else
        echo "   ✗  $name — FAILED (exit code $?)"
        # Do NOT abort — each step is independent.
    fi
}

# ── Step 1: FII F&O participant OI ────────────────────────────────────────────
# NSE publishes F&O participant OI (FII/DII/Client) by ~22:00 IST the same evening.
# We fetch the prior 7 calendar days to fill any gaps (handles weekends + holidays).
run_step "FII F&O participant OI" "v3/data/fetch_fii_fo.py" "--days 7"

# ── Step 2: FII/FPI cash market activity ─────────────────────────────────────
# NSE publishes FPI cash net activity by ~21:00 IST the same evening.
run_step "FII/FPI cash activity" "v3/data/fetch_fii_cash.py" "--days 7"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================"
echo "  morning_fetch.sh COMPLETE  |  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

# ── Full cache freshness report ──────────────────────────────────────────────
echo "Cache freshness check (pre-market):"
python3 - <<'PYEOF'
import pickle, pandas as pd
from pathlib import Path
from datetime import date, timedelta

ROOT  = Path('.').resolve()
today = date.today()
prev  = str(today - timedelta(days=1))
# Prior weekday (handles weekend)
d = today - timedelta(days=1)
while d.weekday() >= 5:
    d -= timedelta(days=1)
last_trading_day = str(d)

def status(last_str, label):
    if not last_str or last_str == 'empty':
        return f'❌  {label}: EMPTY'
    if str(last_str)[:10] >= last_trading_day:
        return f'✓  {label}: {last_str}'
    return f'⚠  {label}: STALE (last={last_str}, expected≥{last_trading_day})'

def check_pkl(label, path_str, key='date'):
    p = ROOT / path_str
    if not p.exists():
        print(f'  ❌  {label}: NOT FOUND at {path_str}')
        return
    try:
        with open(p, 'rb') as f:
            data = pickle.load(f)
        if isinstance(data, pd.DataFrame):
            last = str(data[key].max()) if key in data.columns else str(data.index.max())
        elif isinstance(data, dict):
            last = sorted(data.keys())[-1] if data else 'empty'
        else:
            last = 'loaded'
        print(f'  {status(last, label)}')
    except Exception as e:
        print(f'  ❌  {label}: ERROR — {e}')

def check_csv(label, path_str, key='date'):
    p = ROOT / path_str
    if not p.exists():
        print(f'  ❌  {label}: NOT FOUND at {path_str}')
        return
    try:
        df = pd.read_csv(p)
        last = str(df[key].max()) if key in df.columns else 'unknown'
        print(f'  {status(last, label)}')
    except Exception as e:
        print(f'  ❌  {label}: ERROR — {e}')

# Pre-market inputs (lag-1, must be ≥ last trading day)
check_pkl('FII F&O cache ', 'trade_logs/_fii_fo_cache.pkl')
check_csv('FII cash CSV  ', 'fii_data.csv')
check_csv('PCR daily     ', 'v3/cache/pcr_daily.csv')

# Candle / OI caches (updated EOD by daily_fetch.sh — may lag 1 day pre-open, that's OK)
check_pkl('NIFTY candles ', 'v3/cache/candles_1m_NIFTY.pkl')
check_pkl('NIFTY option OI','v3/cache/option_oi_1m_NIFTY.pkl')
check_pkl('BN candles    ', 'v3/cache/candles_1m_BANKNIFTY.pkl')
check_pkl('BN option OI  ', 'v3/cache/option_oi_1m_BANKNIFTY.pkl')
PYEOF
