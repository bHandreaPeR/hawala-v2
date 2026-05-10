#!/bin/bash
# =============================================================================
# v3/scripts/weekly_backfill.sh
# =============================================================================
# Weekly OI backfill for expired futures contracts.
#
# Groww returns OI only for expired contracts (7-col response).
# Active contracts return 6 cols (no OI). This script re-fetches all dates
# where OI is NaN and the contract has since expired — restoring OI retroactively.
#
# Runs AFTER weekly_report.py so fresh auth token is already valid.
#
# Cron (Sunday 02:30 UTC = 08:00 IST):
#   30 2 * * 0 cd "/path/to/Hawala v2/Hawala v2" && bash v3/scripts/weekly_backfill.sh >> v3/logs/weekly_backfill.log 2>&1
#
# Manual:
#   cd "Hawala v2/Hawala v2"
#   bash v3/scripts/weekly_backfill.sh
#   bash v3/scripts/weekly_backfill.sh --dry-run   # preview only
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/v3/logs"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

mkdir -p "$LOG_DIR"

# Pass --dry-run through if provided
DRY_RUN=""
for arg in "$@"; do
    if [[ "$arg" == "--dry-run" ]]; then
        DRY_RUN="--dry-run"
    fi
done

echo "============================================================"
echo "  weekly_backfill.sh  |  $TIMESTAMP"
echo "  project: $PROJECT_ROOT"
[[ -n "$DRY_RUN" ]] && echo "  MODE: DRY RUN"
echo "============================================================"

cd "$PROJECT_ROOT"

PYTHON=/opt/anaconda3/bin/python3

echo ""
echo "▶  Backfilling expired futures contracts (NIFTY + BANKNIFTY)"
if $PYTHON v3/data/backfill_expired_contracts.py --instrument ALL $DRY_RUN; then
    echo "   ✓  Backfill complete"
else
    echo "   ✗  Backfill FAILED (exit code $?)"
    # Non-fatal — weekly report already sent; this is supplementary
fi

echo ""
echo "▶  OI coverage check after backfill"
$PYTHON - <<'PYEOF'
import pickle, pathlib, datetime

ROOT = pathlib.Path('.').resolve()

def report(label, path_str):
    p = pathlib.Path(path_str)
    if not p.exists():
        print(f"  {label}: NOT FOUND")
        return
    with open(p, 'rb') as f:
        df = pickle.load(f)
    if df.empty or 'oi' not in df.columns:
        print(f"  {label}: empty or no OI column")
        return
    total_dates = df['date'].nunique()
    nan_dates   = (
        df.groupby('date')['oi']
        .apply(lambda s: s.isna().all())
        .sum()
    )
    pct_ok = (total_dates - nan_dates) / total_dates * 100 if total_dates else 0
    today  = datetime.date.today()
    # Identify which NaN dates are active-contract (expected) vs gaps (unexpected)
    from datetime import timedelta
    def last_tuesday(y, m):
        import calendar
        last_day = datetime.date(y if m < 12 else y+1, (m%12)+1, 1) - timedelta(days=1)
        return last_day - timedelta(days=(last_day.weekday()-1)%7)
    def contract_expiry(d):
        y, m = d.year, d.month
        overrides = {datetime.date(2026,3,31): datetime.date(2026,3,30)}
        for _ in range(3):
            exp = last_tuesday(y, m)
            exp = overrides.get(exp, exp)
            if exp >= d:
                return exp
            m = m+1 if m < 12 else 1
            y = y if m > 1 else y+1
        return None
    nan_list = (
        df.groupby('date')['oi']
        .apply(lambda s: s.isna().all())
        .pipe(lambda s: [d for d in s[s].index])
    )
    active_gap = sum(1 for d in nan_list if (contract_expiry(d) or today) >= today)
    real_gap   = len(nan_list) - active_gap
    status = "✓" if real_gap == 0 else "⚠"
    print(f"  {status}  {label}: {total_dates} days  OI coverage {pct_ok:.0f}%  "
          f"({real_gap} unexpected gaps  +  {active_gap} active-contract gaps)")

report("NIFTY     candles", "v3/cache/candles_1m_NIFTY.pkl")
report("BANKNIFTY candles", "v3/cache/candles_1m_BANKNIFTY.pkl")
PYEOF

echo ""
echo "============================================================"
echo "  weekly_backfill.sh done  |  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
