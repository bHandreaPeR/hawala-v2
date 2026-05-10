#!/bin/bash
# reports/refresh_weekend.sh — runs every Saturday/Sunday after market close.
# Refreshes the Excel report and the multi-strategy trade explorer.
#
# cron entry (Saturday 06:00 IST):
#   0 6 * * 6  cd /path/to/hawala && bash reports/refresh_weekend.sh
#
# Two artefacts produced:
#   reports/weekly_backtest.xlsx
#   research/trade_explorer.html

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PY="${PYTHON_BIN:-/opt/anaconda3/bin/python}"
LOG_DIR="$REPO_ROOT/reports/logs"
mkdir -p "$LOG_DIR"
TS="$(date +%Y%m%d-%H%M)"
LOG="$LOG_DIR/refresh-$TS.log"

echo "[$TS] hawala weekend refresh — logging to $LOG"

{
  echo "── 1/3  refresh canonical 1-lot backtest ──────────────────────────"
  "$PY" run_canonical.py

  echo
  echo "── 2/3  build weekly Excel report  ────────────────────────────────"
  "$PY" -m reports.build_weekly_report --skip-rerun

  echo
  echo "── 3/3  rebuild multi-strategy trade explorer ─────────────────────"
  "$PY" -m research.trade_explorer

  echo
  echo "✓ weekend refresh complete  ($(date +%H:%M:%S))"
  echo "  reports/weekly_backtest.xlsx"
  echo "  research/trade_explorer.html"
} 2>&1 | tee "$LOG"
