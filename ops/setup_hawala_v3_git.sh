#!/bin/bash
# setup_hawala_v3_git.sh
# Run this once to commit all v3 code and set up the hawala-v3 remote.
# Usage: cd "Hawala v2" && bash setup_hawala_v3_git.sh

set -e
cd "$(dirname "$0")"

# Remove stale lock if present
[ -f .git/index.lock ] && rm -f .git/index.lock && echo "Removed stale index.lock"

# Stage v3 files + supporting scripts + architecture
git add v3/ ARCHITECTURE.md kill_runners.sh restart_runners.sh .gitignore

# Stage any modified files in the core codebase
git add alerts/ v3/ 2>/dev/null || true

git status --short

echo ""
echo "Committing Hawala v3 initial snapshot..."
git commit -m "Hawala v3: initial v3 runner + backtest snapshot (May 2026)

Live runners (paper mode, production-ready):
- runner_nifty.py   — NIFTY options, LOT=65
- runner_banknifty.py — BANKNIFTY options, LOT=30

Fixes applied this session:
- NIFTY_LOT 75→65 in runner (now matches backtest)
- BankNifty F3 filter: single clf_bn → majority vote (2/3 classifiers)
- BankNifty EOD exit 15:15→15:20 (matches backtest)
- OI Mode-B: _bar_total_oi running history dict (active contract OI fix)
- Log double-write fix: restart script uses /dev/null redirect
- Remove stale 'F0 gate' comment (no such gate exists)

Backtest results (Jan-Apr 2026):
- NIFTY:     22 trades, 68.2% win rate, INR 26,722 total P&L
- BankNifty:  8 trades, 75.0% win rate, INR 27,874 total P&L
- Combined:  30 trades, INR 54,596 total P&L

Signal engine: 6 signals (OI quadrant, basis, PCR, velocity, strike defense, FII/DII)
Architecture documented in ARCHITECTURE.md"

echo ""
echo "✓ Committed."
echo ""

# Add hawala-v3 remote (create the repo on GitHub first)
echo "Next: create https://github.com/bHandreaPeR/hawala-v3 on GitHub, then run:"
echo ""
echo "  git remote add hawala-v3 https://github.com/bHandreaPeR/hawala-v3.git"
echo "  git push hawala-v3 main"
echo ""
echo "Or to push the full history:"
echo "  git push hawala-v3 HEAD:main"
