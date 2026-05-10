#!/bin/bash
# kill_runners.sh — kills ALL runner processes and waits until confirmed dead
cd "$(dirname "$0")"

echo "Killing all runner processes..."
pkill -9 -f "runner_nifty.py"     2>/dev/null
pkill -9 -f "runner_banknifty.py" 2>/dev/null
pkill -9 -f "caffeinate.*runner"  2>/dev/null
pkill -9 -f "news\.runner"         2>/dev/null

# Wait until confirmed dead
for i in {1..10}; do
    sleep 1
    N=$(pgrep -f "runner_nifty\|runner_banknifty\|news\.runner" 2>/dev/null | wc -l | tr -d ' ')
    echo "  [$i] processes still alive: $N"
    if [ "$N" -eq "0" ]; then
        echo "All dead. Safe to restart."
        exit 0
    fi
    # Force kill anything still alive
    pgrep -f "runner_nifty\|runner_banknifty\|news\.runner" 2>/dev/null | xargs kill -9 2>/dev/null
done
echo "WARNING: Could not confirm all processes dead. Check manually with: ps aux | grep runner"
