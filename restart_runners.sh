#!/bin/bash
# restart_runners.sh
cd "$(dirname "$0")"

# Step 1: Kill everything
bash kill_runners.sh
EXIT=$?
if [ $EXIT -ne 0 ]; then
    echo "Kill did not confirm clean. Aborting — check 'ps aux | grep runner' manually."
    exit 1
fi

# Step 2: Start fresh (one of each)
echo ""
echo "Starting NIFTY runner (paper mode)..."
# FileHandler in runner already writes to runner.log — don't redirect stdout/stderr
# or every log line appears twice (FileHandler + StreamHandler both hit the file).
caffeinate -i python3 v3/live/runner_nifty.py > /dev/null 2>&1 &
echo "  PID $!"

echo "Starting BANKNIFTY runner (paper mode)..."
caffeinate -i python3 v3/live/runner_banknifty.py > /dev/null 2>&1 &
echo "  PID $!"

echo ""
echo "Runners started. Tailing logs (Ctrl+C to detach — runners keep going)"
sleep 2
tail -f v3/live/runner.log v3/live/runner_banknifty.log
