# logs/

Central log hub for every running process. Each subfolder corresponds to
one Telegram-channel-or-bot lifecycle.

| Subfolder | Producer | Log file(s) |
|---|---|---|
| `trade_bot/` | `v3/live/runner_nifty.py` | `runner_nifty.log` |
| `trade_bot/` | `v3/live/runner_banknifty.py` | `runner_banknifty.log` |
| `trade_bot/` | (legacy) `alert_runner.py` historical snapshot | `alert_runner_*.log` |
| `news_bot/` | `news/runner.py` | `news_runner.log` |
| `macro_bot/` | `run_daily_report.py` (when redirected) | `daily_report-<date>.log` (recommended) |
| `macro_bot/` | `bots/macro_bot.py` (if scheduled) | `macro_bot-<date>.log` |
| `reports/` | `v3/scripts/weekly_backfill.sh` | `weekly_backfill.log` |
| `reports/` | `reports/refresh_weekend.sh` | `refresh-<timestamp>.log` |

## Source code paths writing here (already wired)

```
news/runner.py              → logs/news_bot/news_runner.log
v3/live/runner_nifty.py     → logs/trade_bot/runner_nifty.log
v3/live/runner_banknifty.py → logs/trade_bot/runner_banknifty.log
```

## Cron entries that should redirect here (recommended update)

```diff
- 32 7 * * 1-5  ... python3 run_daily_report.py >> /tmp/hawala_report.log 2>&1
+ 32 7 * * 1-5  ... python3 run_daily_report.py >> logs/macro_bot/daily_report-$(date +\%Y\%m\%d).log 2>&1

- 30 16 * * 1-5 ... bash v3/scripts/daily_fetch.sh >> /tmp/hawala_daily_fetch.log 2>&1
+ 30 16 * * 1-5 ... bash v3/scripts/daily_fetch.sh >> logs/reports/daily_fetch.log 2>&1

- 0 18 * * 5    ... python3 run_weekly_report.py >> /tmp/hawala_weekly.log 2>&1
+ 0 18 * * 5    ... python3 run_weekly_report.py >> logs/reports/weekly_report-$(date +\%Y\%m\%d).log 2>&1
```

Pure cron-side changes — no code changes required for these. The Python
scripts already log to stdout; the redirect just sends them to a stable path
that survives reboots (vs `/tmp/`).
