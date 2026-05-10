# adhoc/

One-off / research scripts. **None of these are scheduled in cron.** Run
them manually when investigating something.

## Backtest runners

| Script | Purpose |
|---|---|
| `run_canonical.py` | 1-lot reproducible backtest of the active VP-Trail stack with realistic slippage. **The authoritative source for canonical params.** |
| `run_baseline.py` | Compounded full-stack backtest (ORB + OPT_ORB + VP-Trail). |
| `run_full_backtest.py` | Pre-canonical full pipeline. |
| `run_2026_oos.py` | OOS slice for 2026. |
| `run_next_steps.py` | Iteration runner used during forensics. |
| `run_candlestick_backtest.py` | Candlestick strategy backtest (rejected). |
| `run_expiry_spread_backtest.py` | Expiry-spread experiment (rejected). |
| `run_sensex_sweep.py` | SENSEX param sweep. |

## Tools / utilities

| Script | Purpose |
|---|---|
| `analyse_signals.py` | Cross-check `signal_schema.json` against trade logs. |
| `iterate.py` | Quick iteration helper. |
| `set_tokens.py` | Interactive `token.env` builder. |
| `test_groww_expired.py` | One-off Groww expired-token reproduction. |
| `signal_schema.json` | Schema for v2-era trade-log columns. |

## Conventions

- A script that gets cron-scheduled MUST move out of this folder back to
  `top-level/` or to a dedicated module — and CONTEXT.md must be updated.
- A script in here can `import` from `strategies/`, `backtest/`, `data/`,
  but should NOT be imported by anything in `v3/`, `news/`, or top-level
  cron paths.
