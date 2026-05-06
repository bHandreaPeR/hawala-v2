# Hawala v3 — System Architecture
_Last updated: May 2026_

---

## Overview

Hawala v3 is a live intraday options trading system for NIFTY and BANKNIFTY. It runs paper trades by default. Real orders require explicit `--live` flag (not yet implemented).

---

## Repository Structure

```
Hawala v2/                          ← workspace root
├── v3/
│   ├── live/
│   │   ├── runner_nifty.py         ← NIFTY live runner (primary)
│   │   ├── runner_banknifty.py     ← BANKNIFTY live runner
│   │   ├── runner.log              ← NIFTY runner output (appended each session)
│   │   └── runner_banknifty.log    ← BN runner output
│   ├── backtest/
│   │   ├── run_backtest_nifty.py   ← NIFTY backtest (Jan–Apr 2026)
│   │   ├── run_backtest_banknifty.py
│   │   └── *.csv                   ← trade log outputs
│   ├── signals/
│   │   ├── engine.py               ← compute_signal_state(), SignalSmoother
│   │   └── fii_dii_classifier.py   ← FIIDIIClassifier (MIN_WINDOW=5)
│   └── cache/
│       ├── candles_1m_NIFTY.pkl    ← 1m futures candle cache (daily backfill)
│       ├── candles_1m_BANKNIFTY.pkl
│       ├── option_oi_1m_NIFTY.pkl  ← option chain OI history
│       ├── fii_dii_thresholds.json              ← Nifty classifier thresholds
│       ├── fii_dii_thresholds_BANKNIFTY.json    ← BN-specific thresholds
│       └── fii_dii_thresholds_COMBINED.json     ← combined market thresholds
├── alerts/
│   └── telegram.py                 ← send(token, chat_id, msg)
├── restart_runners.sh              ← START HERE — kills old, starts both runners
├── kill_runners.sh                 ← kill both runners, confirm dead
├── alert_runner.py                 ← OLD Hawala v2 master (ORB/VWAP/IC for BN)
│                                      ⚠ NEVER run alongside restart_runners.sh
└── token.env                       ← API keys (gitignored)
```

---

## How to Run

```bash
# Standard: start/restart both runners
cd /path/to/Hawala\ v2/Hawala\ v2
bash restart_runners.sh

# Kill without restart
bash kill_runners.sh

# Tail logs only (runners already running)
tail -f v3/live/runner.log v3/live/runner_banknifty.log
```

**⚠ Never run `alert_runner.py` on the same day as `restart_runners.sh`.** `alert_runner.py` spawns the v3 runners as subprocesses — running both simultaneously creates duplicate runner instances and double Telegram alerts.

---

## Signal Engine

Each runner polls every minute (9:15–15:20/15:15):

| Step | Source | Signal |
|------|--------|--------|
| 1 | Futures 1m candles (+ OI Mode B injection) | `oi_quadrant`: long_buildup / short_buildup / long_unwind / short_cover |
| 2 | Futures LTP vs spot | `futures_basis`: contango / backwardation |
| 3 | Option chain CE+PE OI | `pcr`: bullish / bearish / neutral |
| 4 | Rolling OI deque (60 bars) | `oi_velocity`: net OI change direction |
| 5 | Option chain strike OI | `strike_defense`: wall proximity |
| 6 | FIIDIIClassifier | `fii_signature`: FII_BULL / FII_BEAR / DII_MIXED |

All 6 signals feed into `compute_signal_state()` → `SignalSmoother` → `effective_dir` (+1 LONG / -1 SHORT / 0 no-trade).

### OI Mode B (live-runner fix, Apr 2026)

Futures candle OI is NaN for active contracts (Groww returns 6-col response, no OI column). OI quadrant uses total option market OI as proxy:

```python
_total_option_oi = sum(chain['ce_oi'].values()) + sum(chain['pe_oi'].values())
_bar_total_oi[df_fut['ts'].iloc[-1]] = _total_option_oi   # running history dict
df_fut['oi'] = df_fut['ts'].map(_bar_total_oi)            # inject full history
```

This was validated against 22 backtest trades: Mode A (futures OI) vs Mode B (option OI) produces identical signal direction on 22/22 trades.

---

## Entry Logic

Entry fires when **all** of these hold simultaneously:
1. `bar_idx >= ENTRY_BAR` (105 min after 9:15 = 11:00 AM)
2. `current_hhmm <= LAST_ENTRY_HHMM` (13:00)
3. `effective_dir != 0` (signal smoothed and non-zero)
4. `abs(state.score) >= SIGNAL_SCORE_MIN` (0.35)
5. `state.signal_count >= MIN_SIGNAL_COUNT` (5 of 6 signals must agree)
6. Not already in position (one trade per day)

### Filters (applied to reduce effective_dir to 0)

| Filter | Condition | Action |
|--------|-----------|--------|
| F1 | \|5d regime return\| > 3% AND score < 0.50 | Veto entry |
| F2 | PCR bearish + LONG + score < 0.55 | Suppress LONG |
| F3 | **Majority vote** (≥2/3 classifiers): FII_BULL → block SHORT; FII_BEAR → block LONG | Hard block |
| F4a | OI quadrant = bearish + LONG | Suppress LONG |
| F4b | Price run-up + LONG + strike defense against | Suppress LONG |
| F5 | Crash regime (5d < -3%) + extreme contango | Suppress LONG |
| F6 | Price momentum (last 30 bars) diverges from direction | Suppress |
| no_intraday | velocity_data empty AND fii_dii_result is None | Veto |

**F3 uses majority vote across 3 classifiers** (BN-primary, Combined, Nifty-ref) to avoid single-classifier miscalibration blocking all entries. Falls back to single BN-primary if only one classifier loaded.

---

## Exit Logic

| Exit | Condition |
|------|-----------|
| Stop Loss | Option premium ≤ entry × (1 + SL_PCT) = 50% of entry |
| Take Profit | Option premium ≥ entry × (1 + TP_PCT) = 2× entry |
| Reversal | Signal crosses threshold in opposite direction (checked every 5 bars, after MIN_REVERSAL_HOLD=20 bars) |
| EOD | 15:20 IST (both NIFTY and BANKNIFTY) |

---

## Key Constants

| Constant | NIFTY | BANKNIFTY | Source |
|----------|-------|-----------|--------|
| Lot size | **65** | **30** | NSE (confirmed May 2026) |
| Strike step | 50 | 100 | NSE |
| Entry bar | 105 (11:00 AM) | 105 (11:00 AM) | Backtest MIN_SIGNAL_BAR |
| Last entry | 13:00 | 13:00 | Backtest |
| EOD exit | 15:20 | **15:20** | Backtest (BN fixed May 2026) |
| SL | -50% | -50% | Backtest |
| TP | +100% | +100% | Backtest |
| Score min | 0.35 | 0.35 | Backtest |
| Min signals | 5/6 | 5/6 | Backtest |
| Momentum bars | 30 | 30 | Backtest |
| Reversal hold | 20 bars | 20 bars | Backtest |
| OI history len | 60 bars | 60 bars | Backtest VELOCITY_WINDOW |

---

## Backtest Results (Jan–Apr 2026)

### NIFTY (LOT=65)
- Trading days: 64 | Trades: 22 | Win rate: 68.2%
- Total P&L: ₹26,722 | Avg/trade: ₹1,215
- Win/Loss ratio: 1.03x | Max win: ₹9,058 | Max loss: ₹-5,379

### BANKNIFTY (LOT=30)
- Trading days: 33 | Trades: 8 | Win rate: 75.0%
- Total P&L: ₹27,874 | Avg/trade: ₹3,484
- Win/Loss ratio: 2.89x | Max win: (single TP trade at ₹9,930)

### Combined
- Total P&L: **₹54,596** across 30 trades

---

## Remaining Known Gaps (May 2026)

| Gap | Severity | Note |
|-----|----------|------|
| Entry execution timing | INFO | Runner enters current bar; backtest simulates next-bar fill. ~1 min slippage. Intentional. |
| Score gate (0.35 global) | LOW | Runner has global 0.35 floor; backtest only conditional. Runner is more conservative — acceptable. |
| alert_runner.py isolation | HIGH | Never run alongside restart_runners.sh. Document this clearly before any deployment. |
| End-to-end verification | VERIFY | No paper trade has fired post-fixes. Verify full entry→position→exit→alert chain on next signal. |

---

## Logging

Each runner writes to its own log file via `logging.FileHandler` (append mode):
- `v3/live/runner.log` — NIFTY
- `v3/live/runner_banknifty.log` — BANKNIFTY

`restart_runners.sh` launches with `> /dev/null 2>&1 &` to discard stdout/stderr — prevents duplicate lines from FileHandler + StreamHandler both writing to the same file.

---

## Classifier Details

Three FIIDIIClassifier instances run on BankNifty runner (one on Nifty runner):

| Classifier | Threshold File | Role |
|-----------|---------------|------|
| BN-primary | `fii_dii_thresholds_BANKNIFTY.json` | Drives F3 block |
| Combined | `fii_dii_thresholds_COMBINED.json` | Reference only |
| Nifty-ref | `fii_dii_thresholds.json` | Cross-reference |

Classifier becomes non-UNKNOWN after MIN_WINDOW=5 bars (~5 minutes post-start). Returns dict always (never None); attribution='UNKNOWN' when insufficient data.

---

## Telegram Alerts

Sent by runners on: entry signal, position update (every N bars), EOD exit, SL/TP/reversal exit. Config in `token.env`: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_IDS` (comma-separated for multiple recipients).
