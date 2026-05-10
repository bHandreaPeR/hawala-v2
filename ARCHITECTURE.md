# Hawala v2 — Live Architecture & Final Backtest

> Generated 2026-05-10 after the P0 risk-control pass (realistic slippage,
> per-trade risk cap, daily loss limit, two-bot Telegram split).

---

## 1. Process topology

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              token.env (shared)                            │
│  GROWW_API_KEY / GROWW_TOTP_SECRET                                         │
│  TELEGRAM_BOT_TOKEN          TELEGRAM_CHAT_IDS         (TRADE bot)         │
│  TELEGRAM_BOT_TOKEN_MACRO    TELEGRAM_CHAT_IDS_MACRO   (MACRO bot)         │
└────────────────────────────────────────────────────────────────────────────┘
                                     │
       ┌─────────────────────────────┼──────────────────────────────┐
       ▼                             ▼                              ▼
┌───────────────┐          ┌───────────────────┐         ┌──────────────────┐
│ alert_runner  │          │ alerts/vp_live_   │         │ bots/macro_bot   │
│      .py      │          │   daemon.py       │         │                  │
│               │          │                   │         │                  │
│  ORB / VWAP   │          │  VP-Trail-Swing   │         │  Macro briefs    │
│  futures      │          │  signals (3 inst) │         │  07:30 / 12:00   │
│  & options    │          │                   │         │  / 16:00 IST     │
│               │          │  polls every 5 m  │         │                  │
│  TRADE bot ──►│          │  TRADE bot ──────►│         │  MACRO bot ─────►│
│               │          │                   │         │                  │
│  09:15→15:30  │          │  09:15→15:30      │         │  fixed cron      │
└───────────────┘          └───────────────────┘         └──────────────────┘
       │                             │                              │
       └────────────► Telegram TRADE channel ◄────────┘              │
                                                                     │
                                       Telegram MACRO channel ◄──────┘
```

Three independent processes. Crash of one does not kill the others. Both
trade-side processes (alert_runner + vp_live_daemon) write to the **same**
TRADE Telegram channel so the user sees one chronological feed of signals.

---

## 2. Strategy stack (active)

| Strategy           | Vehicle    | Trigger                                 | Source                                    |
|--------------------|------------|-----------------------------------------|-------------------------------------------|
| Futures ORB        | Futures    | Gap 50–100 pts, Tue/Wed/Fri             | `strategies/orb.py`                       |
| Options ORB        | ATM option | Gap > 100 pts, Tue/Wed/Fri              | `strategies/options_orb.py`               |
| **VP-Trail-Swing** | Futures    | Pierce of 70% Value Area + reversal     | `strategies/vp_trailing_swing.py`         |

Per-instrument tuning lives in `run_canonical.py` → `CANONICAL_PARAMS`.

Dropped (in `_archived/`): VWAP_REV (decayed OOS), long-options overlay
(theta drag), credit spreads (-₹4.8L on 4½ years), original VP simple-target.

---

## 3. Risk controls (P0 — newly added)

```
┌──────────────────────────────────────────────────────────────────────────┐
│ Layer 1 — Strategy-level (vp_trailing_swing.py)                          │
│   VPT_SLIPPAGE_PTS         BN=30, NIFTY=10, SENSEX=20    (per leg)       │
│   VPT_DAILY_MAX_LOSS_PTS   BN=600, NIFTY=200, SENSEX=400 (halt new ents) │
│                                                                          │
│ Layer 2 — Compounding-engine (backtest/compounding_engine.py)            │
│   per_trade_risk_cap_pct=0.02  (2% equity-at-entry hard cap on a loss)   │
│   daily_loss_halt_pct=0.05     (5% intraday drawdown ⇒ skip new trades)  │
└──────────────────────────────────────────────────────────────────────────┘
```

Slippage is realistic — 30 pts/leg on BANKNIFTY ≈ what fast-market stop
orders actually fill at. NIFTY tighter (10), SENSEX in between (20).

---

## 4. Final backtest — 1-lot, no compounding, REALISTIC slippage

`python run_canonical.py` (4½ years: 2022-01 → 2026-05)

```
INSTRUMENT    TRADES    WR     P&L           AVG/TRADE    MAX-DD       MAX-LOSS
─────────────────────────────────────────────────────────────────────────────────
BANKNIFTY       218    27.5%  ₹  -57,470     ₹   -264    ₹ +187,956   ₹ -20,390
NIFTY           120    31.7%  ₹  +31,258     ₹   +260    ₹  +39,406   ₹  -6,101
SENSEX           92    41.3%  ₹  +70,470     ₹   +766    ₹  +14,012   ₹  -5,799
─────────────────────────────────────────────────────────────────────────────────
COMBINED        430    31.6%  ₹  +44,259     ₹   +103    ₹ +187,956   ₹ -20,390

  IS   (2022-2025, 4 yrs)  n=396  WR=31.1%  P&L ₹  -59,477  avg ₹ -150
  OOS  (2026 YTD,    5 mo) n= 34  WR=38.2%  P&L ₹+103,736  avg ₹+3,051
```

### Year-by-year

```
                BANKNIFTY              NIFTY                 SENSEX
year      n    wr     pnl       n    wr     pnl       n    wr     pnl
─────────────────────────────────────────────────────────────────────────
2022     45  26.7  +51,166     22  40.9  +22,801      —     —      —
2023     46  21.7  -81,062     22  31.8   -2,930      —     —      —
2024     53  35.8  +37,311     38  28.9  -15,635     32  40.6  +31,799
2025     58  25.9  -92,490     32  25.0   -8,277     48  39.6   -2,159
2026 ⚐   16  25.0  +27,605      6  50.0  +35,300     12  50.0  +40,831
─────────────────────────────────────────────────────────────────────────
                                                       ⚐ = OOS, 5mo only
```

### What this says

- **SENSEX is the strongest leg** — 41% WR, +₹70k on 92 trades, smooth equity
  curve, OOS 2026 is best year so far.
- **NIFTY is marginal but profitable** — 31.7% WR, low avg-per-trade (+₹260),
  small drawdowns, OOS 2026 already +₹35k.
- **BANKNIFTY is the problem child** — 27.5% WR, net negative IS, but OOS
  2026 is +₹27k. The 2023 (-₹81k) and 2025 (-₹92k) drawdowns are what the
  per-trade risk cap and daily loss halt are designed to soften in live.
- **OOS 2026** (5 months) on all 3 = +₹103k on 34 trades. Holds up.

### What changed vs the pre-slippage canonical run

```
                              BEFORE        AFTER
                              (5 pt slip)   (realistic 10/20/30)
─────────────────────────────────────────────────────────────────
BANKNIFTY  total P&L           +₹3.4 L      -₹0.6 L      (Δ -₹4 L)
NIFTY      total P&L           +₹0.5 L      +₹0.3 L      (Δ -₹0.2 L)
SENSEX     total P&L           +₹1.0 L      +₹0.7 L      (Δ -₹0.3 L)
─────────────────────────────────────────────────────────────────
COMBINED                       +₹4.9 L      +₹0.4 L      (Δ -₹4.5 L)
```

Slippage realism is **brutal** on BANKNIFTY because lot=30 multiplies the
30-pt fill cost: 218 trades × 30 lot × 60 pts round-trip ≈ ₹3.92 L of
realistic slippage that the prior 5-pt assumption was hiding.

**Conclusion**: The strategy still has positive expectancy net of realistic
costs (+₹44k on 1 lot each over 4½ yrs), but the edge is smaller than the
pre-slippage numbers suggested. The OOS-2026 sample is encouraging but
short. Paper-trade for 1–2 months before scaling.

---

## 5. Files reference

| File                                  | Purpose                                              |
|---------------------------------------|------------------------------------------------------|
| `run_canonical.py`                    | 1-lot reproducible backtest of the final config      |
| `run_baseline.py`                     | Compounded full-stack backtest (ORB + OPT_ORB + VPT) |
| `alert_runner.py`                     | Live ORB/VWAP/options daemon → TRADE bot             |
| **`alerts/vp_live_daemon.py`**        | **NEW** — Live VP signal daemon → TRADE bot          |
| `alerts/vp_signal_alert.py`           | Telegram message formatter for raw VP signals        |
| `bots/macro_bot.py`                   | Pre/mid/post-market briefs → MACRO bot               |
| `strategies/vp_trailing_swing.py`     | Canonical VP strategy (slippage + DLL added)         |
| `strategies/orb.py`                   | v2 ORB                                               |
| `strategies/options_orb.py`           | v2 Options ORB                                       |
| `backtest/compounding_engine.py`      | Sequential compounder (risk cap + daily halt added)  |
| `data/fetch_15m_futures.py`           | 15m futures cache fetcher                            |
| `research/trade_explorer.py`          | Per-trade chart visualisation                        |
| `_archived/`                          | Dropped strategies                                   |
| `DEPLOYMENT.md`                       | Operational runbook                                  |
| `ARCHITECTURE.md` *(this file)*       | Live architecture + final backtest                   |

---

## 6. Running the live stack

```sh
# 1. Trade-alert process (ORB / VWAP / options) — original bot
caffeinate -i python alert_runner.py

# 2. VP signal daemon — same TRADE bot, alongside alert_runner
caffeinate -i python -m alerts.vp_live_daemon --mode daemon

# 3. Macro brief daemon — separate MACRO bot
python -m bots.macro_bot --mode daemon
```

Or via cron:

```sh
# trade side (caffeinate keeps the laptop awake)
15  9 * * 1-5  cd /path/to/hawala && caffeinate -i python alert_runner.py
15  9 * * 1-5  cd /path/to/hawala && caffeinate -i python -m alerts.vp_live_daemon --mode daemon

# macro side (3 fixed slots OR one daemon)
30  7 * * 1-5  cd /path/to/hawala && python -m bots.macro_bot --mode premarket
 0 12 * * 1-5  cd /path/to/hawala && python -m bots.macro_bot --mode midday
 0 16 * * 1-5  cd /path/to/hawala && python -m bots.macro_bot --mode eod
```

---

## 7. Pre-live checklist

Before deploying real capital:

- [x] Slippage realism — done (BN=30 / NIFTY=10 / SENSEX=20 pt per leg)
- [x] Daily loss limit — done (BN=600 / NIFTY=200 / SENSEX=400 pt halt)
- [x] Per-trade risk cap — done (2% equity-at-entry in compounding engine)
- [x] Two-bot Telegram split — done (TRADE + MACRO)
- [x] Live VP signal daemon — done (`alerts/vp_live_daemon.py`)
- [ ] **1–2 months paper-trading** to verify live signals match backtest
- [ ] Live equity-curve dashboard — to detect strategy decay early
- [ ] Test daemon failover (what happens if Groww auth expires intraday)

The signal-daemon is best-effort: if Groww auth fails or the cache stops
updating, it will silently emit nothing. A heartbeat alert (every hour
"daemon alive, last bar = …") would close that gap.
