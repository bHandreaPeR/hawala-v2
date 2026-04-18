# Hawala v2 — Project Skills & Learnings

> This file documents everything Claude has learned about this project.
> Read this at the start of every session to pick up context fast.
> Last updated: April 13, 2026

---

## 1. What This Project Is

**Goal:** Fully automated BankNifty gap-fill trading algorithm targeting ₹2,000/day net P&L.

**Strategy:** BankNifty opens with a gap (up or down) → price tends to fill the gap during the session → enter in the opposite direction of the gap → trail stop at STEP_PTS=75 → exit on gap fill or stop.

**Validated:** 4-year backtest, 686 trades, P&L ₹4,05,936, validated in notebook Cell 8.

**Notebook:** `Hawala_v2_consolidated.ipynb` — 12 cells, build via `build_notebook.py`

---

## 2. Project File Map

```
Hawala v2/
├── Hawala_v2_consolidated.ipynb   # Main 12-cell notebook
├── build_notebook.py              # Script that generates the .ipynb JSON
├── gen_report.py                  # PDF report generator (reportlab)
├── push_to_github.sh              # One-command git push (uses stored PAT)
├── .github_token                  # GitHub PAT (do NOT commit this)
├── paper_trades.csv               # Daily paper trade log (Cell 12 output)
├── fii_data.csv                   # FII/FPI net data cache
├── requirements.txt               # Python dependencies
├── SKILLS.md                      # This file
└── market_report_YYYY-MM-DD.pdf   # Daily pre-market report (auto-generated)
```

---

## 3. Notebook Cell Summary

| Cell | Purpose | Key Detail |
|------|---------|-----------|
| 1 | Setup & imports | yfinance, pandas, numpy, scipy |
| 2 | Data fetch (BankNifty OHLCV) | yfinance MultiIndex fix required (see §6) |
| 3 | Gap fill strategy core | STEP_PTS=75, entry logic, trailing stop |
| 4 | Options simulation (Black-Scholes) | bs_full(), intraday theta in HOURS not days |
| 5 | Macro filters | VIX>19 skip, S&P<-1.5% skip, FPI<-3000Cr skip |
| 6 | FII/FPI fetch | Uses NSDL data, cached to fii_data.csv |
| 7 | Backtest engine | 4-year run, P&L tracking |
| 8 | **VIX + Macro filter sweep** | Cross-validates thresholds empirically |
| 9 | Greeks & options P&L | Full IV, delta, gamma, theta, vega |
| 10 | FPI threshold sweep | Empirical FPI cutoff validation |
| 11 | Combined macro backtest | All filters applied together |
| 12 | **Daily paper trade checker** | fetch_today(), check_today_signal(), log_result(), show_week_summary() |

---

## 4. Daily Pre-Market Report — Dual Output Architecture

### Architecture
- **Trigger:** Cron task `daily-trading-alerts` at 7:30 AM IST, Mon–Fri
- **Task ID:** `daily-trading-alerts`
- **Flow:** Claude wakes → WebFetch ALL APIs → writes two output files → Bash runs `gen_report.py` → PDF + Signal JSON in workspace

### Two Output Files Per Day
| File | Purpose | Consumer |
|------|---------|----------|
| `market_report_YYYY-MM-DD.pdf` | 9-page newsletter | Subhransu (human reads) |
| `market_signal_YYYY-MM-DD.json` | Structured signal data | Claude (machine reads for trade decisions) |

### gen_report.py Usage
```bash
# With live data JSON (cron mode):
python3 gen_report.py --data data_2026-04-14.json market_report_2026-04-14.pdf

# With built-in sample data (testing):
python3 gen_report.py market_report_test.pdf
```

### PDF Structure (9 pages)
1. **Page 1:** Fear & Greed gauge + weekly change + quote
2. **Page 2:** Overview — GIFT Nifty, Asian/Europe/US/India markets
3. **Page 3:** Commodities (spot + MCX futures), Crypto, Currencies
4. **Page 4:** Snapshot — Top Gainers/Losers, Volume Shockers, 52W High, Long/Short Buildup
5. **Page 5:** Sectoral Indices (1D + 7D)
6. **Page 6:** India Market Bulletin — top 12-15 news from prev close
7. **Page 7:** Nifty 50 Analysis — OI highlights, PCR trend, FII/DII flows, Pivot Levels
8. **Page 8:** Bank Nifty Analysis — OI bar chart, ATM Greeks, PCR trend, Pivots, Advance/Decline
9. **Page 9:** Hawala v2 Signal — macro filter checklist, gap signal, trade params, macro context

### Signal JSON (`signal_schema.json` = reference template)
Key sections for trade decision-making:
- `macro_filters` — VIX/S&P/FII pass/fail + `all_pass`
- `gap_signal` — direction, %, LONG/SHORT/NO TRADE, `trade_enabled`
- `trade_params` — entry, trailing SL (75 pts), target, R:R
- `banknifty_options` — PCR, max pain, call resistance, put support, ATM Greeks (IV/delta/gamma/theta/vega)
- `nifty_options` — PCR, max pain, key strikes
- `fii_dii` — cash flows + F&O index futures long/short ratio
- `signal_confidence` — 0-10 score + note (DERIVED by Claude, not fetched)

---

## 5. Data Sources & Reliability

### ALWAYS fetch via WebFetch (never estimate, never use memory):

| Data Point | Primary Source | Fallback |
|-----------|---------------|---------|
| GIFT Nifty | `https://www.nseindia.com/api/giftNifty` | https://groww.in/indices/global-indices/sgx-nifty |
| Fear & Greed | `https://production.dataviz.cnn.io/index/fearandgreed/graphdata` | https://feargreedmeter.com |
| US/Global markets | `https://query1.finance.yahoo.com/v8/finance/chart/{TICKER}?interval=1d&range=5d` | WebSearch with date |
| Crypto | `https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_24hr_change=true` | WebSearch |
| MCX Futures | `https://www.mcxindia.com/market-data/commodity-wise-market-summary` | WebSearch |
| NSE Gainers/Losers | `https://www.nseindia.com/api/live-analysis-variations?index=gainers` | WebSearch |
| Sectoral Indices | `https://www.nseindia.com/api/allIndices` | WebSearch |

### NSE Option Chain APIs:
```
Nifty OC:     https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY
BankNifty OC: https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY
FII/DII cash: https://www.nseindia.com/api/fiidiiTradeReact
FII F&O:      https://www.nseindia.com/api/fii-derivatives-statistics-dl
All indices:  https://www.nseindia.com/api/allIndices
Gainers:      https://www.nseindia.com/api/live-analysis-variations?index=gainers
Losers:       https://www.nseindia.com/api/live-analysis-variations?index=loosers
Volume:       https://www.nseindia.com/api/live-analysis-cannons?index=volume
52W High:     https://www.nseindia.com/api/live-analysis-cannons?index=52Week
Long buildup: https://www.nseindia.com/api/live-analysis-variations?index=futures_asc
Short buildup:https://www.nseindia.com/api/live-analysis-variations?index=futures_desc
Note: NSE APIs may require session cookies → if 403, fallback to WebSearch
```

### Option Chain — Key Derived Fields:
```
PCR           = sum(all PE open interest) / sum(all CE open interest)
PCR > 1       = more put buyers = bearish hedge
PCR < 1       = more call buyers = bullish
Max Pain      = strike where sum of option buyer losses is maximized
Call wall     = strike with highest CE OI (resistance)
Put wall      = strike with highest PE OI (support)
```

### Pivot Level Formulas (DERIVED from prev OHLC):
```python
PP = (H + L + C) / 3
# Classic:
R1=2*PP-L; R2=PP+(H-L); R3=H+2*(PP-L)
S1=2*PP-H; S2=PP-(H-L); S3=L-2*(H-PP)
# Fibonacci:
R1=PP+0.382*(H-L); R2=PP+0.618*(H-L); R3=PP+(H-L)
S1=PP-0.382*(H-L); S2=PP-0.618*(H-L); S3=PP-(H-L)
```

### Yahoo Finance tickers (exact):
```
^GSPC  S&P 500        ^DJI    Dow Jones       ^IXIC  Nasdaq
^VIX   VIX            ^NSEI   Nifty 50        ^BSESN Sensex
^NSEBANK  Bank Nifty  ^INDIAVIX India VIX
^N225  Nikkei 225     ^HSI    Hang Seng       ^KS11  KOSPI
^AXJO  ASX 200        ^GDAXI  DAX             ^FTSE  FTSE 100
^FCHI  CAC 40         GC=F    Gold            SI=F   Silver
BZ=F   Brent Crude    CL=F    WTI Crude
USDINR=X  USD/INR     EURUSD=X EUR/USD        USDJPY=X USD/JPY
```

### Yahoo Finance v8 JSON response parsing:
```
meta.regularMarketPrice      → current price
meta.previousClose           → previous close
meta.regularMarketChange     → abs change
meta.regularMarketChangePercent → % change (multiply by 1, already in %)
```

### India timing note:
- At 7:30 AM IST, Indian markets have NOT opened.
- `^NSEI`, `^BSESN`, `^NSEBANK` will show PREVIOUS DAY close.
- GIFT Nifty IS live at 7:30 AM — use it as the pre-open indicator.

---

## 6. Known Bugs & Fixes

### yfinance MultiIndex bug (CRITICAL)
Newer versions of yfinance return a MultiIndex DataFrame. Always fix:
```python
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.droplevel(1)
close = df['Close'].squeeze()
```
Without this, `india_vix` dict is empty → all VIX values NaN → all trades pass → inflated P&L.

### VIX filter producing flat 686-trade output
Root cause: empty `india_vix` dict due to MultiIndex bug above. Fix above resolves it.

### SyntaxError in build_notebook.py
Triple-quoted docstrings inside triple-quoted cell strings conflict. Replace `"""docstring"""` with `# comment` inside cell strings.

### git index.lock on Mac filesystem
Mac mounted filesystem doesn't allow sandbox to delete `.git/index.lock`. Workaround: use temp dir at `/sessions/great-determined-bardeen/hawala_tmp/` for git ops, then run push from Mac.

### github.com blocked in sandbox
Sandbox proxy blocks github.com. Solution: `push_to_github.sh` script for user to run from Mac terminal using stored PAT.

### Notion replace_content wipes page
Never use `replace_content` on Notion — it stores content as a single flat block. Always use `update_content` with targeted edits only. Always fetch first before any update.

---

## 7. What My Report Got Wrong vs Reference (April 13, 2026)

The first attempt at the daily report had major errors because I used WebSearch snippets instead of live API calls:

| Field | My Estimate | Actual |
|-------|------------|--------|
| Fear & Greed | 38 (Fear) | **54.92 (Greed)** |
| GIFT Nifty | +24,091 (+0.17% gap-UP) | **23,778 (−1.30% gap-DOWN)** |
| Bank Nifty prev close | 54,821 | **55,912.75** |
| Nikkei level | ~38,180 | **56,528** |
| Hang Seng level | ~21,840 | **25,623** |
| KOSPI level | ~2,580 | **5,809** |
| BTC | $78,284 (−6.14%) | **$71,112 (+0.50%)** |
| ETH | $2,409 (−9.92%) | **$2,203 (+0.53%)** |
| EUR/USD | 1.0795 | **1.17** |

**Root cause:** WebSearch returns text snippets from articles (often hours or days old). Never use WebSearch for market data. Always use WebFetch on live JSON API endpoints.

---

## 8. Hawala v2 Strategy Parameters

```python
STEP_PTS     = 75       # Trailing stop in index points
GAP_MIN_PCT  = 0.002    # Min gap to trade (0.2%)
SESSION_END  = "15:25"  # Force-exit time
VIX_SKIP     = 19.0     # Skip day if India VIX > this
SP_SKIP      = -1.5     # Skip if S&P overnight < -1.5%
FPI_SKIP     = -3000    # Skip if FPI net < -₹3,000 Cr
```

## 9. Options Simulation (bs_full)

```python
# Intraday theta decay: use hours remaining, not calendar days
time_to_exp = hours_remaining / (6.25 * 252)  # 6.25 trading hours/day

# Black-Scholes Greeks computed: delta, gamma, theta, vega, rho
# Full IV surface via scipy.optimize.brentq
```

## 10. Notion Project Doc

- **URL:** https://www.notion.so/340b040b57a48145883cd854d379fc97
- **Page ID:** `340b040b-57a4-8145-883c-d854d379fc97`
- **Rule:** ALWAYS fetch first (`notion-fetch`), then use `update_content` for targeted edits.
- **NEVER use** `replace_content` — it wipes all structured blocks.

---

## 11. GitHub

- **Repo:** https://github.com/subhraba01/hawala-v2
- **Push:** Run from Mac: `bash "/Users/subhransubaboo/Claude Projects/Hawala v2/Hawala v2/push_to_github.sh"`
- **Token:** Stored in `.github_token` (do NOT read aloud or commit)

---

## 12. Paper Trade Protocol (Cell 12)

After market close (3:30 PM IST), run Cell 12 in the notebook:
```python
fetch_today()           # Get today's BankNifty OHLCV
check_today_signal()    # Determine gap direction and signal
log_result()            # Write to paper_trades.csv
show_week_summary()     # Print week P&L summary
```
Do this daily. Review weekly. Compare vs backtest expectations.

---

## 13. Sandbox Limitations (Important)

| Tool | External Network | Notes |
|------|-----------------|-------|
| Bash/requests | ❌ BLOCKED | Proxy blocks all external APIs |
| Claude WebFetch | ✅ Works | Use for all live data fetching |
| Claude WebSearch | ⚠️ Stale | Returns snippets, not live data — NEVER use for market numbers |
| yfinance (Python) | ❌ Not installed | Works on user's Mac, not sandbox |
| github.com (Bash) | ❌ BLOCKED | Use push_to_github.sh from Mac |

**Critical rule:** For any numeric market data, use WebFetch on a specific JSON API URL. Never trust WebSearch results for prices, indices, or rates.
