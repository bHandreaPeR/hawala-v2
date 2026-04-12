# Hawala v2 — BankNifty Gap Fill + Trailing Stop Strategy

Fully automated BankNifty intraday trading algorithm built for Google Colab
with the Groww free API. Target: ₹2,000/day average profit.

---

## Strategy Overview

**Core idea:** BankNifty opening gaps tend to revert to the previous close.

- Gap UP → SHORT (expect price to fall back to prev close)
- Gap DOWN → LONG (expect price to rise back to prev close)

**Trailing Stop mechanism:** When the gap fills (TP hit), instead of closing
the trade, the SL is moved to that level and the TP advances by STEP_PTS.
This locks in profit while letting winners run further.

---

## Validated Results (1 lot, 15 units)

| Year | Trades | Win Rate | P&L (₹) |
|------|--------|----------|----------|
| 2022 | ~130   | ~55%     | 92,362   |
| 2023 | ~130   | ~55%     | 91,984   |
| 2024 | ~130   | ~55%     | 90,183   |
| 2025 | ~163   | ~57%     | 116,406  |
| **Total** | **~553** | **~55%** | **₹4,05,936** |

2025 is out-of-sample (not used in parameter optimisation).

---

## Files

| File | Description |
|------|-------------|
| `cell_1_setup.py` | Installs, imports, Groww API authentication |
| `cell_2_data_fetch.py` | BankNifty 15-min OHLCV from Groww (chunked 90-day requests) |
| `cell_3_gap_fill_strategy.py` | Core gap fill backtest with candle-by-candle trailing stop |
| `cell_4_options_simulation.py` | Black-Scholes ATM options simulation with intraday theta decay |
| `cell_5_macro_filters.py` | India VIX, S&P 500, and FII macro filter layer |
| `cell_6_fii_fetch.py` | FPI/FII daily net activity from NSE via nselib |
| `requirements.txt` | Python dependencies |

---

## Key Parameters

```python
SLIPPAGE    = 10     # pts — entry/exit slippage
STOP_PTS    = 80     # pts — initial hard stop loss
LOT_SIZE    = 15     # BankNifty lot size (post Nov 2023)
BROKERAGE   = 40     # ₹ per round trip
MIN_GAP_PTS = 50     # ignore gaps smaller than this
MAX_GAP_PTS = 400    # ignore gaps larger than this (fundamental)
STEP_PTS    = 75     # trailing ladder step size
```

## Macro Filter Thresholds

```python
VIX_THRESHOLD = 19.0     # skip if India VIX > 19
SP_THRESHOLD  = -1.5     # skip if S&P overnight return < -1.5%
FPI_THRESHOLD = -3000    # skip if FPI net < -3000 Cr
```

---

## How to Run (Google Colab)

1. Install dependencies:
   ```
   !pip install growwapi scikit-learn yfinance nselib xlrd -q
   ```

2. Paste your Groww API token into `cell_1_setup.py`:
   ```python
   API_AUTH_TOKEN = "your_token_here"
   ```
   Token resets daily at 6 AM — regenerate each morning.

3. Run cells in order: 1 → 2 → 3 → 4 → 5 → 6

4. After Cell 6, uncomment `run_macro_backtest(...)` in Cell 5 to
   apply all filters together.

---

## Broker

**Groww** (free plan). CASH segment historical data available.
1-min candles: last ~60–90 days only (for live execution).
15-min candles: full history available.

---

## Roadmap

- [x] Core gap fill strategy with trailing stop
- [x] Options simulation (Black-Scholes, intraday theta)
- [x] VIX + S&P macro filters
- [x] FII/FPI data integration
- [ ] Volume signals (first-candle volume ratio, prev-day volume)
- [ ] Chartiny delivery % integration
- [ ] Combined macro filter backtest (Cell 9)
- [ ] Paper trading setup
- [ ] Live execution bot (1-min candles, Mac launchd scheduler)
