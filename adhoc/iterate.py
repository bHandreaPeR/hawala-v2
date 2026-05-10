"""
iterate.py — Self-contained runner for candlestick F+O backtest.
Loads token from token.env. Caches futures data across iterations.
"""
import os, sys, pickle, pathlib
from dotenv import load_dotenv

load_dotenv('token.env')
TOKEN = os.getenv('GROWW_API_KEY', '').strip()
if not TOKEN:
    sys.exit("❌  GROWW_API_KEY not found in token.env")

from growwapi import GrowwAPI
groww = GrowwAPI(TOKEN)
print("✅  Groww authenticated")

from run_candlestick_backtest import run_candlestick_fno, _fetch

INSTRUMENT   = 'BANKNIFTY'
START        = '2022-01-01'
END          = '2024-12-31'
STARTING_CAP = 1_00_000
CACHE_FILE   = pathlib.Path(f'trade_logs/_data_cache_{INSTRUMENT}_{START}_{END}.pkl')

# ── Load or fetch futures data ─────────────────────────────────────────────────
if CACHE_FILE.exists():
    print(f"📦  Loading cached futures data from {CACHE_FILE}")
    with open(CACHE_FILE, 'rb') as f:
        data = pickle.load(f)
    print(f"    {len(data)} candles  ({data.index[0].date()} → {data.index[-1].date()})")
else:
    print("🌐  Fetching futures data from Groww API...")
    data = _fetch(groww, INSTRUMENT, START, END, use_futures=True)
    CACHE_FILE.parent.mkdir(exist_ok=True)
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(data, f)
    print(f"💾  Saved to {CACHE_FILE}")

# ── Run backtest ───────────────────────────────────────────────────────────────
run_candlestick_fno(
    groww,
    instrument = INSTRUMENT,
    start      = START,
    end        = END,
    starting   = STARTING_CAP,
    data       = data,
)
