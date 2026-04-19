# ============================================================
# config.py — Hawala v2 Central Registry
# ============================================================
# Single source of truth for:
#   - Instrument specs (symbol, lot size, margin, gaps)
#   - Strategy registry (module path, default params)
#   - Capital model parameters
#
# Adding a new instrument: add an entry to INSTRUMENTS.
# Adding a new strategy : add an entry to STRATEGIES.
# ============================================================

# ── Instrument Registry ───────────────────────────────────────────────────────
INSTRUMENTS = {
    'BANKNIFTY': {
        'symbol':            'NSE-BANKNIFTY',  # Groww spot symbol (SEGMENT_CASH)
        'underlying_symbol': 'BANKNIFTY',      # for get_expiries() / get_contracts()
        'lot_size':          15,               # current lot size (post 20-Nov-2023)
        # Historical lot size changes — used for accurate P&L across backtest periods
        'lot_size_history': [
            ('2020-01-01', '2023-11-19', 25),  # 25 contracts/lot before revision
            ('2023-11-20', '2099-12-31', 15),  # 15 contracts/lot after SEBI revision
        ],
        'brokerage':         40,               # ₹ per round trip (Groww)
        'slippage':          10,               # pts assumed on entry/exit
        'min_gap':           50,               # ignore gaps smaller than this (pts)
        'max_gap':           400,              # ignore fundamental gaps (pts)
        'margin_per_lot':    75_000,           # approx SPAN + exposure margin (₹)
        'strike_interval':   100,              # ATM strike rounding for options
    },
    'NIFTY': {
        'symbol':            'NSE-NIFTY',
        'underlying_symbol': 'NIFTY',
        'lot_size':          25,               # current lot size (post 24-Nov-2023)
        # NIFTY lot size history: changed 50→25 on 24-Nov-2023 per SEBI revision
        'lot_size_history': [
            ('2020-01-01', '2023-11-23', 50),  # 50 contracts/lot before revision
            ('2023-11-24', '2099-12-31', 25),  # 25 contracts/lot after revision
        ],
        'brokerage':         40,
        'slippage':          5,
        'min_gap':           30,               # TODO: re-evaluate after gap_fill sweep
        'max_gap':           200,
        'margin_per_lot':    55_000,           # approx SPAN + exposure margin (₹)
        'strike_interval':   50,
        # Per-instrument strategy param overrides — merged over global STRATEGIES params
        # by backtest/engine.py run_backtest().
        # Gap fill sweep on NIFTY: best combo still ₹-3,194 (37.8% WR) → DO NOT run
        # gap fill on NIFTY. ORB + VWAP only.
        # VWAP sweep on NIFTY: band=0.25%, stop_atr=0.75, target_atr=1.50
        # gives ₹8,092 / 46 trades / 50% WR over 2022-2024.
        'strategy_params': {
            # VWAP — calibrated for NIFTY price level (~₹19k)
            'VWAP_BAND_PCT':   0.0025,  # 0.25% = ~47 pts at NIFTY 19k (same % as BN)
            'VWAP_STOP_ATR':   0.75,    # wider stop works better on NIFTY
            'VWAP_TARGET_ATR': 1.50,    # 2:1 R:R on NIFTY VWAP
        },
    },
}

# ── Strategy Registry ─────────────────────────────────────────────────────────
# 'module'  : dotted import path to the strategy module
# 'function': entry-point function name (run_<strategy>)
# 'params'  : default strategy parameters (passed as strategy_params dict)
STRATEGIES = {
    'gap_fill': {
        'module':   'strategies.gap_fill',
        'function': 'run_gap_fill',
        'params': {
            'STEP_PTS': 75,    # trailing ladder step (pts)
            'STOP_PTS': 80,    # initial hard stop (pts)
        },
    },
    'orb': {
        'module':   'strategies.orb',
        'function': 'run_orb',
        'params': {
            'ORB_WINDOW_END':      '09:30',  # sweep optimal — tighter range, earlier signal
            'ORB_BREAKOUT_BUFFER': 5,        # sweep optimal for 2024+2025 (was 10)
            # ATR-based stops — recalibrated on 2024+2025 data
            # Old target_atr=0.75 was too far (daily ATR ~600 pts → target=450 pts, hit rate 7%)
            # New target_atr=0.45 → target=270 pts, hit rate improves dramatically
            # Combined 2024+2025 sweep winner: stop=0.40, target=0.45, window=09:30, buf=5
            'ORB_USE_ATR_STOPS':   True,
            'ORB_STOP_ATR':        0.40,     # 0.40 × ATR14 ≈ 240 pts (BANKNIFTY daily ATR ~600)
            'ORB_TARGET_ATR':      0.45,     # 0.45 × ATR14 ≈ 270 pts — reachable in 2-3H
            # Breakeven trailing stop — move SL to entry after 0.20 × ATR14 profit
            # Protects against reversals without cutting short winning trades
            'ORB_BREAKEVEN_ATR':   0.0,   # disabled — empirically hurts by converting sq-off wins to BE (-₹40 brokerage)
            # IC-validated filters (from research/signal_ic.py on 192 trades)
            # Thu=35% WR (expiry pinning), Mon=44% WR — skip both; Tue/Wed are 71/81% WR
            'ORB_DOW_ALLOW':       [1, 2, 4],  # Tue=1, Wed=2, Fri=4 (Mon/Thu excluded)
            # Large gaps (>100pt) routed to options_orb; futures ORB stays on moderate gaps
            'ORB_MAX_GAP_FUTURES': 100,
            # Tight ORB range filter: IC=0.22 validated but threshold needs sweep
            # Set to None (disabled) until sweep calibrates the right cutoff
            'ORB_RANGE_ATR_MAX':   None,
            # Legacy params kept for backward compat / mode='legacy' sweep
            'ORB_STOP_PCT':        0.005,
            'ORB_TARGET_R':        2.0,
        },
    },
    'options_orb': {
        'module':   'strategies.options_orb',
        'function': 'run_options_orb',
        'params': {
            'ORB_WINDOW_END':      '09:30',
            'ORB_BREAKOUT_BUFFER': 5,
            'OPTIONS_GAP_MIN':     100,    # fires on gaps > 100 pts (large gap convexity play)
            'OPTIONS_DOW_ALLOW':   [1, 2, 4],  # same DOW filter as futures ORB
            'OPTIONS_RISK_PCT':    0.10,   # 10% of equity = defined max loss (premium paid)
            'OPTIONS_TARGET_MULT': 2.0,    # 2× entry premium → EV positive at 48% WR
            'OPTIONS_STOP_MULT':   0.50,   # stop at 50% of entry premium
            'OPTIONS_SQUAREOFF':   '12:00',
        },
    },
    'vwap_reversion': {
        'module':   'strategies.vwap_reversion',
        'function': 'run_vwap_reversion',
        'params': {
            'VWAP_BAND_PCT':   0.0025,  # 0.25% = ~100 pts @ BN 40k (sweep optimal)
            'VWAP_STOP_ATR':   0.5,     # 0.5x ATR14 as stop (sweep optimal)
            'VWAP_TARGET_ATR': 0.75,    # 0.75x ATR14 as target (sweep optimal)
        },
    },
    'candlestick': {
        'module':   'strategies.candlestick',
        'function': 'run_candlestick',
        'params': {
            # Native 15-min bar pattern detection (no resampling).
            # All periods are in 15-min bar counts:
            #   EMA_FAST=20  → 20×15min = 5H  (intraday trend)
            #   EMA_SLOW=50  → 50×15min ≈ 2 days
            #   RSI_PERIOD=14 → 14×15min = 3.5H
            'STOP_ATR':      1.0,   # 1×ATR14 stop
            'TARGET_ATR':    2.0,   # 2:1 R:R — breakeven WR = 33.3%
            'EMA_FAST':      20,
            'EMA_SLOW':      50,
            'RSI_PERIOD':    14,
            # Tighter RSI windows for 15-min (less range than 1H)
            'RSI_LONG_MIN':  42, 'RSI_LONG_MAX':  65,
            'RSI_SHORT_MIN': 35, 'RSI_SHORT_MAX': 58,
            # BODY_ATR_MIN raised to 0.5 to filter noise on 15-min bars
            # (15-min has 4× more bars than 1H — more false patterns without this)
            'BODY_ATR_MIN':  0.5,
            'WICK_RATIO':    2.0,
        },
    },
}

# ── Capital Model ─────────────────────────────────────────────────────────────
# Used by backtest/combiner.py to size lots and track deployed capital.
CAPITAL = {
    'starting':        1_00_000,   # ₹1,00,000 default starting capital
    'max_capital_pct': 0.90,       # never deploy more than 90% at once
    'min_lots':        1,          # minimum trade size (lots)
    # margin_per_lot is pulled from INSTRUMENTS[instrument]['margin_per_lot']
}

# ── Macro Filter Defaults ─────────────────────────────────────────────────────
MACRO = {
    'vix_threshold': 19.0,     # skip if India VIX > this
    'sp_threshold':  -1.5,     # skip if S&P 500 overnight return < this %
    'fpi_threshold': -3000.0,  # skip if FPI net cash < this ₹ Cr
    'min_filters':   2,        # 2-of-3 voting to block a trade
    'brent_spike':   2.0,      # Brent % move → regime tag (not a block)
    'usdinr_spike':  0.4,      # USD/INR % move → regime tag (not a block)
}
