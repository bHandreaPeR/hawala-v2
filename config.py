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
        'lot_size':          25,               # current lot size
        # NIFTY lot size history: changed 50→25 on 24-Nov-2023 per SEBI revision
        'lot_size_history': [
            ('2020-01-01', '2023-11-23', 50),  # 50 contracts/lot before revision
            ('2023-11-24', '2099-12-31', 25),  # 25 contracts/lot after revision
        ],
        'brokerage':         40,
        'slippage':          5,
        'min_gap':           30,               # ~0.1% of NIFTY at 30,000
        'max_gap':           200,
        'margin_per_lot':    55_000,           # approx SPAN + exposure margin (₹)
        'strike_interval':   50,
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
            'ORB_WINDOW_END':      '10:00',  # best window from legacy sweep
            'ORB_BREAKOUT_BUFFER': 10,       # best buffer from legacy sweep
            # ATR-based stops — fixes structural problem where ORB-range stops
            # were 200-400 pts making targets unreachable intraday.
            # Same pattern as VWAP: stop = ATR14 × multiplier.
            'ORB_USE_ATR_STOPS':   True,
            'ORB_STOP_ATR':        0.3,      # 0.3 × ATR14 ≈ 90-120 pts (BANKNIFTY)
            'ORB_TARGET_ATR':      0.6,      # target = 2× stop = 180-240 pts
            # Legacy params kept for backward compat / mode='legacy' sweep
            'ORB_STOP_PCT':        0.005,
            'ORB_TARGET_R':        2.0,
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
