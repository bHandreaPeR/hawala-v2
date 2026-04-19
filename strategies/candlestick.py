# ============================================================
# strategies/candlestick.py — Candlestick Pattern Strategy
# ============================================================
# Signal  : bullish/bearish candlestick pattern on native 15-min bars
#           confirmed by EMA trend + RSI regime filters.
# Entry   : next 15-min open after the signal bar closes.
# Exit    : ATR-based stop / ATR-based target / 15:15 IST square-off.
#
# 15-min rationale: patterns on native resolution avoid resampling
# artefacts. BODY_ATR_MIN ≥ 0.5 filters the extra noise that comes
# with 4× more bars vs 1H. Phase-1 key-level + volume filters
# (not yet in this file) will further clean signals.
#
# Slippage applied ONCE at entry (in price). Exit price is the
# actual bar price — no second slippage deduction.
#
# Compounding capital columns (capital_at_entry, lots_at_entry,
# equity_after) are added downstream by backtest/compounding_engine.py.
# Contract/expiry/option columns are added by run_candlestick_backtest.py.
# ============================================================

import numpy as np
import pandas as pd
from datetime import time as dtime

from strategies.patterns import (
    atr, ema, rsi,
    detect_all_patterns,
)


# ── ATM strike helper ─────────────────────────────────────────────────────────
def _atm_strike(price: float, interval: int) -> int:
    return int(round(price / interval) * interval)


def _option_symbol(underlying: str, expiry_date, strike: int,
                   option_type: str) -> str:
    """
    Build an approximate NSE-style option symbol.
    expiry_date : datetime.date or pd.Timestamp
    """
    try:
        d = pd.Timestamp(expiry_date)
        return f"{underlying}{d.strftime('%d%b%y').upper()}{strike}{option_type}"
    except Exception:
        return f"{underlying}??{strike}{option_type}"


# ── Main strategy function ────────────────────────────────────────────────────
def run_candlestick(data: pd.DataFrame,
                    instrument_config: dict,
                    strategy_params: dict,
                    regime_df=None,
                    params=None) -> pd.DataFrame:
    """
    Candlestick-pattern backtest on native 15-min bars.

    Pipeline
    --------
    1. Compute ATR14, EMA_FAST, EMA_SLOW, RSI14 on the 15-min series.
    2. Detect every candlestick pattern on each 15-min bar.
    3. For each bar where a pattern fires AND EMA trend + RSI filter agree:
       enter at the NEXT bar's open (one 15-min bar look-ahead).
    4. Track stop / target on subsequent 15-min bars.
    5. Square off at 15:15 IST if neither stop nor target hit.

    Args
    ----
    data              : 15-min OHLCV DataFrame indexed by datetime (IST).
                        Must include Open, High, Low, Close. Volume optional.
    instrument_config : dict from config.INSTRUMENTS[instrument]
    strategy_params   : dict from config.STRATEGIES['candlestick']['params']
    regime_df         : optional DataFrame with [date, regime] for tagging
    params            : optional override dict used by sweep runner

    Returns
    -------
    pd.DataFrame — one row per trade with the full schema below.
    Capital / lot / contract columns are placeholders at this stage.
    """
    # ── Unpack instrument config ──────────────────────────────────────────────
    LOT_SIZE        = instrument_config.get('lot_size',       15)
    BROKERAGE       = instrument_config.get('brokerage',      40)
    SLIPPAGE        = instrument_config.get('slippage',        5)
    STRIKE_INTERVAL = instrument_config.get('strike_interval', 100)
    UNDERLYING      = instrument_config.get('underlying_symbol', 'BANKNIFTY')
    MARGIN          = instrument_config.get('margin_per_lot', 75_000)

    # ── Resolve params (sweep overrides strategy_params) ─────────────────────
    def _p(key, default):
        if params is not None and key.lower() in params:
            return params[key.lower()]
        return strategy_params.get(key, default)

    STOP_ATR      = _p('STOP_ATR',      1.0)
    TARGET_ATR    = _p('TARGET_ATR',    3.0)   # 3:1 R:R default (breakeven WR = 25%)
    EMA_FAST      = int(_p('EMA_FAST',  20))   # 20 × 15min = 5H intraday trend
    EMA_SLOW      = int(_p('EMA_SLOW',  50))   # 50 × 15min ≈ 2 days
    RSI_PERIOD    = int(_p('RSI_PERIOD', 14))
    RSI_LONG_MIN  = _p('RSI_LONG_MIN',  42)   # tightened from 35
    RSI_LONG_MAX  = _p('RSI_LONG_MAX',  65)   # tightened from 70
    RSI_SHORT_MIN = _p('RSI_SHORT_MIN', 35)
    RSI_SHORT_MAX = _p('RSI_SHORT_MAX', 58)   # tightened from 65
    BODY_ATR_MIN  = _p('BODY_ATR_MIN',  0.5)   # raised from 0.3 for 15-min noise
    WICK_RATIO    = _p('WICK_RATIO',    2.0)

    SQUAREOFF  = dtime(15, 15)
    ENTRY_CUT  = dtime(13, 0)   # no entries after 13:00 — enough time to reach target before square-off
    MIN_WARMUP = max(EMA_SLOW, RSI_PERIOD, 14) + 10  # bars before first trade

    # ── Regime lookup (optional) ──────────────────────────────────────────────
    regime_lookup = {}
    if regime_df is not None:
        for _, row in regime_df.iterrows():
            regime_lookup[row['date']] = row.get('regime', 'neutral')

    # ── Compute indicators on 15-min bars ────────────────────────────────────
    df = data.sort_index().copy()
    df = df.between_time('09:15', '15:30')

    if len(df) < MIN_WARMUP + 5:
        print("  ⚠ Not enough 15-min bars to compute indicators.")
        return pd.DataFrame()

    bar_atr  = atr(df, period=14)
    bar_ema_f = ema(df['Close'], EMA_FAST)
    bar_ema_s = ema(df['Close'], EMA_SLOW)
    bar_rsi   = rsi(df['Close'], RSI_PERIOD)
    bar_trend = np.where(bar_ema_f > bar_ema_s, 1,
                np.where(bar_ema_f < bar_ema_s, -1, 0))
    # 200-bar EMA ≈ 50H ≈ 2-week regime bias (LONG only above, SHORT only below)
    bar_ema200 = ema(df['Close'], 200)

    df['_atr']    = bar_atr.values
    df['_ef']     = bar_ema_f.values
    df['_es']     = bar_ema_s.values
    df['_rsi']    = bar_rsi.values
    df['_trend']  = bar_trend
    df['_ema200'] = bar_ema200.values

    # ── Intraday VWAP (resets each session) ──────────────────────────────────
    typical_price = (df['High'] + df['Low'] + df['Close']) / 3
    vwap_values = []
    _cur_date = None
    _cum_vol_tp = 0.0
    _cum_vol = 0.0
    for ts, row in df.iterrows():
        if ts.date() != _cur_date:
            _cur_date = ts.date()
            _cum_vol_tp = 0.0
            _cum_vol = 0.0
        vol = max(float(row.get('Volume', 1) or 1), 1)
        _cum_vol_tp += vol * float(typical_price[ts])
        _cum_vol += vol
        vwap_values.append(_cum_vol_tp / _cum_vol)
    df['_vwap'] = vwap_values

    # ── Rolling volume mean (20-bar) for confirmation ────────────────────────
    if 'Volume' in df.columns:
        df['_vol_mean'] = df['Volume'].rolling(20, min_periods=5).mean()
    else:
        df['_vol_mean'] = 1.0

    # ── Detect patterns on 15-min bars ───────────────────────────────────────
    patterns = detect_all_patterns(
        df, atr_series=bar_atr,
        body_atr_min=BODY_ATR_MIN, wick_ratio=WICK_RATIO,
    )
    df['_bull_hits']  = patterns['bullish_hits'].values
    df['_bear_hits']  = patterns['bearish_hits'].values
    df['_bull_names'] = patterns['bullish_names'].values
    df['_bear_names'] = patterns['bearish_names'].values

    # ── Iterate bars, find signals ────────────────────────────────────────────
    records  = []
    all_ts   = df.index.tolist()
    open_until: pd.Timestamp | None = None   # block new entries while trade is live

    for i, sig_ts in enumerate(all_ts):
        # Need warmup bars and a next bar to enter on
        if i < MIN_WARMUP or i + 1 >= len(all_ts):
            continue

        # One trade at a time — skip if previous trade still open
        if open_until is not None and sig_ts <= open_until:
            continue

        sig_bar = df.iloc[i]

        # Pattern ≥ 1: VWAP zone below acts as the quality gate, so single
        # patterns are acceptable — the VWAP deviation confirms mean-reversion context.
        bull = int(sig_bar['_bull_hits']) >= 1
        bear = int(sig_bar['_bear_hits']) >= 1
        if not bull and not bear:
            continue

        trend = int(sig_bar['_trend'])
        rsi_v = float(sig_bar['_rsi'])
        atr_v = float(sig_bar['_atr'])

        if atr_v <= 0 or np.isnan(atr_v):
            continue

        # ATR regime filter: only trade in below-median volatility (regression: best WR at atr_ratio<0.95)
        # Elevated ATR = choppy/event-driven market where pattern signals fail
        atr_median = bar_atr.iloc[max(0, i - 59):i + 1].median()
        if not np.isnan(atr_median) and atr_median > 0:
            if atr_v > 1.1 * atr_median:
                continue

        # ── VWAP zone check (mean reversion signal) ──────────────────────────
        # Buy when price is below VWAP (expect bounce); sell when above.
        # This replaces EMA direction — regression showed EMA has zero
        # predictive power; VWAP deviation captures the mean-reversion edge.
        vwap_v = float(sig_bar.get('_vwap', 0))
        close_v = float(sig_bar['Close'])
        vwap_dev = (close_v - vwap_v) / vwap_v if vwap_v > 0 else 0
        VWAP_BAND = 0.0025  # 0.25%

        direction = 0
        pat_name  = ''
        pat_count = 0
        if bull and vwap_dev <= -VWAP_BAND and (RSI_LONG_MIN <= rsi_v <= RSI_LONG_MAX):
            direction = 1
            pat_name  = sig_bar['_bull_names']
            pat_count = int(sig_bar['_bull_hits'])
        elif bear and vwap_dev >= VWAP_BAND and (RSI_SHORT_MIN <= rsi_v <= RSI_SHORT_MAX):
            direction = -1
            pat_name  = sig_bar['_bear_names']
            pat_count = int(sig_bar['_bear_hits'])

        if direction == 0:
            continue

        # ── Entry bar: next 15-min bar, same session ──────────────────────────
        entry_ts   = all_ts[i + 1]
        entry_bar  = df.iloc[i + 1]
        tdate      = sig_ts.date()

        if entry_ts.date() != tdate:
            continue    # no overnight entry
        if entry_ts.time() > ENTRY_CUT:
            continue    # only trade until entry cutoff
        if tdate.weekday() == 0:
            continue    # skip Monday — gap days, trend not yet established

        # Slippage applied once at entry
        raw_open = float(entry_bar['Open'])
        entry_px = raw_open + (SLIPPAGE if direction == 1 else -SLIPPAGE)

        stop_pts   = atr_v * STOP_ATR
        target_pts = atr_v * TARGET_ATR
        current_sl = entry_px - stop_pts if direction == 1 else entry_px + stop_pts
        current_tp = entry_px + target_pts if direction == 1 else entry_px - target_pts

        # ATM strike + option details (uses entry price as reference)
        atm_stk     = _atm_strike(raw_open, STRIKE_INTERVAL)
        opt_type    = 'CE' if direction == 1 else 'PE'

        # ── Simulate on subsequent 15-min bars ────────────────────────────────
        pnl_pts     = None
        exit_reason = None
        exit_ts     = None
        exit_px     = None

        sim_bars = df[(df.index.date == tdate) & (df.index > entry_ts)]

        for eidx, erow in sim_bars.iterrows():
            et = eidx.time()

            if et >= SQUAREOFF:
                exit_px     = float(erow['Open'])
                pnl_pts     = (exit_px - entry_px) * direction
                exit_reason = 'SQUARE OFF'
                exit_ts     = eidx
                break

            e_low  = float(erow['Low'])
            e_high = float(erow['High'])

            if direction == 1:
                if e_low <= current_sl:
                    exit_px     = current_sl
                    pnl_pts     = current_sl - entry_px
                    exit_reason = 'STOP LOSS'
                    exit_ts     = eidx
                    break
                if e_high >= current_tp:
                    exit_px     = current_tp
                    pnl_pts     = current_tp - entry_px
                    exit_reason = 'TARGET HIT'
                    exit_ts     = eidx
                    break
            else:
                if e_high >= current_sl:
                    exit_px     = current_sl
                    pnl_pts     = entry_px - current_sl
                    exit_reason = 'STOP LOSS'
                    exit_ts     = eidx
                    break
                if e_low <= current_tp:
                    exit_px     = current_tp
                    pnl_pts     = entry_px - current_tp
                    exit_reason = 'TARGET HIT'
                    exit_ts     = eidx
                    break

        if pnl_pts is None:
            # End-of-session fallback
            last_in_session = sim_bars[sim_bars.index.time < SQUAREOFF]
            if last_in_session.empty:
                continue
            last_bar    = last_in_session.iloc[-1]
            exit_px     = float(last_bar['Close'])
            pnl_pts     = (exit_px - entry_px) * direction
            exit_reason = 'SQUARE OFF'
            exit_ts     = last_in_session.index[-1]

        open_until = exit_ts   # block new signals until this trade exits

        # Net P&L: pnl_pts × lot_size × 1 lot (lots scaled by compounding engine)
        # No second slippage — exit_px is the actual bar price (stop/target/close)
        pnl_rs = round(pnl_pts * LOT_SIZE - BROKERAGE, 2)

        # Trade duration in minutes
        duration_min = int((exit_ts - entry_ts).total_seconds() // 60)

        # Bias score: 1 pattern = 0.33, 2 = 0.67, 3+ = 1.0
        bias_score = round(min(pat_count / 3.0, 1.0), 4)

        records.append({
            # ── Date / Time ─────────────────────────────────────────────────
            'date':          tdate,
            'entry_time':    entry_ts,
            'exit_time':     exit_ts,
            'duration_min':  duration_min,
            'year':          tdate.year,
            'month':         tdate.month,
            'day_of_week':   tdate.weekday(),    # 0=Mon, 4=Fri

            # ── Instrument ──────────────────────────────────────────────────
            'instrument':    UNDERLYING,
            'instrument_symbol': instrument_config.get('symbol', 'NSE-BANKNIFTY'),
            'instrument_type':   'FUT',          # updated to OPT by runbook if needed

            # Contract details — populated by run_candlestick_backtest.py
            # via _attach_contract_metadata (futures) or option symbol builder
            'contract':      '',
            'expiry_date':   None,

            # Option equivalent (ATM strike at entry)
            'atm_strike':    atm_stk,
            'option_type':   opt_type,           # CE / PE
            'option_symbol': '',                 # filled by runbook

            # ── Trade setup ─────────────────────────────────────────────────
            'strategy':      'CANDLESTICK',
            'direction':     'LONG' if direction == 1 else 'SHORT',
            'entry_price':   round(entry_px, 2),
            'exit_price':    round(exit_px,  2),
            'stop_price':    round(current_sl, 2),
            'target_price':  round(current_tp, 2),
            'stop_pts':      round(stop_pts,   2),
            'target_pts':    round(target_pts, 2),
            'rr_ratio':      round(target_pts / stop_pts, 2),

            # ── Position sizing (lots filled by compounding engine) ───────────
            'lot_size':      LOT_SIZE,           # contracts per lot
            'lots_traded':   1,                  # placeholder; overwritten downstream
            'margin_per_lot': MARGIN,
            'margin_used':   MARGIN,             # 1 lot placeholder
            'brokerage':     BROKERAGE,

            # ── P&L ─────────────────────────────────────────────────────────
            'pnl_pts':       round(pnl_pts, 2),
            'pnl_rs':        pnl_rs,
            'win':           1 if pnl_rs > 0 else 0,
            'exit_reason':   exit_reason,

            # ── Signal features ─────────────────────────────────────────────
            'pattern_name':  pat_name,
            'pattern_stack': pat_count,
            'bias_score':    bias_score,
            'signal_bar_time': sig_ts.time().strftime('%H:%M'),
            'atr14_15m':     round(atr_v, 2),
            'ema_fast':      round(float(sig_bar['_ef']), 2),
            'ema_slow':      round(float(sig_bar['_es']), 2),
            'ema_trend':     int(trend),
            'rsi14':         round(rsi_v, 2),
            'vwap':          round(vwap_v, 2),
            'vwap_dev_pct':  round(vwap_dev * 100, 4),

            # ── Compounding placeholders ─────────────────────────────────────
            'capital_at_entry': np.nan,
            'lots_at_entry':    np.nan,
            'equity_after':     np.nan,

            # ── Macro ────────────────────────────────────────────────────────
            'regime':   regime_lookup.get(tdate, 'neutral'),
            'macro_ok': True,
        })

    if not records:
        return pd.DataFrame()

    tl = pd.DataFrame(records)
    tl = tl.sort_values('entry_time').reset_index(drop=True)
    return tl


# ── Parameter sweep ───────────────────────────────────────────────────────────
def candlestick_parameter_sweep(data: pd.DataFrame,
                                instrument_config: dict,
                                stop_atrs=None,
                                target_atrs=None,
                                body_atr_mins=None) -> pd.DataFrame:
    """
    Grid sweep over STOP_ATR × TARGET_ATR × BODY_ATR_MIN.
    Results sorted by total P&L (single lot, no compounding).
    """
    if stop_atrs     is None: stop_atrs     = [0.75, 1.0, 1.25, 1.5]
    if target_atrs   is None: target_atrs   = [2.0, 2.5, 3.0, 3.5]
    if body_atr_mins is None: body_atr_mins = [0.4, 0.5, 0.6]

    total = len(stop_atrs) * len(target_atrs) * len(body_atr_mins)
    print(f"Running Candlestick 15-min sweep ({total} combos)...")

    results = []
    for sa in stop_atrs:
        for ta in target_atrs:
            for bmin in body_atr_mins:
                tl = run_candlestick(
                    data, instrument_config, strategy_params={},
                    params={'stop_atr': sa, 'target_atr': ta,
                            'body_atr_min': bmin}
                )
                if tl.empty or len(tl) < 10:
                    continue
                wins   = tl[tl['pnl_rs'] > 0]['pnl_rs'].sum()
                losses = abs(tl[tl['pnl_rs'] < 0]['pnl_rs'].sum())
                pf     = wins / losses if losses > 0 else float('inf')
                results.append({
                    'stop_atr':     sa,
                    'target_atr':   ta,
                    'body_atr_min': bmin,
                    'trades':       len(tl),
                    'win_rate':     tl['win'].mean() * 100,
                    'total_pl':     tl['pnl_rs'].sum(),
                    'avg_pl':       tl['pnl_rs'].mean(),
                    'pf':           pf,
                })

    if not results:
        print("No valid combinations found.")
        return pd.DataFrame()

    res_df = pd.DataFrame(results).sort_values('total_pl', ascending=False)
    print(f"\n  CANDLESTICK 15-MIN SWEEP — Top 10")
    print(f"  {'StopATR':>7} {'TgtATR':>7} {'BodyMin':>8} "
          f"{'Trades':>7} {'WinRate':>8} {'TotalP&L':>12} {'PF':>6}")
    for _, row in res_df.head(10).iterrows():
        print(f"  {row['stop_atr']:>7.2f} {row['target_atr']:>7.2f} "
              f"{row['body_atr_min']:>8.2f} {row['trades']:>7.0f}  "
              f"{row['win_rate']:>7.1f}%  ₹{row['total_pl']:>10,.0f}  "
              f"{row['pf']:>5.2f}x")
    return res_df
