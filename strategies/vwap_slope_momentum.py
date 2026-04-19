# ============================================================
# strategies/vwap_slope_momentum.py — VWAP Slope Momentum
# ============================================================
# Trend-following on no-gap days where the morning session
# shows a clear directional slope.
#
# Philosophy (inverse of VWAP_REV):
#   VWAP_REV fades deviations on flat mornings (mean reversion)
#   VWAP_SLOPE rides pullbacks to VWAP on trending mornings
#
# The two strategies are mutually exclusive:
#   Trending morning (slope > VWAP_SLOPE_MIN_PCT) → VWAP_SLOPE
#   Flat morning    (slope ≤ VWAP_SLOPE_MIN_PCT)  → VWAP_REV
#
# Entry: when price pulls back to within VWAP ± entry_band,
#        enter in the direction of the morning slope.
# Stop:  ATR × stop_atr below entry
# Target: ATR × target_atr above entry (default 2:1 R:R)
# ============================================================

import numpy as np
import pandas as pd
from datetime import time as dtime
from strategies.vwap_reversion import compute_vwap, check_volume_availability


def run_vwap_slope_momentum(data: pd.DataFrame,
                             instrument_config: dict,
                             strategy_params: dict,
                             regime_df=None,
                             params=None) -> pd.DataFrame:
    """
    VWAP Slope Momentum — trend-following pullback to VWAP on no-gap days.

    Args:
        data              : 15-min OHLCV DataFrame
        instrument_config : dict from config.INSTRUMENTS[instrument]
        strategy_params   : dict from config.STRATEGIES['vwap_slope']['params']
        regime_df         : optional DataFrame with [date, regime]
        params            : optional override dict (sweep use)
    """
    LOT_SIZE  = instrument_config.get('lot_size',  15)
    BROKERAGE = instrument_config.get('brokerage', 40)
    MIN_GAP   = instrument_config.get('min_gap',   50)

    sp = strategy_params
    p  = params or {}

    _slope_min    = p.get('slope_min',    sp.get('VWAP_SLOPE_MIN_PCT',   0.003))   # min morning slope to trigger
    _entry_band   = p.get('entry_band',   sp.get('VWAP_SLOPE_ENTRY_BAND', 0.0015)) # how close to VWAP to enter
    _stop_atr     = p.get('stop_atr',     sp.get('VWAP_SLOPE_STOP_ATR',  0.35))
    _target_atr   = p.get('target_atr',   sp.get('VWAP_SLOPE_TARGET_ATR', 0.70))

    MIN_HOUR  = dtime(10, 0)
    MAX_HOUR  = dtime(13, 30)
    SQUAREOFF = dtime(14, 30)

    regime_lookup = {}
    if regime_df is not None:
        for _, row in regime_df.iterrows():
            regime_lookup[row['date']] = row.get('regime', 'neutral')

    use_volume = check_volume_availability(data)

    records = []
    dates   = sorted(set(data.index.date))

    for i, tdate in enumerate(dates):
        if i < 15:
            continue

        day      = data[data.index.date == tdate]
        prev_day = data[data.index.date == dates[i - 1]]
        if day.empty or prev_day.empty:
            continue

        prev_close   = float(prev_day['Close'].iloc[-1])
        first_candle = day.between_time('09:15', '09:15')
        if first_candle.empty:
            continue
        today_open = float(first_candle['Open'].iloc[0])
        gap_pts    = abs(today_open - prev_close)

        # Only trade no-gap days (same condition as VWAP_REV)
        if gap_pts >= MIN_GAP:
            continue

        # ── Morning slope: 09:15-09:45 (first 3 candles) ─────────────────────
        morning = day.between_time('09:15', '09:45')
        if len(morning) < 2:
            continue

        morning_open  = float(morning['Open'].iloc[0])
        morning_close = float(morning['Close'].iloc[-1])
        if morning_open <= 0:
            continue

        slope_pct = (morning_close - morning_open) / morning_open

        # Only fire if morning has a clear directional slope
        if abs(slope_pct) < _slope_min:
            continue

        slope_dir = 1 if slope_pct > 0 else -1   # 1=uptrend, -1=downtrend

        # ── ATR14 ─────────────────────────────────────────────────────────────
        recent_ranges = [
            float(data[data.index.date == dates[i - k]]['High'].max()) -
            float(data[data.index.date == dates[i - k]]['Low'].min())
            for k in range(1, 15)
            if not data[data.index.date == dates[i - k]].empty
        ]
        atr14      = np.mean(recent_ranges) if recent_ranges else 300
        stop_pts   = atr14 * _stop_atr
        target_pts = atr14 * _target_atr
        regime     = regime_lookup.get(tdate, 'neutral')

        # ── Full-day VWAP ─────────────────────────────────────────────────────
        session = day.between_time('09:15', '15:30')
        vwap    = compute_vwap(session, use_volume=use_volume)

        post_open = session.copy()
        post_open['vwap'] = vwap

        # ── Scan for pullback entry ───────────────────────────────────────────
        # In an uptrend: wait for price to dip to within entry_band below VWAP
        # In a downtrend: wait for price to rise to within entry_band above VWAP
        entry     = None
        entry_ts  = None
        dev_pct   = 0.0

        for fidx, frow in post_open.iterrows():
            t = fidx.time()
            if t < MIN_HOUR:
                continue
            if t > MAX_HOUR and entry is None:
                break

            c_close = float(frow['Close'])
            c_vwap  = float(frow['vwap'])
            dev     = (c_close - c_vwap) / c_vwap   # positive = above VWAP

            if slope_dir == 1:
                # Uptrend: enter when price dips to within entry_band of VWAP
                # i.e. dev is between -entry_band and +entry_band/2
                if -_entry_band <= dev <= _entry_band * 0.5:
                    entry    = c_close
                    entry_ts = fidx
                    dev_pct  = dev
                    break
            else:
                # Downtrend: enter when price rises to within entry_band of VWAP
                if -_entry_band * 0.5 <= dev <= _entry_band:
                    entry    = c_close
                    entry_ts = fidx
                    dev_pct  = dev
                    break

        if entry is None:
            continue

        direction = slope_dir
        current_sl = entry - stop_pts   if direction == 1 else entry + stop_pts
        current_tp = entry + target_pts if direction == 1 else entry - target_pts

        bias_score = round(min(abs(slope_pct) / (_slope_min * 3), 1.0), 4)

        # ── Simulate trade ────────────────────────────────────────────────────
        entry_idx_loc = post_open.index.get_loc(entry_ts)
        post_entry    = post_open.iloc[entry_idx_loc + 1:]

        pnl_pts     = None
        exit_reason = None
        exit_ts     = None

        for eidx, erow in post_entry.iterrows():
            et = eidx.time()

            if et >= SQUAREOFF:
                ep          = float(erow['Close'])
                pnl_pts     = (ep - entry) * direction
                exit_reason = 'SQUARE OFF'
                exit_ts     = eidx
                break

            e_low  = float(erow['Low'])
            e_high = float(erow['High'])

            if direction == 1:
                if e_low <= current_sl:
                    pnl_pts     = current_sl - entry
                    exit_reason = 'STOP LOSS'
                    exit_ts     = eidx
                    break
                if e_high >= current_tp:
                    pnl_pts     = current_tp - entry
                    exit_reason = 'TARGET HIT'
                    exit_ts     = eidx
                    break
            else:
                if e_high >= current_sl:
                    pnl_pts     = entry - current_sl
                    exit_reason = 'STOP LOSS'
                    exit_ts     = eidx
                    break
                if e_low <= current_tp:
                    pnl_pts     = entry - current_tp
                    exit_reason = 'TARGET HIT'
                    exit_ts     = eidx
                    break

        if pnl_pts is None:
            last_bar    = day.between_time('14:15', '14:45')
            ep          = float(last_bar['Close'].iloc[-1]) if not last_bar.empty else entry
            pnl_pts     = (ep - entry) * direction
            exit_reason = 'SQUARE OFF'
            exit_ts     = last_bar.index[-1] if not last_bar.empty else entry_ts

        pnl_rs = round(pnl_pts * LOT_SIZE - BROKERAGE, 2)

        records.append({
            'date':         tdate,
            'entry_ts':     entry_ts,
            'exit_ts':      exit_ts,
            'year':         tdate.year,
            'instrument':   instrument_config.get('symbol', 'NSE-BANKNIFTY'),
            'strategy':     'VWAP_SLOPE',
            'direction':    'LONG' if direction == 1 else 'SHORT',
            'entry':        round(entry, 2),
            'exit_price':   round(entry + pnl_pts * direction, 2),
            'stop':         round(current_sl, 2),
            'target':       round(current_tp, 2),
            'pnl_pts':      round(pnl_pts, 2),
            'pnl_rs':       pnl_rs,
            'win':          1 if pnl_rs > 0 else 0,
            'exit_reason':  exit_reason,
            'bias_score':   bias_score,
            'lots_used':    LOT_SIZE,
            'capital_used': instrument_config.get('margin_per_lot', 75_000),
            'gap_pts':      round(gap_pts, 2),
            'vwap_dev_pct': round(abs(dev_pct) * 100, 3),
            'slope_pct':    round(slope_pct * 100, 3),
            'stop_pts':     round(stop_pts, 2),
            'target_pts':   round(target_pts, 2),
            'atr14':        round(atr14, 2),
            'regime':       regime,
            'macro_ok':     True,
        })

    return pd.DataFrame(records)
