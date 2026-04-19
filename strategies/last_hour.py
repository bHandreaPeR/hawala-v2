# ============================================================
# strategies/last_hour.py — Last Hour Momentum
# ============================================================
# Fires on all days (gap or no-gap) during 14:30–14:45.
# Measures the 13:00–14:30 trend slope and VWAP position
# to trade momentum into the close.
# Hard squareoff at 15:10 — no overnight positions.
# ============================================================

import numpy as np
import pandas as pd
from datetime import time as dtime

from strategies.vwap_reversion import compute_vwap, check_volume_availability


def run_last_hour(data: pd.DataFrame,
                  instrument_config: dict,
                  strategy_params: dict,
                  regime_df=None,
                  params=None) -> pd.DataFrame:
    """
    Last Hour Momentum — all days, entry 14:30–14:45, exit by 15:10.

    Args:
        data              : 15-min OHLCV DataFrame (futures)
        instrument_config : dict from config.INSTRUMENTS[instrument]
        strategy_params   : dict from config.STRATEGIES['last_hour']['params']
        regime_df         : optional DataFrame with [date, regime]
        params            : optional override dict (sweep use)
                            Keys: trend_start, entry_start, entry_end,
                                  slope_min, stop_atr, target_atr, squareoff

    Returns:
        pd.DataFrame: one row per Last Hour trade
    """
    # ── Unpack instrument config ──────────────────────────────────────────────
    LOT_SIZE  = instrument_config.get('lot_size',  15)
    BROKERAGE = instrument_config.get('brokerage', 40)
    MARGIN    = instrument_config.get('margin_per_lot', 75_000)
    SYMBOL    = instrument_config.get('symbol', 'NSE-BANKNIFTY')

    # ── Resolve strategy params ───────────────────────────────────────────────
    sp = strategy_params
    p  = params or {}

    _trend_start  = p.get('trend_start',  sp.get('LH_TREND_START',  '13:00'))
    _entry_start  = p.get('entry_start',  sp.get('LH_ENTRY_START',  '14:30'))
    _entry_end    = p.get('entry_end',    sp.get('LH_ENTRY_END',    '14:45'))
    _slope_min    = p.get('slope_min',    sp.get('LH_SLOPE_MIN',    0.002))
    _stop_atr     = p.get('stop_atr',     sp.get('LH_STOP_ATR',     0.20))
    _target_atr   = p.get('target_atr',   sp.get('LH_TARGET_ATR',   0.30))
    _squareoff    = dtime(*[int(x) for x in sp.get('LH_SQUAREOFF', '15:10').split(':')])

    # ── Regime lookup ─────────────────────────────────────────────────────────
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

        day = data[data.index.date == tdate]
        if day.empty:
            continue

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

        # Guard: skip days with abnormal volatility (stop > 200 pts)
        if stop_pts > 200:
            continue

        regime = regime_lookup.get(tdate, 'neutral')

        # ── Trend measurement: _trend_start → _entry_start ───────────────────
        trend_window = day.between_time(_trend_start, _entry_start)
        if len(trend_window) < 3:
            continue

        trend_open  = float(trend_window['Close'].iloc[0])
        trend_close = float(trend_window['Close'].iloc[-1])
        if trend_open == 0:
            continue
        slope_pct = (trend_close - trend_open) / trend_open

        # Require minimum slope magnitude
        if abs(slope_pct) < _slope_min:
            continue

        # ── VWAP at entry window ───────────────────────────────────────────────
        session = day.between_time('09:15', '15:30')
        vwap    = compute_vwap(session, use_volume=use_volume)
        session_vwap = session.copy()
        session_vwap['vwap'] = vwap

        # Get VWAP value at the entry window start
        entry_window_vwap = session_vwap.between_time(_entry_start, _entry_end)
        if entry_window_vwap.empty:
            continue

        # ── Determine direction ───────────────────────────────────────────────
        # Long: slope up AND close above VWAP
        # Short: slope down AND close below VWAP
        entry_bar = entry_window_vwap.iloc[0]
        bar_close = float(entry_bar['Close'])
        bar_vwap  = float(entry_bar['vwap'])

        if slope_pct >= _slope_min and bar_close > bar_vwap:
            direction = 1
        elif slope_pct <= -_slope_min and bar_close < bar_vwap:
            direction = -1
        else:
            continue

        entry    = bar_close
        entry_ts = entry_window_vwap.index[0]

        current_sl = entry - stop_pts   if direction == 1 else entry + stop_pts
        current_tp = entry + target_pts if direction == 1 else entry - target_pts

        bias_score = round(min(abs(slope_pct) / (_slope_min * 3), 1.0), 4)

        # ── Simulate trade ────────────────────────────────────────────────────
        post_entry = session_vwap.loc[session_vwap.index > entry_ts]

        pnl_pts     = None
        exit_reason = None
        exit_ts     = None
        exit_price  = None

        for eidx, erow in post_entry.iterrows():
            et     = eidx.time()
            e_low  = float(erow['Low'])
            e_high = float(erow['High'])

            if et >= _squareoff:
                ep          = float(erow['Open'])
                pnl_pts     = (ep - entry) * direction
                exit_price  = ep
                exit_reason = 'SQUARE OFF'
                exit_ts     = eidx
                break

            if direction == 1:
                if e_low <= current_sl:
                    pnl_pts     = current_sl - entry
                    exit_price  = current_sl
                    exit_reason = 'STOP LOSS'
                    exit_ts     = eidx
                    break
                if e_high >= current_tp:
                    pnl_pts     = current_tp - entry
                    exit_price  = current_tp
                    exit_reason = 'TARGET HIT'
                    exit_ts     = eidx
                    break
            else:
                if e_high >= current_sl:
                    pnl_pts     = entry - current_sl
                    exit_price  = current_sl
                    exit_reason = 'STOP LOSS'
                    exit_ts     = eidx
                    break
                if e_low <= current_tp:
                    pnl_pts     = entry - current_tp
                    exit_price  = current_tp
                    exit_reason = 'TARGET HIT'
                    exit_ts     = eidx
                    break

        if pnl_pts is None:
            sq_bar     = day.between_time('15:00', '15:15')
            ep         = float(sq_bar['Close'].iloc[-1]) if not sq_bar.empty else entry
            pnl_pts    = (ep - entry) * direction
            exit_price = ep
            exit_reason = 'SQUARE OFF'
            exit_ts    = sq_bar.index[-1] if not sq_bar.empty else entry_ts

        pnl_rs = round(pnl_pts * LOT_SIZE - BROKERAGE, 2)

        records.append({
            'date':         tdate,
            'entry_ts':     entry_ts,
            'exit_ts':      exit_ts,
            'year':         tdate.year,
            'instrument':   SYMBOL,
            'strategy':     'LAST_HOUR',
            'direction':    'LONG' if direction == 1 else 'SHORT',
            'entry':        round(entry, 2),
            'exit_price':   round(exit_price, 2),
            'stop':         round(current_sl, 2),
            'target':       round(current_tp, 2),
            'pnl_pts':      round(pnl_pts, 2),
            'pnl_rs':       pnl_rs,
            'win':          1 if pnl_rs > 0 else 0,
            'exit_reason':  exit_reason,
            'bias_score':   bias_score,
            'lots_used':    LOT_SIZE,
            'capital_used': MARGIN,
            'slope_pct':    round(slope_pct * 100, 4),
            'stop_pts':     round(stop_pts, 2),
            'target_pts':   round(target_pts, 2),
            'atr14':        round(atr14, 2),
            'margin_per_lot': MARGIN,
            'regime':       regime,
            'macro_ok':     True,
        })

    return pd.DataFrame(records)
