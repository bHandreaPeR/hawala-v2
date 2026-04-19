# ============================================================
# strategies/narrow_range_breakout.py — NRB Midday Breakout
# ============================================================
# Fires on no-gap days only (complement to ORB).
# Identifies midday (11:00–12:30) consolidation ranges that
# are tight relative to ATR14, then trades the breakout in
# the direction of the morning session trend.
# ============================================================

import numpy as np
import pandas as pd
from datetime import time as dtime

from strategies.vwap_reversion import compute_vwap, check_volume_availability


def run_narrow_range_breakout(data: pd.DataFrame,
                               instrument_config: dict,
                               strategy_params: dict,
                               regime_df=None,
                               params=None) -> pd.DataFrame:
    """
    NRB Midday Breakout — no-gap days only.

    Args:
        data              : 15-min OHLCV DataFrame (futures)
        instrument_config : dict from config.INSTRUMENTS[instrument]
        strategy_params   : dict from config.STRATEGIES['narrow_range_breakout']['params']
        regime_df         : optional DataFrame with [date, regime]
        params            : optional override dict (sweep use)
                            Keys: window_start, window_end, range_atr,
                                  breakout_buf, target_atr, squareoff

    Returns:
        pd.DataFrame: one row per NRB trade
    """
    # ── Unpack instrument config ──────────────────────────────────────────────
    LOT_SIZE   = instrument_config.get('lot_size',  15)
    BROKERAGE  = instrument_config.get('brokerage', 40)
    MIN_GAP    = instrument_config.get('min_gap',   50)
    MARGIN     = instrument_config.get('margin_per_lot', 75_000)
    SYMBOL     = instrument_config.get('symbol', 'NSE-BANKNIFTY')

    # ── Resolve strategy params ───────────────────────────────────────────────
    sp = strategy_params
    p  = params or {}

    _win_start  = p.get('window_start', sp.get('NRB_WINDOW_START', '11:00'))
    _win_end    = p.get('window_end',   sp.get('NRB_WINDOW_END',   '12:30'))
    _range_atr  = p.get('range_atr',   sp.get('NRB_RANGE_ATR',    0.25))
    _buf        = p.get('breakout_buf', sp.get('NRB_BREAKOUT_BUF', 5))
    _target_atr = p.get('target_atr',  sp.get('NRB_TARGET_ATR',   0.50))
    _squareoff  = dtime(*[int(x) for x in sp.get('NRB_SQUAREOFF', '14:30').split(':')])

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

        day      = data[data.index.date == tdate]
        prev_day = data[data.index.date == dates[i - 1]]
        if day.empty or prev_day.empty:
            continue

        prev_close = float(prev_day['Close'].iloc[-1])
        open_bar   = day.between_time('09:15', '09:15')
        if open_bar.empty:
            continue
        today_open = float(open_bar['Open'].iloc[0])
        gap_pts    = abs(today_open - prev_close)

        # ── No-gap days only ──────────────────────────────────────────────────
        if gap_pts >= MIN_GAP:
            continue

        # ── ATR14 ─────────────────────────────────────────────────────────────
        recent_ranges = [
            float(data[data.index.date == dates[i - k]]['High'].max()) -
            float(data[data.index.date == dates[i - k]]['Low'].min())
            for k in range(1, 15)
            if not data[data.index.date == dates[i - k]].empty
        ]
        atr14      = np.mean(recent_ranges) if recent_ranges else 300
        target_pts = atr14 * _target_atr
        regime     = regime_lookup.get(tdate, 'neutral')

        # ── Consolidation window ──────────────────────────────────────────────
        consol = day.between_time(_win_start, _win_end)
        if len(consol) < 4:
            continue

        range_high = float(consol['High'].max())
        range_low  = float(consol['Low'].min())
        range_pts  = range_high - range_low

        # Skip if range is too wide (not a tight consolidation)
        if range_pts > atr14 * _range_atr:
            continue

        # ── Morning trend bias ────────────────────────────────────────────────
        # Compare 09:15 open vs 12:30 close to determine preferred direction
        close_bar = day.between_time(_win_end, _win_end)
        if close_bar.empty:
            continue
        morning_close = float(close_bar['Close'].iloc[-1])
        bias_dir = 1 if morning_close > today_open else -1

        # ── VWAP at window end (for bias confirmation) ─────────────────────────
        session = day.between_time('09:15', '15:30')
        vwap    = compute_vwap(session, use_volume=use_volume)
        session_vwap = session.copy()
        session_vwap['vwap'] = vwap

        # ── Scan for breakout after consolidation window ──────────────────────
        long_trigger  = range_high + _buf
        short_trigger = range_low  - _buf

        post_consol = day.between_time(
            _win_end, '15:30'
        ).iloc[1:]   # skip the exact window-end bar

        if post_consol.empty:
            continue

        entry     = None
        direction = None
        entry_ts  = None
        stop      = None

        for fidx, frow in post_consol.iterrows():
            t = fidx.time()
            if t >= _squareoff:
                break

            c_high  = float(frow['High'])
            c_low   = float(frow['Low'])
            c_close = float(frow['Close'])

            if direction is None:
                if bias_dir == 1 and c_close >= long_trigger:
                    entry     = c_close
                    direction = 1
                    entry_ts  = fidx
                    stop      = range_low - _buf
                    break
                elif bias_dir == -1 and c_close <= short_trigger:
                    entry     = c_close
                    direction = -1
                    entry_ts  = fidx
                    stop      = range_high + _buf
                    break

        if entry is None:
            continue

        current_sl = stop
        current_tp = entry + target_pts if direction == 1 else entry - target_pts

        stop_pts_actual = abs(entry - current_sl)
        bias_score      = round(min(range_pts / (atr14 * _range_atr), 1.0), 4)

        # ── Simulate trade ────────────────────────────────────────────────────
        entry_idx_loc = post_consol.index.get_loc(entry_ts)
        post_entry    = post_consol.iloc[entry_idx_loc + 1:]

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
            sq_bar     = day.between_time('14:15', '14:30')
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
            'strategy':     'NRB_MIDDAY',
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
            'range_pts':    round(range_pts, 2),
            'stop_pts':     round(stop_pts_actual, 2),
            'target_pts':   round(target_pts, 2),
            'atr14':        round(atr14, 2),
            'margin_per_lot': MARGIN,
            'regime':       regime,
            'macro_ok':     True,
        })

    return pd.DataFrame(records)
