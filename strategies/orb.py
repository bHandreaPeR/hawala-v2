# ============================================================
# strategies/orb.py — Opening Range Breakout Strategy
# ============================================================
# Complement to gap fill: fires only on gap days where the
# gap does NOT fill within the ORB window (09:15–09:45).
# Follows the trend direction instead of fading.
#
# Refactored from cell_7_orb_strategy.py:
#   - Params injected via dicts (no hardcoding)
#   - entry_ts, exit_ts added to output
#   - bias_score: normalised breakout distance vs ATR
#   - sweep fix: no global mutation (uses params= arg)
# ============================================================

import numpy as np
import pandas as pd
from datetime import time as dtime


def run_orb(data: pd.DataFrame,
            instrument_config: dict,
            strategy_params: dict,
            regime_df=None,
            allowed_regimes=None,
            params=None) -> pd.DataFrame:
    """
    ORB backtest — candle-by-candle simulation.

    Args:
        data              : 15-min OHLCV DataFrame
        instrument_config : dict from config.INSTRUMENTS[instrument]
        strategy_params   : dict from config.STRATEGIES['orb']['params']
        regime_df         : optional DataFrame with [date, regime] from macro/filters.py
        allowed_regimes   : list of regimes to allow, e.g. ['neutral', 'risk_off']
                            None = all regimes allowed
        params            : optional override dict (used by parameter sweep only)
                            Keys: window_end, stop_pct, target_r, buffer

    Returns:
        pd.DataFrame: One row per ORB trade — standard trade log schema
    """
    # ── Unpack instrument config ──────────────────────────────────────────────
    LOT_SIZE    = instrument_config.get('lot_size',  15)
    BROKERAGE   = instrument_config.get('brokerage', 40)
    MIN_GAP     = instrument_config.get('min_gap',   50)
    MAX_GAP     = instrument_config.get('max_gap',   400)

    # ── Resolve strategy params (sweep overrides via params= arg) ─────────────
    _window_end    = params.get('window_end',    strategy_params.get('ORB_WINDOW_END',      '09:45')) if params else strategy_params.get('ORB_WINDOW_END',      '09:45')
    _stop_pct      = params.get('stop_pct',      strategy_params.get('ORB_STOP_PCT',        0.005))   if params else strategy_params.get('ORB_STOP_PCT',        0.005)
    _target_r      = params.get('target_r',      strategy_params.get('ORB_TARGET_R',        2.0))     if params else strategy_params.get('ORB_TARGET_R',        2.0)
    _buffer        = params.get('buffer',        strategy_params.get('ORB_BREAKOUT_BUFFER', 5))       if params else strategy_params.get('ORB_BREAKOUT_BUFFER', 5)
    # ATR-based stop/target params — when use_atr_stops=True the ORB-range stop is
    # replaced with a volatility-normalised stop (same pattern as VWAP reversion).
    # Fixes the structural problem where ORB-range stops = 200-400 pts on gap days,
    # making targets unreachable intraday and producing negative P&L despite 51% WR.
    _use_atr_stops = params.get('use_atr_stops', strategy_params.get('ORB_USE_ATR_STOPS', True))  if params else strategy_params.get('ORB_USE_ATR_STOPS', True)
    _stop_atr      = params.get('stop_atr',      strategy_params.get('ORB_STOP_ATR',      0.3))   if params else strategy_params.get('ORB_STOP_ATR',      0.3)
    _target_atr    = params.get('target_atr',    strategy_params.get('ORB_TARGET_ATR',    0.6))   if params else strategy_params.get('ORB_TARGET_ATR',    0.6)

    # ── Regime lookup ─────────────────────────────────────────────────────────
    regime_lookup = {}
    if regime_df is not None:
        for _, row in regime_df.iterrows():
            regime_lookup[row['date']] = row.get('regime', 'neutral')

    records = []
    dates   = sorted(set(data.index.date))

    for i, tdate in enumerate(dates):
        if i < 15:
            continue

        day      = data[data.index.date == tdate]
        prev_day = data[data.index.date == dates[i - 1]]
        if day.empty or prev_day.empty:
            continue

        # ── Previous close ────────────────────────────────────────────────────
        prev_close = float(prev_day['Close'].iloc[-1])

        # ── Opening gap ───────────────────────────────────────────────────────
        first_candle = day.between_time('09:15', '09:15')
        if first_candle.empty:
            continue
        today_open = float(first_candle['Open'].iloc[0])
        gap_pts    = today_open - prev_close
        gap_pct    = gap_pts / prev_close * 100

        if abs(gap_pts) < MIN_GAP or abs(gap_pts) > MAX_GAP:
            continue

        # ── Regime filter ─────────────────────────────────────────────────────
        regime = regime_lookup.get(tdate, 'neutral')
        if allowed_regimes is not None and regime not in allowed_regimes:
            continue

        # ── Direction: WITH the gap (trend following) ─────────────────────────
        gap_direction = 1 if gap_pts > 0 else -1   # +1=gap up, -1=gap down

        # ── Build opening range ───────────────────────────────────────────────
        orb_candles = day.between_time('09:15', _window_end)
        if orb_candles.empty:
            continue
        orb_high = float(orb_candles['High'].max())
        orb_low  = float(orb_candles['Low'].min())
        orb_size = orb_high - orb_low

        # ── Check if gap filled during ORB window ─────────────────────────────
        # Requires a candle to CLOSE through prev_close (not just touch it)
        gap_filled_early = False
        for _, candle in orb_candles.iterrows():
            c = float(candle['Close'])
            if gap_direction == 1 and c <= prev_close:
                gap_filled_early = True
                break
            if gap_direction == -1 and c >= prev_close:
                gap_filled_early = True
                break
        if gap_filled_early:
            continue

        # ── 14-day ATR ────────────────────────────────────────────────────────
        recent_ranges = [
            float(data[data.index.date == dates[i - k]]['High'].max()) -
            float(data[data.index.date == dates[i - k]]['Low'].min())
            for k in range(1, 15)
            if not data[data.index.date == dates[i - k]].empty
        ]
        atr14 = np.mean(recent_ranges) if recent_ranges else 300

        # ── Post-ORB breakout scan ────────────────────────────────────────────
        post_orb   = day.between_time('10:00', '15:10')
        entry      = None
        direction  = None
        stop_pts   = None
        target_pts = None
        entry_ts   = None

        for fidx, frow in post_orb.iterrows():
            c_close = float(frow['Close'])

            if gap_direction == 1 and c_close > orb_high + _buffer:
                entry     = c_close
                direction = 1   # LONG
                if _use_atr_stops:
                    # ATR-based: normalised risk regardless of ORB range width
                    stop_pts   = atr14 * _stop_atr
                    target_pts = atr14 * _target_atr
                else:
                    # Legacy ORB-range-based stop
                    stop_loss  = orb_low - (orb_size * _stop_pct)
                    stop_pts   = entry - stop_loss
                    target_pts = stop_pts * _target_r
                entry_ts  = fidx
                break
            elif gap_direction == -1 and c_close < orb_low - _buffer:
                entry     = c_close
                direction = -1  # SHORT
                if _use_atr_stops:
                    stop_pts   = atr14 * _stop_atr
                    target_pts = atr14 * _target_atr
                else:
                    stop_loss  = orb_high + (orb_size * _stop_pct)
                    stop_pts   = stop_loss - entry
                    target_pts = stop_pts * _target_r
                entry_ts  = fidx
                break

        if entry is None:
            continue   # no breakout today

        # ── Bias score: breakout distance relative to ATR ─────────────────────
        breakout_dist = abs(entry - (orb_high if direction == 1 else orb_low))
        bias_score    = round(min(breakout_dist / (atr14 * 0.3), 1.0), 4)

        current_sl = entry - stop_pts  if direction == 1 else entry + stop_pts
        current_tp = entry + target_pts if direction == 1 else entry - target_pts

        # ── Simulate trade ────────────────────────────────────────────────────
        entry_idx  = post_orb.index.get_loc(fidx)
        post_entry = post_orb.iloc[entry_idx + 1:]

        pnl_pts    = None
        exit_reason= None
        exit_ts    = None

        for eidx, erow in post_entry.iterrows():
            if erow.name.time() >= dtime(15, 10):
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
            last_bar    = day.between_time('15:00', '15:30')
            ep          = float(last_bar['Close'].iloc[-1]) if not last_bar.empty else entry
            pnl_pts     = (ep - entry) * direction
            exit_reason = 'SQUARE OFF'
            exit_ts     = last_bar.index[-1] if not last_bar.empty else entry_ts

        pnl_rs = round(pnl_pts * LOT_SIZE - BROKERAGE, 2)

        records.append({
            'date':        tdate,
            'entry_ts':    entry_ts,
            'exit_ts':     exit_ts,
            'year':        tdate.year,
            'instrument':  instrument_config.get('symbol', 'NSE-BANKNIFTY'),
            'strategy':    'ORB',
            'direction':   'LONG' if direction == 1 else 'SHORT',
            'entry':       round(entry, 2),
            'exit_price':  round(entry + pnl_pts * direction, 2),
            'stop':        round(current_sl, 2),
            'target':      round(current_tp, 2),
            'pnl_pts':     round(pnl_pts, 2),
            'pnl_rs':      pnl_rs,
            'win':         1 if pnl_rs > 0 else 0,
            'exit_reason': exit_reason,
            'bias_score':  bias_score,
            'lots_used':   LOT_SIZE,
            'capital_used': instrument_config.get('margin_per_lot', 75_000),
            'gap_pts':     round(abs(gap_pts), 2),
            'gap_pct':     round(abs(gap_pct), 3),
            'gap_vs_atr':  round(abs(gap_pts) / atr14, 3),
            'orb_size':    round(orb_size, 2),
            'stop_pts':    round(stop_pts, 2),
            'target_pts':  round(target_pts, 2),
            'atr14':       round(atr14, 2),
            'regime':      regime,
            'macro_ok':    True,
        })

    return pd.DataFrame(records)


def orb_parameter_sweep(data: pd.DataFrame,
                        instrument_config: dict,
                        regime_df=None,
                        mode: str = 'atr') -> pd.DataFrame:
    """
    Sweep key ORB parameters to find optimal settings.
    Passes params via dict — no global mutation.

    Args:
        mode : 'atr'    → sweep ATR-based stops (new default, fixes structural problem)
               'legacy' → sweep ORB-range-based stops (original broken mode)
               'both'   → run both and combine into one sorted table
    """
    results = []

    if mode in ('atr', 'both'):
        windows    = ['09:30', '09:45', '10:00']
        stop_atrs  = [0.25, 0.3, 0.4, 0.5]
        target_atrs = [0.45, 0.6, 0.75, 1.0]   # target_pts = atr14 × target_atr
        buffers    = [5, 10, 15]
        total      = len(windows) * len(stop_atrs) * len(target_atrs) * len(buffers)
        print(f"Running ORB ATR-stop sweep ({total} combos)...")

        for win in windows:
            for sa in stop_atrs:
                for ta in target_atrs:
                    for buf in buffers:
                        orb = run_orb(data, instrument_config,
                                      strategy_params={},
                                      regime_df=regime_df,
                                      params={'window_end': win,
                                              'use_atr_stops': True,
                                              'stop_atr': sa, 'target_atr': ta,
                                              'buffer': buf})
                        if orb.empty:
                            continue
                        results.append({
                            'mode': 'ATR', 'window': win,
                            'stop_atr': sa, 'target_atr': ta, 'buffer': buf,
                            'stop_pct': None, 'target_r': None,
                            'trades':   len(orb),
                            'win_rate': orb['win'].mean() * 100,
                            'total_pl': orb['pnl_rs'].sum(),
                            'avg_pl':   orb['pnl_rs'].mean(),
                        })

    if mode in ('legacy', 'both'):
        windows   = ['09:30', '09:45', '10:00']
        stop_pcts = [0.003, 0.005, 0.0075]
        target_rs = [1.5, 2.0, 2.5]
        buffers   = [5, 10]
        total     = len(windows) * len(stop_pcts) * len(target_rs) * len(buffers)
        print(f"Running ORB legacy sweep ({total} combos)...")

        for win in windows:
            for sp in stop_pcts:
                for tr in target_rs:
                    for buf in buffers:
                        orb = run_orb(data, instrument_config,
                                      strategy_params={},
                                      regime_df=regime_df,
                                      params={'window_end': win,
                                              'use_atr_stops': False,
                                              'stop_pct': sp, 'target_r': tr,
                                              'buffer': buf})
                        if orb.empty:
                            continue
                        results.append({
                            'mode': 'LEGACY', 'window': win,
                            'stop_atr': None, 'target_atr': None, 'buffer': buf,
                            'stop_pct': sp * 100, 'target_r': tr,
                            'trades':   len(orb),
                            'win_rate': orb['win'].mean() * 100,
                            'total_pl': orb['pnl_rs'].sum(),
                            'avg_pl':   orb['pnl_rs'].mean(),
                        })

    if not results:
        print("No valid combinations found.")
        return pd.DataFrame()

    res_df = pd.DataFrame(results).sort_values('total_pl', ascending=False)

    print(f"\n  ORB SWEEP — Top 15 by Total P&L")
    print(f"  {'Mode':>6} {'Window':>7} {'StopATR':>8} {'TgtATR':>7} "
          f"{'Buf':>4} {'Trades':>7} {'WinRate':>8} {'TotalP&L':>12}")
    print(f"  {'-'*68}")
    for _, row in res_df.head(15).iterrows():
        sa  = f"{row['stop_atr']:.2f}"   if row['stop_atr']  is not None else f"{row['stop_pct']:.2f}%"
        ta  = f"{row['target_atr']:.2f}" if row['target_atr'] is not None else f"{row['target_r']:.1f}R"
        print(f"  {row['mode']:>6} {row['window']:>7}  {sa:>8}  {ta:>7}  "
              f"{row['buffer']:>4.0f}  {row['trades']:>7.0f}  "
              f"{row['win_rate']:>7.1f}%  ₹{row['total_pl']:>10,.0f}")
    return res_df
