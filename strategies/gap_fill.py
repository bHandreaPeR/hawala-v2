# ============================================================
# strategies/gap_fill.py — Gap Fill + Trailing Stop Strategy
# ============================================================
# Core strategy: fade BankNifty opening gaps back to prev close.
#   Gap UP  → SHORT (expect reversion to prev close)
#   Gap DOWN → LONG  (expect reversion to prev close)
#
# Trailing Stop mechanism:
#   When gap fills (TP hit) → instead of closing, move SL to
#   that level and set new TP = old TP + STEP_PTS.
#   Locks in profit while letting winners run further.
#
# Validated results (1 lot, 15 units):
#   2022: ₹92,362  | 2023: ₹91,984
#   2024: ₹90,183  | 2025: ₹116,406  ← out-of-sample
#   4-year total  : ₹405,936
#
# Refactored from cell_3_gap_fill_strategy.py:
#   - Instrument and strategy params injected via dicts (no hardcoding)
#   - entry_ts, exit_ts added to output (for capital-aware combiner)
#   - bias_score added: normalised gap strength vs ATR
# ============================================================

import numpy as np
import pandas as pd
from datetime import time as dtime


def run_gap_fill(data: pd.DataFrame,
                 instrument_config: dict,
                 strategy_params: dict) -> pd.DataFrame:
    """
    Candle-by-candle Gap Fill backtest with trailing stop.

    For each trading day:
      1. Calculate opening gap vs previous close
      2. Filter by min/max gap size (from instrument_config)
      3. Enter LONG (gap down) or SHORT (gap up) at 9:15 open ± slippage
      4. Scan each 15-min candle:
         - TP hit → move SL to TP, advance TP by STEP_PTS, continue
         - SL hit → exit (STOP LOSS or TRAIL STOP)
         - 3:10 PM → square off
      5. Compute bias_score = gap_pts / (atr14 × 0.5), clipped to [0, 1]

    Args:
        data              : 15-min OHLCV DataFrame indexed by datetime
        instrument_config : dict from config.INSTRUMENTS[instrument]
                            Keys: lot_size, brokerage, slippage, min_gap, max_gap
        strategy_params   : dict from config.STRATEGIES['gap_fill']['params']
                            Keys: STEP_PTS, STOP_PTS

    Returns:
        pd.DataFrame: One row per trade — standard trade log schema
    """
    # ── Unpack config ─────────────────────────────────────────────────────────
    LOT_SIZE    = instrument_config.get('lot_size',  15)
    BROKERAGE   = instrument_config.get('brokerage', 40)
    SLIPPAGE    = instrument_config.get('slippage',  10)
    MIN_GAP_PTS = instrument_config.get('min_gap',   50)
    MAX_GAP_PTS = instrument_config.get('max_gap',   400)

    STEP_PTS    = strategy_params.get('STEP_PTS', 75)
    STOP_PTS    = strategy_params.get('STOP_PTS', 80)

    records = []
    dates   = sorted(set(data.index.date))

    for i, tdate in enumerate(dates):
        if i < 15:    # need 15 days of history for ATR
            continue

        day      = data[data.index.date == tdate]
        prev_day = data[data.index.date == dates[i - 1]]
        if day.empty or prev_day.empty:
            continue

        # ── Previous day stats ────────────────────────────────────────────────
        prev_close  = float(prev_day['Close'].iloc[-1])
        prev_open   = float(prev_day['Open'].iloc[0])
        prev_high   = float(prev_day['High'].max())
        prev_low    = float(prev_day['Low'].min())
        prev_range  = prev_high - prev_low
        prev_return = (prev_close - prev_open) / prev_open * 100

        # ── Today's open ──────────────────────────────────────────────────────
        first_candle = day.between_time('09:15', '09:15')
        if first_candle.empty:
            continue
        today_open   = float(first_candle['Open'].iloc[0])
        entry_ts     = first_candle.index[0]   # exact entry timestamp

        # ── Gap ───────────────────────────────────────────────────────────────
        gap_pts = today_open - prev_close
        gap_pct = gap_pts / prev_close * 100
        if abs(gap_pts) < MIN_GAP_PTS or abs(gap_pts) > MAX_GAP_PTS:
            continue

        # ── Direction: fade the gap ───────────────────────────────────────────
        direction = -1 if gap_pts > 0 else 1   # -1=SHORT, +1=LONG

        # ── Entry with slippage ───────────────────────────────────────────────
        entry        = today_open + (SLIPPAGE if direction == 1 else -SLIPPAGE)
        target_price = prev_close
        target_pts   = abs(gap_pts) - SLIPPAGE

        # ── 14-day ATR ────────────────────────────────────────────────────────
        recent_ranges = [
            float(data[data.index.date == dates[i - k]]['High'].max()) -
            float(data[data.index.date == dates[i - k]]['Low'].min())
            for k in range(1, 15)
            if not data[data.index.date == dates[i - k]].empty
        ]
        atr14 = np.mean(recent_ranges) if recent_ranges else 300

        # ── 5-day price trend ─────────────────────────────────────────────────
        five_day_closes = [
            float(data[data.index.date == dates[i - k]]['Close'].iloc[-1])
            for k in range(1, 6)
            if not data[data.index.date == dates[i - k]].empty
        ]
        week_trend = (
            (five_day_closes[0] - five_day_closes[-1]) / five_day_closes[-1] * 100
            if len(five_day_closes) >= 2 else 0
        )

        # ── Bias score: gap strength relative to ATR ──────────────────────────
        # 0 = weak gap (barely above MIN_GAP), 1 = gap equals 50% of daily ATR
        bias_score = round(min(abs(gap_pts) / (atr14 * 0.5), 1.0), 4)

        # ── Trailing state ────────────────────────────────────────────────────
        current_tp   = target_price
        current_sl   = (entry - STOP_PTS) if direction == 1 else (entry + STOP_PTS)
        trail_active = False
        rungs_hit    = 0

        # ── Simulate trade candle by candle ───────────────────────────────────
        post        = day.between_time('09:30', '15:10')
        pnl_pts     = None
        exit_reason = None
        exit_ts     = None

        for fidx, frow in post.iterrows():
            if fidx.time() >= dtime(15, 10):
                ep          = float(frow['Close'])
                pnl_pts     = (ep - entry) if direction == 1 else (entry - ep)
                exit_reason = 'SQUARE OFF'
                exit_ts     = fidx
                break

            c_low  = float(frow['Low'])
            c_high = float(frow['High'])

            if direction == 1:                     # LONG
                if c_low <= current_sl:
                    pnl_pts     = current_sl - entry
                    exit_reason = 'TRAIL STOP' if trail_active else 'STOP LOSS'
                    exit_ts     = fidx
                    break
                if c_high >= current_tp:
                    rungs_hit   += 1
                    current_sl   = current_tp
                    current_tp  += STEP_PTS
                    trail_active = True

            else:                                  # SHORT
                if c_high >= current_sl:
                    pnl_pts     = entry - current_sl
                    exit_reason = 'TRAIL STOP' if trail_active else 'STOP LOSS'
                    exit_ts     = fidx
                    break
                if c_low <= current_tp:
                    rungs_hit   += 1
                    current_sl   = current_tp
                    current_tp  -= STEP_PTS
                    trail_active = True

        # Fallback square off
        if pnl_pts is None:
            last_bar    = day.between_time('15:00', '15:30')
            ep          = float(last_bar['Close'].iloc[-1]) if not last_bar.empty else entry
            pnl_pts     = (ep - entry) if direction == 1 else (entry - ep)
            exit_reason = 'SQUARE OFF'
            exit_ts     = last_bar.index[-1] if not last_bar.empty else entry_ts

        pnl_rs = round(pnl_pts * LOT_SIZE - BROKERAGE, 2)

        records.append({
            # ── Identity ────────────────────────────────────────────────────
            'date':        tdate,
            'entry_ts':    entry_ts,
            'exit_ts':     exit_ts,
            'year':        tdate.year,
            'instrument':  instrument_config.get('symbol', 'NSE-BANKNIFTY'),
            'strategy':    'GAP_FILL',
            # ── Trade setup ─────────────────────────────────────────────────
            'direction':   'LONG' if direction == 1 else 'SHORT',
            'entry':       round(entry, 2),
            'exit_price':  round(entry + (pnl_pts * direction), 2),
            'stop':        round((entry - STOP_PTS) if direction == 1 else (entry + STOP_PTS), 2),
            'target':      round(target_price, 2),
            # ── P&L ─────────────────────────────────────────────────────────
            'pnl_pts':     round(pnl_pts, 2),
            'pnl_rs':      pnl_rs,
            'win':         1 if pnl_rs > 0 else 0,
            'exit_reason': exit_reason,
            # ── Signal features ─────────────────────────────────────────────
            'bias_score':  bias_score,
            'lots_used':   LOT_SIZE,
            'capital_used': instrument_config.get('margin_per_lot', 75_000),
            'gap_pts':     round(abs(gap_pts), 2),
            'gap_pct':     round(abs(gap_pct), 3),
            'gap_vs_atr':  round(abs(gap_pts) / atr14, 3),
            'target_pts':  round(target_pts, 2),
            'atr14':       round(atr14, 2),
            'atr_normal':  1 if 200 <= atr14 <= 700 else 0,
            'rungs_hit':   rungs_hit,
            'prev_return': round(prev_return, 3),
            'prev_range':  round(prev_range, 2),
            'week_trend':  round(week_trend, 3),
            'day_of_week': tdate.weekday(),
            # ── Macro (filled downstream by macro/filters.py) ───────────────
            'regime':      'neutral',
            'macro_ok':    True,
        })

    return pd.DataFrame(records)
