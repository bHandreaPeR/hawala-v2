# ============================================================
# CELL 7 — ORB (Opening Range Breakout) Strategy
# ============================================================
# Complement to the gap fill strategy (Cell 3).
#
# Core logic:
#   On days where the opening gap does NOT fill within the ORB
#   window (09:15–09:45), and price breaks out of the opening range,
#   follow the trend instead of fading it.
#
#   Gap UP  + ORB breakout HIGH  → LONG  (gap is holding, trend up)
#   Gap DOWN + ORB breakout LOW  → SHORT (gap is holding, trend down)
#   No breakout by cutoff time   → No trade
#
# Regime integration (from Cell 5):
#   risk_off days  → ORB SHORT only (trending down days)
#   risk_on  days  → ORB LONG only  (trending up days)
#   neutral  days  → both directions allowed
#
# Relationship with gap fill:
#   Gap fill fires FIRST (Cell 3).
#   ORB fires ONLY on days where:
#     (a) There is a gap (> MIN_GAP_PTS), AND
#     (b) Gap did NOT fill by end of ORB window (09:45), AND
#     (c) Price breaks the ORB range
#   This means gap fill and ORB cannot fire on the same day —
#   they are mutually exclusive by design.
#
# Parameters (to be optimised via sweep):
#   ORB_WINDOW_END : '09:45'   (30-min opening range)
#   ORB_STOP_PCT   : 0.5%      (SL below/above ORB boundary)
#   ORB_TARGET_R   : 2.0       (TP = 2x the risk)
#   MIN_GAP_PTS    : 50        (same as gap fill filter)
# ============================================================

import pandas as pd
import numpy as np
from datetime import time as dtime


# ── Strategy Parameters ───────────────────────────────────────────────────────
ORB_WINDOW_END  = '09:45'   # ORB range is 09:15 to this candle
ORB_STOP_PCT    = 0.005     # SL = 0.5% beyond ORB boundary
ORB_TARGET_R    = 2.0       # TP = 2R (2x the initial risk)
ORB_LOT_SIZE    = 15        # BankNifty lot size
ORB_BROKERAGE   = 40        # ₹ per round trip
ORB_MIN_GAP     = 50        # only consider days with a gap
ORB_MAX_GAP     = 400       # ignore fundamental gap days (same as gap fill)
ORB_BREAKOUT_BUFFER = 5     # pts buffer above/below ORB to avoid false breaks


def run_orb(data, regime_df=None, allowed_regimes=None, params=None):
    """
    ORB backtest — candle by candle simulation.

    For each trading day:
      1. Check opening gap vs previous close
      2. If gap is within range [MIN, MAX], define opening range:
             ORB_high = max(High) of 09:15–09:45 candles
             ORB_low  = min(Low)  of 09:15–09:45 candles
      3. Check if gap starts filling before 09:45:
             If close returns to within 10pts of prev_close → gap filled,
             skip this day (gap fill strategy owns it)
      4. After 09:45, scan for breakout:
             LONG  if close > ORB_high + buffer  (gap up held / trending)
             SHORT if close < ORB_low  - buffer  (gap down held / trending)
      5. Trade with 2R target and % stop loss
      6. Square off at 15:10 if not exited

    Args:
        data           : DataFrame from Cell 2 (15-min OHLCV)
        regime_df      : DataFrame from Cell 5 classify_regime() — optional
                         If provided, filters trades by regime.
        allowed_regimes: list of regimes to trade, e.g. ['neutral', 'risk_off']
                         None = all regimes allowed

    Returns:
        pd.DataFrame: One row per ORB trade with P&L and metadata
    """
    # Resolve params: sweep can pass overrides without touching module globals
    _window_end = params.get('window_end', ORB_WINDOW_END)      if params else ORB_WINDOW_END
    _stop_pct   = params.get('stop_pct',   ORB_STOP_PCT)        if params else ORB_STOP_PCT
    _target_r   = params.get('target_r',   ORB_TARGET_R)        if params else ORB_TARGET_R
    _buffer     = params.get('buffer',     ORB_BREAKOUT_BUFFER) if params else ORB_BREAKOUT_BUFFER

    # Build regime lookup if provided
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

        # ── Previous close ────────────────────────────────────────────────
        prev_close = float(prev_day['Close'].iloc[-1])

        # ── Opening gap ───────────────────────────────────────────────────
        first_candle = day.between_time('09:15', '09:15')
        if first_candle.empty:
            continue
        today_open = float(first_candle['Open'].iloc[0])
        gap_pts    = today_open - prev_close
        gap_pct    = gap_pts / prev_close * 100

        if abs(gap_pts) < ORB_MIN_GAP or abs(gap_pts) > ORB_MAX_GAP:
            continue

        # ── Regime filter ─────────────────────────────────────────────────
        regime = regime_lookup.get(tdate, 'neutral')
        if allowed_regimes is not None and regime not in allowed_regimes:
            continue

        # ── Expected ORB direction: WITH the gap (trend following) ────────
        gap_direction = 1 if gap_pts > 0 else -1   # +1=gap up, -1=gap down

        # ── Build opening range (09:15–09:45) ────────────────────────────
        orb_candles = day.between_time('09:15', _window_end)
        if orb_candles.empty:
            continue
        orb_high = float(orb_candles['High'].max())
        orb_low  = float(orb_candles['Low'].min())
        orb_size = orb_high - orb_low

        # ── Check if gap actually filled during ORB window ───────────────
        # A gap is "filled" only when a candle CLOSES through prev_close.
        # Checking Low/High within 10pts was wrong — any ordinary pullback
        # during the ORB window would trigger it, killing all ORB setups.
        # We now require the candle Close to cross prev_close.
        gap_filled_early = False
        for _, candle in orb_candles.iterrows():
            c = float(candle['Close'])
            if gap_direction == 1 and c <= prev_close:   # gap up filled
                gap_filled_early = True
                break
            if gap_direction == -1 and c >= prev_close:  # gap down filled
                gap_filled_early = True
                break
        if gap_filled_early:
            continue

        # ── 14-day ATR ────────────────────────────────────────────────────
        recent_ranges = [
            float(data[data.index.date == dates[i - k]]['High'].max()) -
            float(data[data.index.date == dates[i - k]]['Low'].min())
            for k in range(1, 15)
            if not data[data.index.date == dates[i - k]].empty
        ]
        atr14 = np.mean(recent_ranges) if recent_ranges else 300

        # ── Post-ORB scan for breakout ────────────────────────────────────
        post_orb   = day.between_time('10:00', '15:10')
        entry      = None
        direction  = None
        stop_pts   = None
        target_pts = None

        for fidx, frow in post_orb.iterrows():
            c_close = float(frow['Close'])
            c_high  = float(frow['High'])
            c_low   = float(frow['Low'])

            # Only trade in the direction of the gap (trend confirmation)
            if gap_direction == 1 and c_close > orb_high + _buffer:
                entry     = c_close
                direction = 1   # LONG
                stop_loss = orb_low - (orb_size * _stop_pct)
                stop_pts  = entry - stop_loss
                target_pts= stop_pts * _target_r
                break
            elif gap_direction == -1 and c_close < orb_low - _buffer:
                entry     = c_close
                direction = -1  # SHORT
                stop_loss = orb_high + (orb_size * _stop_pct)
                stop_pts  = stop_loss - entry
                target_pts= stop_pts * _target_r
                break

        if entry is None:
            continue   # no breakout — no trade today

        current_sl = entry - stop_pts  if direction == 1 else entry + stop_pts
        current_tp = entry + target_pts if direction == 1 else entry - target_pts

        # ── Simulate trade after entry ────────────────────────────────────
        entry_idx  = post_orb.index.get_loc(fidx)
        post_entry = post_orb.iloc[entry_idx + 1:]

        pnl_pts    = None
        exit_reason= None

        for _, erow in post_entry.iterrows():
            if erow.name.time() >= dtime(15, 10):
                ep          = float(erow['Close'])
                pnl_pts     = (ep - entry) * direction
                exit_reason = 'SQUARE OFF'
                break

            e_low  = float(erow['Low'])
            e_high = float(erow['High'])

            if direction == 1:
                if e_low <= current_sl:
                    pnl_pts    = current_sl - entry
                    exit_reason= 'STOP LOSS'
                    break
                if e_high >= current_tp:
                    pnl_pts    = current_tp - entry
                    exit_reason= 'TARGET HIT'
                    break
            else:
                if e_high >= current_sl:
                    pnl_pts    = entry - current_sl
                    exit_reason= 'STOP LOSS'
                    break
                if e_low <= current_tp:
                    pnl_pts    = entry - current_tp
                    exit_reason= 'TARGET HIT'
                    break

        if pnl_pts is None:
            last_bar    = day.between_time('15:00', '15:30')
            ep          = float(last_bar['Close'].iloc[-1]) \
                          if not last_bar.empty else entry
            pnl_pts     = (ep - entry) * direction
            exit_reason = 'SQUARE OFF'

        pnl_rs = round(pnl_pts * ORB_LOT_SIZE - ORB_BROKERAGE, 2)

        records.append({
            'date':        tdate,
            'year':        tdate.year,
            'strategy':    'ORB',
            'direction':   'LONG' if direction == 1 else 'SHORT',
            'regime':      regime,
            'gap_pts':     round(abs(gap_pts), 2),
            'gap_pct':     round(abs(gap_pct), 3),
            'gap_vs_atr':  round(abs(gap_pts) / atr14, 3),
            'orb_size':    round(orb_size, 2),
            'entry':       round(entry, 2),
            'stop_pts':    round(stop_pts, 2),
            'target_pts':  round(target_pts, 2),
            'atr14':       round(atr14, 2),
            'exit_reason': exit_reason,
            'pnl_pts':     round(pnl_pts, 2),
            'pnl_rs':      pnl_rs,
            'win':         1 if pnl_rs > 0 else 0,
        })

    return pd.DataFrame(records)


def orb_parameter_sweep(data, regime_df=None):
    """
    Sweep key ORB parameters to find optimal settings.

    Tests combinations of:
        ORB window end  : 09:30, 09:45, 10:00
        Stop pct        : 0.3%, 0.5%, 0.75%
        Target R        : 1.5, 2.0, 2.5, 3.0
        Breakout buffer : 0, 5, 10 pts

    Prints top 10 combos by total P&L.
    """
    windows   = ['09:30', '09:45', '10:00']
    stop_pcts = [0.003, 0.005, 0.0075]
    target_rs = [1.5, 2.0, 2.5, 3.0]
    buffers   = [0, 5, 10]

    results = []
    total   = len(windows) * len(stop_pcts) * len(target_rs) * len(buffers)
    done    = 0

    print(f"Running ORB parameter sweep ({total} combos)...")

    for win in windows:
        for sp in stop_pcts:
            for tr in target_rs:
                for buf in buffers:
                    orb = run_orb(data, regime_df, params={
                        'window_end': win,
                        'stop_pct':   sp,
                        'target_r':   tr,
                        'buffer':     buf,
                    })
                    done += 1
                    if orb.empty:
                        continue
                    results.append({
                        'window': win, 'stop_pct': sp*100,
                        'target_r': tr, 'buffer': buf,
                        'trades':   len(orb),
                        'win_rate': orb['win'].mean() * 100,
                        'total_pl': orb['pnl_rs'].sum(),
                        'avg_pl':   orb['pnl_rs'].mean(),
                    })

    if not results:
        print("No valid combinations found.")
        return

    res_df = pd.DataFrame(results).sort_values('total_pl', ascending=False)
    print(f"\n{'='*75}")
    print(f"  ORB PARAMETER SWEEP — Top 15 by Total P&L")
    print(f"{'='*75}")
    print(f"  {'Window':>7} {'Stop%':>6} {'TgtR':>5} {'Buf':>4} "
          f"{'Trades':>7} {'WinRate':>8} {'TotalP&L':>12} {'Avg/Trade':>10}")
    print(f"  {'-'*66}")
    for _, row in res_df.head(15).iterrows():
        print(f"  {row['window']:>7}  {row['stop_pct']:>5.2f}%  "
              f"{row['target_r']:>4.1f}  {row['buffer']:>4.0f}  "
              f"{row['trades']:>7.0f}  {row['win_rate']:>7.1f}%  "
              f"₹{row['total_pl']:>10,.0f}  ₹{row['avg_pl']:>8,.0f}")

    best = res_df.iloc[0]
    print(f"\n  ★ Best: window={best['window']}  stop={best['stop_pct']:.2f}%  "
          f"R={best['target_r']:.1f}  buf={best['buffer']:.0f}pts")
    print(f"    → {best['trades']:.0f} trades | "
          f"Win {best['win_rate']:.1f}% | ₹{best['total_pl']:,.0f}")
    return res_df


def run_orb_report(orb_df):
    """
    Print full ORB strategy report — year by year, regime breakdown, exit reasons.
    """
    if orb_df.empty:
        print("No ORB trades found.")
        return

    print(f"\n{'='*55}")
    print(f"  ORB STRATEGY RESULTS")
    print(f"{'='*55}")
    print(f"  Total trades : {len(orb_df)}")
    print(f"  Win rate     : {orb_df['win'].mean()*100:.1f}%")
    print(f"  Total P&L    : ₹{orb_df['pnl_rs'].sum():,.0f}")
    print(f"  Avg P&L/trade: ₹{orb_df['pnl_rs'].mean():,.0f}")

    print(f"\n  Year-by-year:")
    print(f"  {'Year':<6} {'Trades':>7} {'WinRate':>8} {'TotalP&L':>12} {'Avg/Trade':>10}")
    print(f"  {'-'*46}")
    for yr in sorted(orb_df['year'].unique()):
        y  = orb_df[orb_df['year'] == yr]
        wr = y['win'].mean() * 100
        pl = y['pnl_rs'].sum()
        ap = y['pnl_rs'].mean()
        print(f"  {yr:<6} {len(y):>7}  {wr:>7.1f}%  ₹{pl:>10,.0f}  ₹{ap:>8,.0f}")

    print(f"\n  By direction:")
    for d in ['LONG', 'SHORT']:
        sub = orb_df[orb_df['direction'] == d]
        if sub.empty:
            continue
        print(f"    {d:5} : {len(sub):3d} trades | "
              f"Win {sub['win'].mean()*100:.1f}% | ₹{sub['pnl_rs'].sum():,.0f}")

    print(f"\n  Exit breakdown:")
    print(orb_df['exit_reason'].value_counts().to_string())

    if 'regime' in orb_df.columns:
        print(f"\n  By regime:")
        for reg in ['neutral', 'risk_on', 'risk_off']:
            sub = orb_df[orb_df['regime'] == reg]
            if sub.empty:
                continue
            print(f"    {reg:<10}: {len(sub):3d} trades | "
                  f"Win {sub['win'].mean()*100:.1f}% | ₹{sub['pnl_rs'].sum():,.0f}")


# ── Main execution ────────────────────────────────────────────────────────────
print("Running ORB strategy...\n")

# Use regime_df if available from Cell 5 (classify_regime output)
_regime_input = regime_df if 'regime_df' in dir() else None

# Run with default parameters first
orb_df = run_orb(data, regime_df=_regime_input)

print(f"\n✅ ORB found {len(orb_df)} trades")
run_orb_report(orb_df)

# Optional: run parameter sweep (takes ~2-3 min)
# orb_sweep_results = orb_parameter_sweep(data, _regime_input)

print("\n✅ Cell 7 complete.")
print("   orb_df available — feed into Cell 8 (combined regime framework)")
