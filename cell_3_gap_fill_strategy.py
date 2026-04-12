# ============================================================
# CELL 3 — Gap Fill + Trailing Stop Strategy
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
# ============================================================

# ── Strategy Parameters ───────────────────────────────────────────────────
SLIPPAGE    = 10     # pts — entry/exit slippage
STOP_PTS    = 80     # pts — initial hard stop loss
LOT_SIZE    = 15     # BankNifty lot size (post Nov 2023)
BROKERAGE   = 40     # ₹ per round trip
MIN_GAP_PTS = 50     # ignore tiny gaps (not worth trading)
MAX_GAP_PTS = 400    # ignore huge gaps (fundamental, won't fill)
STEP_PTS    = 75     # trailing ladder step size (optimised)


def run_gap_fill(data):
    """
    Candle-by-candle Gap Fill backtest with trailing stop.

    For each trading day:
      1. Calculate opening gap vs previous close
      2. Filter by MIN/MAX gap size
      3. Enter LONG (gap down) or SHORT (gap up) at 9:15 open
      4. Check each 15-min candle for:
         - TP hit → move SL to TP, advance TP by STEP_PTS, continue
         - SL hit → exit (STOP LOSS or TRAIL STOP)
         - 3:10 PM → square off at close

    Returns:
        pd.DataFrame: One row per trade with all features and P&L
    """
    records = []
    dates   = sorted(set(data.index.date))

    for i, tdate in enumerate(dates):
        if i < 15:    # need 15 days of history for ATR
            continue

        day      = data[data.index.date == tdate]
        prev_day = data[data.index.date == dates[i - 1]]
        if day.empty or prev_day.empty:
            continue

        # ── Previous day stats ────────────────────────────────────────────
        prev_close  = float(prev_day['Close'].iloc[-1])
        prev_open   = float(prev_day['Open'].iloc[0])
        prev_high   = float(prev_day['High'].max())
        prev_low    = float(prev_day['Low'].min())
        prev_range  = prev_high - prev_low
        prev_return = (prev_close - prev_open) / prev_open * 100

        # ── Today's open ──────────────────────────────────────────────────
        first_candle = day.between_time('09:15', '09:15')
        if first_candle.empty:
            continue
        today_open = float(first_candle['Open'].iloc[0])

        # ── Gap ───────────────────────────────────────────────────────────
        gap_pts = today_open - prev_close
        gap_pct = gap_pts / prev_close * 100
        if abs(gap_pts) < MIN_GAP_PTS or abs(gap_pts) > MAX_GAP_PTS:
            continue

        # ── Direction: fade the gap ───────────────────────────────────────
        direction = -1 if gap_pts > 0 else 1   # -1=SHORT, +1=LONG

        # ── Entry with slippage ───────────────────────────────────────────
        entry        = today_open + (SLIPPAGE if direction == 1 else -SLIPPAGE)
        target_price = prev_close
        target_pts   = abs(gap_pts) - SLIPPAGE

        # ── 14-day ATR ────────────────────────────────────────────────────
        recent_ranges = [
            float(data[data.index.date == dates[i - k]]['High'].max()) -
            float(data[data.index.date == dates[i - k]]['Low'].min())
            for k in range(1, 15)
            if not data[data.index.date == dates[i - k]].empty
        ]
        atr14 = np.mean(recent_ranges) if recent_ranges else 300

        # ── 5-day price trend ─────────────────────────────────────────────
        five_day_closes = [
            float(data[data.index.date == dates[i - k]]['Close'].iloc[-1])
            for k in range(1, 6)
            if not data[data.index.date == dates[i - k]].empty
        ]
        week_trend = (five_day_closes[0] - five_day_closes[-1]) / \
                     five_day_closes[-1] * 100 \
                     if len(five_day_closes) >= 2 else 0

        # ── Trailing state ────────────────────────────────────────────────
        current_tp   = target_price
        current_sl   = (entry - STOP_PTS) if direction == 1 else (entry + STOP_PTS)
        trail_active = False
        rungs_hit    = 0

        # ── Simulate trade candle by candle ───────────────────────────────
        post        = day.between_time('09:30', '15:10')
        pnl_pts     = None
        exit_reason = None

        for fidx, frow in post.iterrows():
            if fidx.time() >= dtime(15, 10):
                ep          = float(frow['Close'])
                pnl_pts     = (ep - entry) if direction == 1 else (entry - ep)
                exit_reason = 'SQUARE OFF'
                break

            c_low  = float(frow['Low'])
            c_high = float(frow['High'])

            if direction == 1:                     # LONG
                if c_low <= current_sl:
                    pnl_pts     = current_sl - entry
                    exit_reason = 'TRAIL STOP' if trail_active else 'STOP LOSS'
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
                    break
                if c_low <= current_tp:
                    rungs_hit   += 1
                    current_sl   = current_tp
                    current_tp  -= STEP_PTS
                    trail_active = True

        # Fallback square off if candle loop ended without break
        if pnl_pts is None:
            last_bar    = day.between_time('15:00', '15:30')
            ep          = float(last_bar['Close'].iloc[-1]) \
                          if not last_bar.empty else entry
            pnl_pts     = (ep - entry) if direction == 1 else (entry - ep)
            exit_reason = 'SQUARE OFF'

        pnl_rs = round(pnl_pts * LOT_SIZE - BROKERAGE, 2)

        records.append({
            'date':        tdate,
            'year':        tdate.year,
            'direction':   'LONG' if direction == 1 else 'SHORT',
            'gap_pts':     round(abs(gap_pts), 2),
            'gap_pct':     round(abs(gap_pct), 3),
            'gap_vs_atr':  round(abs(gap_pts) / atr14, 3),
            'target_pts':  round(target_pts, 2),
            'prev_return': round(prev_return, 3),
            'prev_range':  round(prev_range, 2),
            'week_trend':  round(week_trend, 3),
            'day_of_week': tdate.weekday(),
            'atr14':       round(atr14, 2),
            'atr_normal':  1 if 200 <= atr14 <= 700 else 0,
            'rungs_hit':   rungs_hit,
            'exit_reason': exit_reason,
            'pnl_pts':     round(pnl_pts, 2),
            'pnl_rs':      pnl_rs,
            'win':         1 if pnl_rs > 0 else 0,
        })

    return pd.DataFrame(records)


# ── Run backtest ──────────────────────────────────────────────────────────
print("Running Gap Fill + Trailing Stop backtest...\n")
gap_df     = run_gap_fill(data)
results_df = gap_df

print(f"✅ {len(gap_df)} gap days found\n")
for yr in [2022, 2023, 2024]:
    y  = gap_df[gap_df['year'] == yr]
    wr = y['win'].mean() * 100
    pl = y['pnl_rs'].sum()
    print(f"  {yr}: {len(y):3d} trades | Win: {wr:.1f}% | P&L: ₹{pl:>10,.0f}")

total_wr = gap_df['win'].mean() * 100
total_pl = gap_df['pnl_rs'].sum()
be_wr    = STOP_PTS / (STOP_PTS + gap_df['target_pts'].mean()) * 100

print(f"\n  Total : {len(gap_df)} trades | Win: {total_wr:.1f}% | "
      f"P&L: ₹{total_pl:>10,.0f}")
print(f"  Avg P&L/trade       : ₹{gap_df['pnl_rs'].mean():,.0f}")
print(f"  Avg breakeven needed: {be_wr:.1f}%")
print(f"\n  Exit breakdown:")
print(gap_df['exit_reason'].value_counts().to_string())
print(f"\n  Trailing rungs:")
for r in range(5):
    n = (gap_df['rungs_hit'] == r).sum()
    print(f"    {r} rungs: {n:3d} trades")
n5 = (gap_df['rungs_hit'] >= 5).sum()
if n5 > 0:
    print(f"    5+ rungs: {n5:3d} trades")
