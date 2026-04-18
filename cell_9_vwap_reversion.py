# ============================================================
# CELL 9 — VWAP Reversion Strategy (BankNifty + NIFTY)
# ============================================================
# Targets intraday mean reversion to VWAP on low/no-gap days.
# Fires ONLY on days where the opening gap is below MIN_GAP_PTS
# (i.e., days where gap fill and ORB do NOT fire).
#
# Strategy logic:
#   1. Calculate cumulative VWAP from 09:15 onwards each day
#   2. Wait for price to deviate from VWAP by >= BAND_PCT %
#   3. Entry: when price CROSSES BACK through VWAP (reversion confirmed)
#      NOT on the first touch — wait for confirmation candle closing
#      through VWAP
#   4. Stop loss: ATR-based (0.5x ATR14) beyond entry
#   5. Target: VWAP ± VWAP_TARGET_PCT of ATR
#   6. Square off at 14:45 (give 30 mins to exit cleanly before close)
#
# Why VWAP reversion vs other intraday strategies:
#   - VWAP is computed fresh each day — no lookahead bias
#   - Works on ranging/low-volatility days where gap fill doesn't fire
#   - Directionally neutral — fades both upside and downside
#   - Has strong institutional basis (VWAP is a primary algo benchmark)
#
# NIFTY note:
#   NIFTY is less volatile than BankNifty (lower daily ATR, tighter ranges)
#   making VWAP reversion potentially cleaner with less whipsaw.
#   Same logic, different data feed and lot size.
#
# Parameters:
#   VWAP_BAND_PCT    : 0.5% deviation from VWAP before considering entry
#   VWAP_STOP_ATR    : 0.5 × ATR14 as stop loss
#   VWAP_TARGET_ATR  : 1.0 × ATR14 as target (1:2 R/R)
#   VWAP_MIN_HOUR    : Earliest entry time (10:00 — avoid open noise)
#   VWAP_MAX_HOUR    : Latest entry time  (13:30 — need time to play out)
#   VWAP_SQUAREOFF   : '14:45'
# ============================================================

import pandas as pd
import numpy as np
from datetime import time as dtime


# ── Parameters ────────────────────────────────────────────────────────────────
VWAP_BAND_PCT   = 0.005     # 0.5% deviation from VWAP triggers watch
VWAP_STOP_ATR   = 0.5       # SL = 0.5x ATR14 beyond entry
VWAP_TARGET_ATR = 1.0       # TP = 1.0x ATR14 from entry
VWAP_MIN_HOUR   = dtime(10, 0)
VWAP_MAX_HOUR   = dtime(13, 30)
VWAP_SQUAREOFF  = dtime(14, 45)
VWAP_LOT_BN     = 15        # BankNifty lot size
VWAP_LOT_NF     = 25        # NIFTY lot size
VWAP_BROKERAGE  = 40        # ₹ per round trip
GAP_FILL_MIN    = 50        # no-gap days defined as gap < this


def check_volume_availability(data, sample_days=20):
    """
    Check whether Volume data is meaningful (non-zero) in the feed.
    BankNifty / NIFTY index feeds from some APIs return zero Volume.
    If Volume is all zeros, true VWAP cannot be computed — we fall back
    to Equal-Weight VWAP (cumulative mean of typical price).

    Returns:
        bool: True if real volume data is available
    """
    if 'Volume' not in data.columns:
        print("  ⚠ No Volume column — using equal-weight VWAP")
        return False
    sample = data.head(sample_days * 26)  # ~26 candles/day
    nonzero = (sample['Volume'] > 0).sum()
    pct = nonzero / len(sample) * 100
    if pct < 10:
        print(f"  ⚠ Volume is {pct:.0f}% non-zero — data likely zero-filled.")
        print(f"    True VWAP not computable. Using equal-weight VWAP.")
        print(f"    Note: equal-weight VWAP ≈ cumulative avg of typical price.")
        print(f"    Results will be directionally valid but less precise.")
        return False
    print(f"  ✅ Volume available ({pct:.0f}% non-zero) — using true VWAP")
    return True


def compute_vwap(day_df, use_volume=True):
    """
    Compute cumulative intraday VWAP.

    If use_volume=True  → VWAP = cumsum(TP × Vol) / cumsum(Vol)   [true VWAP]
    If use_volume=False → VWAP = cumulative mean of typical price  [fallback]

    The fallback is a valid proxy on datasets where volume = 0 (index feeds).
    It is NOT the same as VWAP and will produce different levels, but the
    mean-reversion logic (fade extreme deviations, enter on cross) still holds.

    Returns:
        pd.Series: VWAP (or equal-weight proxy) at each candle timestamp
    """
    tp = (day_df['High'] + day_df['Low'] + day_df['Close']) / 3

    if use_volume and 'Volume' in day_df.columns:
        vol = day_df['Volume'].replace(0, np.nan).ffill().fillna(1)
        cum_tpv = (tp * vol).cumsum()
        cum_vol = vol.cumsum()
        return cum_tpv / cum_vol
    else:
        # Equal-weight: cumulative mean of typical price
        return tp.expanding().mean()


def run_vwap_reversion(data, instrument='BANKNIFTY', regime_df=None, params=None):
    """
    VWAP reversion backtest — no-gap days only.

    Args:
        data       : 15-min OHLCV DataFrame (BankNifty or NIFTY)
        instrument : 'BANKNIFTY' or 'NIFTY' (affects lot size)
        regime_df  : optional DataFrame with regime column per date

    Returns:
        pd.DataFrame: One row per VWAP trade
    """
    # Resolve params: sweep can pass overrides without touching module globals
    _band_pct   = params.get('band_pct',   VWAP_BAND_PCT)   if params else VWAP_BAND_PCT
    _stop_atr   = params.get('stop_atr',   VWAP_STOP_ATR)   if params else VWAP_STOP_ATR
    _target_atr = params.get('target_atr', VWAP_TARGET_ATR) if params else VWAP_TARGET_ATR

    lot_size      = VWAP_LOT_BN if instrument == 'BANKNIFTY' else VWAP_LOT_NF
    regime_lookup = {}
    if regime_df is not None:
        for _, row in regime_df.iterrows():
            regime_lookup[row['date']] = row.get('regime', 'neutral')

    # Check volume once upfront — don't re-check every day
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
        first_candle = day.between_time('09:15', '09:15')
        if first_candle.empty:
            continue
        today_open = float(first_candle['Open'].iloc[0])
        gap_pts    = abs(today_open - prev_close)

        # ── Only trade no-gap days ────────────────────────────────────────
        if gap_pts >= GAP_FILL_MIN:
            continue

        # ── 14-day ATR ────────────────────────────────────────────────────
        recent_ranges = [
            float(data[data.index.date == dates[i - k]]['High'].max()) -
            float(data[data.index.date == dates[i - k]]['Low'].min())
            for k in range(1, 15)
            if not data[data.index.date == dates[i - k]].empty
        ]
        atr14 = np.mean(recent_ranges) if recent_ranges else 300
        stop_pts   = atr14 * _stop_atr
        target_pts = atr14 * _target_atr

        regime = regime_lookup.get(tdate, 'neutral')

        # ── Compute full-day VWAP (or equal-weight proxy) ────────────────
        session = day.between_time('09:15', '15:30')
        vwap    = compute_vwap(session, use_volume=use_volume)

        # ── Scan for reversion setup ──────────────────────────────────────
        entry       = None
        direction   = None
        in_setup    = False   # True when price has deviated enough
        setup_dir   = None    # expected reversion direction once in setup

        post_open = session.copy()
        post_open['vwap'] = vwap

        for fidx, frow in post_open.iterrows():
            t = fidx.time()
            if t < VWAP_MIN_HOUR:
                continue
            if t > VWAP_MAX_HOUR and entry is None:
                break   # past entry window, no trade today

            c_close  = float(frow['Close'])
            c_vwap   = float(frow['vwap'])
            dev_pct  = (c_close - c_vwap) / c_vwap

            if entry is None:
                # Phase 1: wait for deviation
                if not in_setup:
                    if dev_pct >= _band_pct:      # price well above VWAP
                        in_setup  = True
                        setup_dir = -1   # expect reversion SHORT
                    elif dev_pct <= -_band_pct:   # price well below VWAP
                        in_setup  = True
                        setup_dir = 1    # expect reversion LONG

                # Phase 2: wait for price to cross back through VWAP
                else:
                    if setup_dir == -1 and c_close <= c_vwap:
                        entry     = c_close
                        direction = -1   # SHORT reversion
                        break
                    elif setup_dir == 1 and c_close >= c_vwap:
                        entry     = c_close
                        direction = 1    # LONG reversion
                        break
                    # Reset if price deviates further in opposite direction
                    if setup_dir == -1 and dev_pct <= -_band_pct:
                        in_setup  = True
                        setup_dir = 1
                    elif setup_dir == 1 and dev_pct >= _band_pct:
                        in_setup  = True
                        setup_dir = -1

        if entry is None:
            continue   # no valid setup found today

        current_sl = entry - stop_pts   if direction == 1 else entry + stop_pts
        current_tp = entry + target_pts if direction == 1 else entry - target_pts

        # ── Simulate trade from entry candle onwards ───────────────────────
        entry_idx_loc = post_open.index.get_loc(fidx)
        post_entry    = post_open.iloc[entry_idx_loc + 1:]

        pnl_pts    = None
        exit_reason= None

        for eidx, erow in post_entry.iterrows():
            et = eidx.time()

            if et >= VWAP_SQUAREOFF:
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
            last_bar    = day.between_time('14:30', '15:00')
            ep          = float(last_bar['Close'].iloc[-1]) \
                          if not last_bar.empty else entry
            pnl_pts     = (ep - entry) * direction
            exit_reason = 'SQUARE OFF'

        pnl_rs = round(pnl_pts * lot_size - VWAP_BROKERAGE, 2)

        records.append({
            'date':        tdate,
            'year':        tdate.year,
            'strategy':    'VWAP_REV',
            'instrument':  instrument,
            'direction':   'LONG' if direction == 1 else 'SHORT',
            'regime':      regime,
            'gap_pts':     round(gap_pts, 2),
            'entry':       round(entry, 2),
            'vwap_dev_pct':round(abs(dev_pct) * 100, 3),
            'stop_pts':    round(stop_pts, 2),
            'target_pts':  round(target_pts, 2),
            'atr14':       round(atr14, 2),
            'exit_reason': exit_reason,
            'pnl_pts':     round(pnl_pts, 2),
            'pnl_rs':      pnl_rs,
            'win':         1 if pnl_rs > 0 else 0,
        })

    return pd.DataFrame(records)


def vwap_parameter_sweep(data, instrument='BANKNIFTY'):
    """
    Sweep VWAP band threshold and R/R to find optimal parameters.
    """
    bands      = [0.003, 0.004, 0.005, 0.006, 0.008]
    stop_atrs  = [0.4, 0.5, 0.6, 0.75]
    target_atrs= [0.75, 1.0, 1.25, 1.5]

    results = []
    total   = len(bands) * len(stop_atrs) * len(target_atrs)
    print(f"Running VWAP sweep ({total} combos)...")

    for band in bands:
        for sa in stop_atrs:
            for ta in target_atrs:
                vw = run_vwap_reversion(data, instrument, params={
                    'band_pct':   band,
                    'stop_atr':   sa,
                    'target_atr': ta,
                })
                if vw.empty or len(vw) < 20:
                    continue
                results.append({
                    'band_pct': band * 100, 'stop_atr': sa, 'target_atr': ta,
                    'trades':   len(vw),
                    'win_rate': vw['win'].mean() * 100,
                    'total_pl': vw['pnl_rs'].sum(),
                    'avg_pl':   vw['pnl_rs'].mean(),
                })

    if not results:
        print("No valid combinations.")
        return

    res_df = pd.DataFrame(results).sort_values('total_pl', ascending=False)
    print(f"\n{'='*65}")
    print(f"  VWAP SWEEP — Top 10  [{instrument}]")
    print(f"{'='*65}")
    print(f"  {'Band%':>6} {'StopATR':>8} {'TgtATR':>7} "
          f"{'Trades':>7} {'WinRate':>8} {'TotalP&L':>12}")
    print(f"  {'-'*54}")
    for _, row in res_df.head(10).iterrows():
        print(f"  {row['band_pct']:>5.2f}%  {row['stop_atr']:>8.2f}  "
              f"{row['target_atr']:>7.2f}  {row['trades']:>7.0f}  "
              f"{row['win_rate']:>7.1f}%  ₹{row['total_pl']:>10,.0f}")
    best = res_df.iloc[0]
    print(f"\n  ★ Best: band={best['band_pct']:.2f}%  "
          f"stop={best['stop_atr']:.2f}x ATR  target={best['target_atr']:.2f}x ATR")
    return res_df


def run_vwap_report(vwap_df):
    """
    Full report for VWAP reversion strategy.
    """
    if vwap_df.empty:
        print("No VWAP trades found.")
        return

    instr = vwap_df['instrument'].iloc[0] if 'instrument' in vwap_df.columns else 'BN'
    print(f"\n{'='*55}")
    print(f"  VWAP REVERSION — {instr}")
    print(f"{'='*55}")
    print(f"  Total trades : {len(vwap_df)}")
    print(f"  Win rate     : {vwap_df['win'].mean()*100:.1f}%")
    print(f"  Total P&L    : ₹{vwap_df['pnl_rs'].sum():,.0f}")
    print(f"  Avg P&L/trade: ₹{vwap_df['pnl_rs'].mean():,.0f}")

    print(f"\n  Year-by-year:")
    for yr in sorted(vwap_df['year'].unique()):
        y  = vwap_df[vwap_df['year'] == yr]
        wr = y['win'].mean() * 100
        pl = y['pnl_rs'].sum()
        ap = y['pnl_rs'].mean()
        print(f"    {yr}: {len(y):3d} trades | Win {wr:.1f}% | "
              f"₹{pl:>10,.0f} | Avg ₹{ap:>6,.0f}")

    print(f"\n  Exit breakdown:")
    print(vwap_df['exit_reason'].value_counts().to_string())

    if 'regime' in vwap_df.columns:
        print(f"\n  By regime:")
        for reg in ['neutral', 'risk_on', 'risk_off']:
            sub = vwap_df[vwap_df['regime'] == reg]
            if sub.empty:
                continue
            print(f"    {reg:<10}: {len(sub):3d} trades | "
                  f"Win {sub['win'].mean()*100:.1f}% | ₹{sub['pnl_rs'].sum():,.0f}")


# ════════════════════════════════════════════════════════════
# NIFTY SECTION
# ════════════════════════════════════════════════════════════

def fetch_nifty(start_date, end_date):
    """
    Fetch NIFTY 50 15-min data via Groww API.
    Uses same chunking pattern as fetch_banknifty() in Cell 2.

    Returns:
        pd.DataFrame: 15-min OHLCV indexed by datetime
    """
    groww_symbol    = "NSE-NIFTY"
    segment         = groww.SEGMENT_CASH
    candle_interval = groww.CANDLE_INTERVAL_MIN_15
    chunk_days      = 88

    start  = datetime.strptime(start_date, "%Y-%m-%d")
    end    = datetime.strptime(end_date,   "%Y-%m-%d")
    frames = []
    cursor = start

    print(f"Fetching NIFTY 15-min data {start_date} → {end_date}...")
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=chunk_days), end)
        try:
            result = groww.get_historical_candles(
                exchange        = "NSE",
                segment         = segment,
                groww_symbol    = groww_symbol,
                start_time      = cursor.strftime("%Y-%m-%d"),
                end_time        = chunk_end.strftime("%Y-%m-%d"),
                candle_interval = candle_interval
            )
            candles = result.get('candles', result.get('data', [])) \
                      if isinstance(result, dict) else result
            if candles:
                frames.append(pd.DataFrame(candles))
        except Exception as e:
            print(f"  Warning: {cursor.date()} → {chunk_end.date()}: {e}")
        cursor = chunk_end + timedelta(days=1)
        time.sleep(0.3)

    if not frames:
        print("❌ No NIFTY data fetched")
        return pd.DataFrame()

    raw = pd.concat(frames, ignore_index=True)
    raw.columns = [c.capitalize() for c in raw.columns]
    time_col = [c for c in raw.columns
                if c.lower() in ('timestamp', 'datetime', 'time', 'date')][0]
    raw.index = pd.to_datetime(raw[time_col], unit='ms', errors='coerce') \
                .fillna(pd.to_datetime(raw[time_col], errors='coerce'))
    raw.index = raw.index.tz_localize(None)
    raw = raw.sort_index().between_time('09:00', '15:30')
    raw = raw[~raw.index.duplicated(keep='first')]
    print(f"✅ NIFTY: {len(raw):,} candles | "
          f"{raw.index[0].date()} → {raw.index[-1].date()}")
    return raw


# ── Main execution ────────────────────────────────────────────────────────────
print("Running VWAP Reversion strategy...\n")

_regime_input = regime_df if 'regime_df' in dir() else None

# BankNifty VWAP
print("── BankNifty VWAP Reversion ──")
vwap_df = run_vwap_reversion(data, instrument='BANKNIFTY',
                              regime_df=_regime_input)
run_vwap_report(vwap_df)

# NIFTY VWAP — requires data_nifty to be fetched first
# Uncomment after fetching NIFTY data:
#
# print("\n── NIFTY VWAP Reversion ──")
# data_nifty = fetch_nifty("2022-01-01", "2025-12-31")
# vwap_nifty_df = run_vwap_reversion(data_nifty, instrument='NIFTY',
#                                     regime_df=_regime_input)
# run_vwap_report(vwap_nifty_df)

# Optional parameter sweep (takes ~1 min):
# vwap_sweep_results = vwap_parameter_sweep(data, 'BANKNIFTY')

print("\n✅ Cell 9 complete.")
print("   vwap_df available (BankNifty no-gap days)")
print("   Uncomment NIFTY block after fetching data_nifty")
print("   Feed vwap_df into Cell 8 (combined framework)")
