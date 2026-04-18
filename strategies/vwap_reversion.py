# ============================================================
# strategies/vwap_reversion.py — VWAP Reversion Strategy
# ============================================================
# Intraday mean reversion to VWAP on low/no-gap days.
# Fires ONLY on days where the opening gap < min_gap
# (i.e., days where gap fill and ORB do NOT fire).
#
# Refactored from cell_9_vwap_reversion.py:
#   - Params injected via dicts (no hardcoding)
#   - entry_ts, exit_ts added to output
#   - bias_score: normalised VWAP deviation
#   - Fixed: pandas .ffill() deprecation
#   - Fixed: no global mutation in sweep (params= arg)
# ============================================================

import numpy as np
import pandas as pd
from datetime import time as dtime


def check_volume_availability(data, sample_days=20):
    """
    Check whether Volume data is meaningful (non-zero) in the feed.
    Returns True if real volume is available; False if feed returns zeros.
    """
    if 'Volume' not in data.columns:
        print("  ⚠ No Volume column — using equal-weight VWAP")
        return False
    sample  = data.head(sample_days * 26)
    nonzero = (sample['Volume'] > 0).sum()
    pct     = nonzero / len(sample) * 100
    if pct < 10:
        print(f"  ⚠ Volume is {pct:.0f}% non-zero — falling back to equal-weight VWAP.")
        return False
    print(f"  ✅ Volume available ({pct:.0f}% non-zero) — using true VWAP")
    return True


def compute_vwap(day_df, use_volume=True):
    """
    Compute cumulative intraday VWAP.
    If use_volume=False, falls back to cumulative mean of typical price.
    """
    tp = (day_df['High'] + day_df['Low'] + day_df['Close']) / 3

    if use_volume and 'Volume' in day_df.columns:
        vol     = day_df['Volume'].replace(0, np.nan).ffill().fillna(1)
        cum_tpv = (tp * vol).cumsum()
        cum_vol = vol.cumsum()
        return cum_tpv / cum_vol
    else:
        return tp.expanding().mean()


def run_vwap_reversion(data: pd.DataFrame,
                       instrument_config: dict,
                       strategy_params: dict,
                       regime_df=None,
                       params=None) -> pd.DataFrame:
    """
    VWAP reversion backtest — no-gap days only.

    Args:
        data              : 15-min OHLCV DataFrame
        instrument_config : dict from config.INSTRUMENTS[instrument]
        strategy_params   : dict from config.STRATEGIES['vwap_reversion']['params']
        regime_df         : optional DataFrame with [date, regime]
        params            : optional override dict (used by sweep only)
                            Keys: band_pct, stop_atr, target_atr

    Returns:
        pd.DataFrame: One row per VWAP trade — standard trade log schema
    """
    # ── Unpack instrument config ──────────────────────────────────────────────
    LOT_SIZE    = instrument_config.get('lot_size',  15)
    BROKERAGE   = instrument_config.get('brokerage', 40)
    MIN_GAP     = instrument_config.get('min_gap',   50)

    # ── Resolve strategy params ───────────────────────────────────────────────
    _band_pct   = params.get('band_pct',   strategy_params.get('VWAP_BAND_PCT',   0.005)) if params else strategy_params.get('VWAP_BAND_PCT',   0.005)
    _stop_atr   = params.get('stop_atr',   strategy_params.get('VWAP_STOP_ATR',   0.5))   if params else strategy_params.get('VWAP_STOP_ATR',   0.5)
    _target_atr = params.get('target_atr', strategy_params.get('VWAP_TARGET_ATR', 1.0))   if params else strategy_params.get('VWAP_TARGET_ATR', 1.0)

    MIN_HOUR   = dtime(10, 0)
    MAX_HOUR   = dtime(13, 30)
    SQUAREOFF  = dtime(14, 45)

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

        prev_close   = float(prev_day['Close'].iloc[-1])
        first_candle = day.between_time('09:15', '09:15')
        if first_candle.empty:
            continue
        today_open = float(first_candle['Open'].iloc[0])
        gap_pts    = abs(today_open - prev_close)

        # ── Only trade no-gap days ────────────────────────────────────────────
        if gap_pts >= MIN_GAP:
            continue

        # ── 14-day ATR ────────────────────────────────────────────────────────
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

        # ── Compute full-day VWAP ─────────────────────────────────────────────
        session      = day.between_time('09:15', '15:30')
        vwap         = compute_vwap(session, use_volume=use_volume)

        # ── Scan for reversion setup ──────────────────────────────────────────
        entry     = None
        direction = None
        in_setup  = False
        setup_dir = None
        entry_ts  = None
        dev_pct   = 0.0

        post_open = session.copy()
        post_open['vwap'] = vwap

        for fidx, frow in post_open.iterrows():
            t = fidx.time()
            if t < MIN_HOUR:
                continue
            if t > MAX_HOUR and entry is None:
                break

            c_close = float(frow['Close'])
            c_vwap  = float(frow['vwap'])
            dev_pct = (c_close - c_vwap) / c_vwap

            if entry is None:
                if not in_setup:
                    if dev_pct >= _band_pct:
                        in_setup  = True
                        setup_dir = -1   # expect SHORT reversion
                    elif dev_pct <= -_band_pct:
                        in_setup  = True
                        setup_dir = 1    # expect LONG reversion
                else:
                    if setup_dir == -1 and c_close <= c_vwap:
                        entry     = c_close
                        direction = -1
                        entry_ts  = fidx
                        break
                    elif setup_dir == 1 and c_close >= c_vwap:
                        entry     = c_close
                        direction = 1
                        entry_ts  = fidx
                        break
                    # Reset if price crosses to other extreme
                    if setup_dir == -1 and dev_pct <= -_band_pct:
                        in_setup  = True
                        setup_dir = 1
                    elif setup_dir == 1 and dev_pct >= _band_pct:
                        in_setup  = True
                        setup_dir = -1

        if entry is None:
            continue

        # ── Bias score: VWAP deviation strength ──────────────────────────────
        bias_score = round(min(abs(dev_pct) / (_band_pct * 2), 1.0), 4)

        current_sl = entry - stop_pts   if direction == 1 else entry + stop_pts
        current_tp = entry + target_pts if direction == 1 else entry - target_pts

        # ── Simulate trade ────────────────────────────────────────────────────
        entry_idx_loc = post_open.index.get_loc(fidx)
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
            last_bar    = day.between_time('14:30', '15:00')
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
            'strategy':    'VWAP_REV',
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
            'gap_pts':     round(gap_pts, 2),
            'vwap_dev_pct': round(abs(dev_pct) * 100, 3),
            'stop_pts':    round(stop_pts, 2),
            'target_pts':  round(target_pts, 2),
            'atr14':       round(atr14, 2),
            'regime':      regime,
            'macro_ok':    True,
        })

    return pd.DataFrame(records)


def vwap_parameter_sweep(data: pd.DataFrame,
                         instrument_config: dict) -> pd.DataFrame:
    """
    Sweep VWAP band and R/R parameters. No global mutation.
    """
    bands       = [0.003, 0.004, 0.005, 0.006, 0.008]
    stop_atrs   = [0.4, 0.5, 0.6, 0.75]
    target_atrs = [0.75, 1.0, 1.25, 1.5]

    results = []
    total   = len(bands) * len(stop_atrs) * len(target_atrs)
    print(f"Running VWAP sweep ({total} combos)...")

    for band in bands:
        for sa in stop_atrs:
            for ta in target_atrs:
                vw = run_vwap_reversion(data, instrument_config,
                                        strategy_params={},
                                        params={'band_pct': band,
                                                'stop_atr': sa,
                                                'target_atr': ta})
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
        return pd.DataFrame()

    res_df = pd.DataFrame(results).sort_values('total_pl', ascending=False)
    print(f"\n  VWAP SWEEP — Top 10")
    print(f"  {'Band%':>6} {'StopATR':>8} {'TgtATR':>7} "
          f"{'Trades':>7} {'WinRate':>8} {'TotalP&L':>12}")
    for _, row in res_df.head(10).iterrows():
        print(f"  {row['band_pct']:>5.2f}%  {row['stop_atr']:>8.2f}  "
              f"{row['target_atr']:>7.2f}  {row['trades']:>7.0f}  "
              f"{row['win_rate']:>7.1f}%  ₹{row['total_pl']:>10,.0f}")
    return res_df
