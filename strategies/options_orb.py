# ============================================================
# strategies/options_orb.py — Options ORB (Large Gap Days)
# ============================================================
# Fires on gap days where gap_pts > OPTIONS_GAP_MIN (default 100 pts).
# Uses the same ORB breakout signal but buys ATM CE/PE instead
# of trading futures.
#
# Capital efficiency at ₹1L:
#   Premium ~₹300-600 × 15 contracts = ₹4,500-9,000 per lot
#   At 10% of equity = ₹10,000 → 1 lot fits comfortably
#   Defined risk: max loss = premium paid (no margin call)
#   At 2× target: 48% WR → EV = 0.48×1 - 0.52×0.5 = +0.22 per ₹ risked
#
# Requires groww API handle for live option candle fetching.
# In backtest mode (groww=None), falls back to ATR-based proxy P&L.
# ============================================================

import time
import numpy as np
import pandas as pd
from datetime import time as dtime

from data.options_fetch import get_nearest_expiry, fetch_option_candles, lookup_option_price


def run_options_orb(data: pd.DataFrame,
                    instrument_config: dict,
                    strategy_params: dict,
                    groww=None,
                    regime_df=None,
                    params=None) -> pd.DataFrame:
    """
    Options ORB — large gap days, ATM CE/PE buy at breakout.

    Args:
        data              : 15-min OHLCV futures DataFrame
        instrument_config : dict from config.INSTRUMENTS[instrument]
        strategy_params   : dict from config.STRATEGIES['options_orb']['params']
        groww             : GrowwAPI instance (required for live option prices)
                            If None, falls back to ATR-proxy P&L (backtest mode)
        regime_df         : optional DataFrame with [date, regime]
        params            : optional override dict (sweep use)

    Returns:
        pd.DataFrame: one row per Options ORB trade
    """
    # ── Unpack instrument config ──────────────────────────────────────────────
    LOT_SIZE        = instrument_config.get('lot_size',  15)
    BROKERAGE       = instrument_config.get('brokerage', 40)
    MIN_GAP         = instrument_config.get('min_gap',   50)
    MAX_GAP         = instrument_config.get('max_gap',   400)
    STRIKE_INTERVAL = instrument_config.get('strike_interval', 100)
    SYMBOL          = instrument_config.get('symbol', 'NSE-BANKNIFTY')
    UNDERLYING      = instrument_config.get('underlying_symbol', 'BANKNIFTY')

    # ── Resolve strategy params ───────────────────────────────────────────────
    sp = strategy_params
    p  = params or {}

    _window_end    = p.get('window_end',      sp.get('ORB_WINDOW_END',      '09:30'))
    _buffer        = p.get('buffer',          sp.get('ORB_BREAKOUT_BUFFER', 5))
    _gap_min       = p.get('gap_min',         sp.get('OPTIONS_GAP_MIN',     100))
    _dow_allow     = p.get('dow_allow',       sp.get('OPTIONS_DOW_ALLOW',   None))
    _risk_pct      = p.get('risk_pct',        sp.get('OPTIONS_RISK_PCT',    0.10))
    _target_mult   = p.get('target_mult',     sp.get('OPTIONS_TARGET_MULT', 2.0))
    _stop_mult     = p.get('stop_mult',       sp.get('OPTIONS_STOP_MULT',   0.50))
    _squareoff     = dtime(*[int(x) for x in sp.get('OPTIONS_SQUAREOFF', '12:00').split(':')])
    _max_dte       = p.get('max_dte',         sp.get('OPTIONS_MAX_DTE',     None))  # None = no cap

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

        # ── Day-of-week filter ────────────────────────────────────────────────
        if _dow_allow is not None and tdate.weekday() not in _dow_allow:
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
        gap_pts    = today_open - prev_close

        # ── Large gap filter — options zone only ──────────────────────────────
        if abs(gap_pts) < _gap_min or abs(gap_pts) > MAX_GAP:
            continue

        gap_direction = 1 if gap_pts > 0 else -1
        opt_type      = 'CE' if gap_direction == 1 else 'PE'

        # ── ATR14 ─────────────────────────────────────────────────────────────
        recent_ranges = [
            float(data[data.index.date == dates[i - k]]['High'].max()) -
            float(data[data.index.date == dates[i - k]]['Low'].min())
            for k in range(1, 15)
            if not data[data.index.date == dates[i - k]].empty
        ]
        atr14  = np.mean(recent_ranges) if recent_ranges else 300
        regime = regime_lookup.get(tdate, 'neutral')

        # ── ORB range ─────────────────────────────────────────────────────────
        orb_candles = day.between_time('09:15', _window_end)
        if orb_candles.empty:
            continue
        orb_high = float(orb_candles['High'].max())
        orb_low  = float(orb_candles['Low'].min())

        # Check if gap fills during ORB window
        gap_filled_early = False
        for _, candle in orb_candles.iterrows():
            c = float(candle['Close'])
            if gap_direction == 1 and c <= prev_close:
                gap_filled_early = True; break
            if gap_direction == -1 and c >= prev_close:
                gap_filled_early = True; break
        if gap_filled_early:
            continue

        # ── Post-ORB breakout scan ────────────────────────────────────────────
        post_orb  = day.between_time('10:00', '15:10')
        entry_fut = None
        entry_ts  = None

        for fidx, frow in post_orb.iterrows():
            c_close = float(frow['Close'])
            if gap_direction == 1 and c_close > orb_high + _buffer:
                entry_fut = c_close
                entry_ts  = fidx
                break
            elif gap_direction == -1 and c_close < orb_low - _buffer:
                entry_fut = c_close
                entry_ts  = fidx
                break

        if entry_fut is None or entry_ts.time() >= _squareoff:
            continue

        # ── ATM strike ───────────────────────────────────────────────────────
        atm_strike = int(round(entry_fut / STRIKE_INTERVAL) * STRIKE_INTERVAL)

        # ── Fetch option candle (real premium) ────────────────────────────────
        entry_premium = None
        exit_premium  = None
        expiry_date   = None

        if groww is not None:
            try:
                expiry_date = get_nearest_expiry(
                    groww, UNDERLYING, tdate, min_days=0
                )
                if expiry_date is None:
                    expiry_date = get_nearest_expiry(groww, UNDERLYING, tdate, min_days=1)

                # DTE filter: skip if expiry is too far out (far-DTE = expensive premium,
                # low delta — 2× target becomes unreachable on BANKNIFTY monthly options)
                if expiry_date is not None and _max_dte is not None:
                    dte = (pd.Timestamp(expiry_date).date() - tdate).days
                    if dte > _max_dte:
                        continue  # too far from expiry — skip, don't trade options today

                if expiry_date is not None:
                    tdate_str = str(tdate)
                    opt_df = fetch_option_candles(
                        groww, UNDERLYING, expiry_date,
                        atm_strike, opt_type,
                        tdate_str, tdate_str
                    )
                    if not opt_df.empty:
                        entry_premium = lookup_option_price(opt_df, entry_ts, field='Open')
            except Exception:
                pass  # fall through to ATR proxy

        # ── ATR-proxy P&L when no real premium available ──────────────────────
        # Proxy: assume ATM premium ≈ 0.15 × ATR14 (empirical rule of thumb for
        # near-expiry ATM options on BANKNIFTY at typical IV ~20-25%)
        if entry_premium is None or entry_premium <= 0:
            entry_premium = atr14 * 0.15
            expiry_date   = expiry_date or tdate

        target_premium = entry_premium * _target_mult
        stop_premium   = entry_premium * _stop_mult

        # ── Simulate options trade ────────────────────────────────────────────
        entry_idx_loc = post_orb.index.get_loc(entry_ts)
        post_entry    = post_orb.iloc[entry_idx_loc + 1:]

        # Fetch option candles for exit simulation if available
        opt_df_exit = pd.DataFrame()
        if groww is not None and expiry_date is not None:
            try:
                opt_df_exit = fetch_option_candles(
                    groww, UNDERLYING, expiry_date,
                    atm_strike, opt_type, str(tdate), str(tdate)
                )
            except Exception:
                pass

        pnl_pts     = None
        exit_reason = None
        exit_ts     = None
        exit_premium_final = None

        for eidx, erow in post_entry.iterrows():
            et = eidx.time()

            if et >= _squareoff:
                if not opt_df_exit.empty:
                    ep = lookup_option_price(opt_df_exit, eidx, field='Open')
                    exit_premium_final = ep if ep else entry_premium * 0.9
                else:
                    minutes_held = (eidx - entry_ts).seconds / 60
                    decay = max(0.5, 1.0 - (minutes_held / 150) * 0.3)
                    exit_premium_final = entry_premium * decay
                pnl_pts     = exit_premium_final - entry_premium
                exit_reason = 'SQUARE OFF'
                exit_ts     = eidx
                break

            # ── Use real option candle High/Low when available ────────────────
            # This correctly captures intrabar target/stop hits.
            # For a long (buying CE/PE): High could hit target, Low could hit stop.
            # Check target first — if both breach in same candle, price reached
            # target before reversing (continuous price assumption).
            if not opt_df_exit.empty:
                opt_bar = lookup_option_price(opt_df_exit, eidx, field=None)
                if opt_bar is not None:
                    bar_high = float(opt_bar.get('High', 0))
                    bar_low  = float(opt_bar.get('Low',  float('inf')))

                    if bar_high >= target_premium:
                        exit_premium_final = target_premium
                        pnl_pts     = target_premium - entry_premium
                        exit_reason = 'TARGET HIT'
                        exit_ts     = eidx
                        break
                    if bar_low <= stop_premium:
                        exit_premium_final = stop_premium
                        pnl_pts     = stop_premium - entry_premium
                        exit_reason = 'STOP LOSS'
                        exit_ts     = eidx
                        break
                    continue  # real data checked — move to next candle

            # ── Fallback: delta proxy on futures Close ────────────────────────
            fut_move       = (float(erow['Close']) - entry_fut) * gap_direction
            estimated_prem = entry_premium + fut_move * 0.5

            if estimated_prem <= stop_premium:
                exit_premium_final = stop_premium
                pnl_pts     = stop_premium - entry_premium
                exit_reason = 'STOP LOSS'
                exit_ts     = eidx
                break
            if estimated_prem >= target_premium:
                exit_premium_final = target_premium
                pnl_pts     = target_premium - entry_premium
                exit_reason = 'TARGET HIT'
                exit_ts     = eidx
                break

        if pnl_pts is None:
            exit_premium_final = entry_premium * 0.8
            pnl_pts     = exit_premium_final - entry_premium
            exit_reason = 'SQUARE OFF'
            exit_ts     = entry_ts

        # ── Sizing: risk_pct of equity (uses equity=STARTING_CAP placeholder) ─
        # Actual sizing done by compounding engine using margin_per_lot
        margin_per_lot_opt = entry_premium * LOT_SIZE  # actual capital at risk

        pnl_rs_per_lot = round(pnl_pts * LOT_SIZE, 2)
        bias_score     = round(min(abs(gap_pts) / (MAX_GAP - _gap_min), 1.0), 4)

        records.append({
            'date':           tdate,
            'entry_ts':       entry_ts,
            'exit_ts':        exit_ts,
            'year':           tdate.year,
            'instrument':     SYMBOL,
            'strategy':       'OPT_ORB',
            'direction':      'LONG' if gap_direction == 1 else 'SHORT',
            'entry':          round(entry_fut, 2),
            'exit_price':     round(entry_fut + (exit_premium_final - entry_premium) / 0.5, 2),
            'stop':           round(entry_fut - atr14 * 0.40 * gap_direction, 2),
            'target':         round(entry_fut + atr14 * 0.45 * gap_direction, 2),
            'pnl_pts':        round(pnl_pts, 4),
            'pnl_rs':         round(pnl_rs_per_lot - BROKERAGE, 2),
            'win':            1 if pnl_rs_per_lot > BROKERAGE else 0,
            'exit_reason':    exit_reason,
            'bias_score':     bias_score,
            'lots_used':      1,
            'capital_used':   margin_per_lot_opt,
            'gap_pts':        round(gap_pts, 2),
            'atr14':          round(atr14, 2),
            'atm_strike':     atm_strike,
            'opt_type':       opt_type,
            'expiry':         str(expiry_date) if expiry_date else '',
            'dte':            (pd.Timestamp(expiry_date).date() - tdate).days if expiry_date else None,
            'premium_entry':  round(entry_premium, 2),
            'premium_exit':   round(exit_premium_final, 2),
            'margin_per_lot':     margin_per_lot_opt,
            'per_trade_risk_pct': _risk_pct,  # caps compounding engine at 10% of equity
            'regime':             regime,
            'macro_ok':           True,
        })

    return pd.DataFrame(records)
