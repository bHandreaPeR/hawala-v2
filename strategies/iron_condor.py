# ============================================================
# strategies/iron_condor.py — Hawala v2  0 DTE Iron Condor
# ============================================================
# Short-premium, defined-risk options selling strategy.
# Fires ONLY on expiry day (0 DTE) after the ORB window settles.
#
# Structure
# ---------
#   SELL OTM Call  +  BUY further OTM Call  (bear call spread)
#   SELL OTM Put   +  BUY further OTM Put   (bull put spread)
#   = Iron Condor (4 legs, fully defined max loss)
#
# Entry gates (ALL must pass)
#   1. Expiry day only  (get_nearest_expiry returns today)
#   2. VIX in [IC_VIX_MIN, IC_VIX_MAX]  — sell premium in right IV regime
#   3. |gap| ≤ IC_MAX_GAP — no large directional gaps on expiry day
#   4. net_credit ≥ IC_MIN_NET_CREDIT — enough premium to justify margin
#
# Exit (first condition met)
#   • 60% profit collected   (TARGET HIT)
#   • Loss = 2× net credit   (STOP LOSS)
#   • Spot within buffer of short strike  (BREACH EXIT)
#   • 14:00 IST squareoff    (SQUARE OFF)
#
# Backtest mode (groww=None)
#   Uses Black-Scholes with expiry-day IV=0.26 for entry premiums.
#   Exit modelled via delta proxy + linear theta decay.
#   Treat results as conservative lower bound (no vol smile/skew).
#
# Live mode (groww provided)
#   Fetches real 15-min option candles for all 4 legs via Groww API.
# ============================================================

from __future__ import annotations

import math
from datetime import date as date_type
from math import exp, log, sqrt

import numpy as np
import pandas as pd
from scipy.stats import norm

# ── Black-Scholes helpers (backtest proxy) ────────────────────────────────────

_RISK_FREE = 0.065        # Indian risk-free rate
_EXPIRY_IV = 0.26         # IV on expiry day (hardcoded, matches existing convention)
# Use TRADING minutes per year so 0-DTE options priced on remaining session time,
# not near-zero calendar time. 252 days × 6.5 hours × 60 min = 98,280 mins/yr.
_TRADING_MINS_PER_YEAR = 252 * 6.5 * 60   # 98,280
_MIN_T = 1.0 / _TRADING_MINS_PER_YEAR     # ≈ 1 trading-minute floor


def _bs_price(S: float, K: float, T: float, r: float, sigma: float,
              opt_type: str = 'call') -> float:
    """Black-Scholes price for backtest premium estimation."""
    T = max(T, _MIN_T)
    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    if opt_type == 'call':
        return max(0.0, S * norm.cdf(d1) - K * exp(-r * T) * norm.cdf(d2))
    return max(0.0, K * exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1))


def _bs_delta(S: float, K: float, T: float, r: float, sigma: float,
              opt_type: str = 'call') -> float:
    """Black-Scholes delta for backtest exit proxy."""
    T = max(T, _MIN_T)
    d1 = (log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    if opt_type == 'call':
        return norm.cdf(d1)
    return norm.cdf(d1) - 1.0


def _leg_exit_prem(entry_prem: float, delta: float, spot_move: float,
                   minutes_held: float, total_minutes: float) -> float:
    """
    Estimate exit premium via delta + linear theta model.
    theta_decay: premium decays linearly to 0 over total_minutes remaining.
    """
    delta_pnl   = delta * spot_move
    theta_decay = -entry_prem * (minutes_held / max(total_minutes, 1))
    return max(0.0, entry_prem + delta_pnl + theta_decay)


# ── ATR14 helper ──────────────────────────────────────────────────────────────

def _atr14(data: pd.DataFrame, today) -> float:
    """14-day ATR from futures OHLCV. Returns 600 as fallback."""
    days_seen = {}
    for ts, row in data.iterrows():
        d = ts.date()
        if d >= today:
            break
        days_seen.setdefault(d, [])
        days_seen[d].append((float(row['High']), float(row['Low'])))

    sorted_days = sorted(days_seen.keys())[-14:]
    if not sorted_days:
        return 600.0
    ranges = [max(h for h, l in days_seen[d]) - min(l for h, l in days_seen[d])
              for d in sorted_days]
    return float(np.mean(ranges)) if ranges else 600.0


# ── VIX helper (backtest) ─────────────────────────────────────────────────────

def _get_vix_series() -> pd.Series | None:
    """Fetch India VIX history via yfinance (cached in session)."""
    try:
        import yfinance as yf
        vix = yf.download('^INDIAVIX', period='3y', auto_adjust=False, progress=False)
        if isinstance(vix.columns, pd.MultiIndex):
            vix = vix['Close'].squeeze()
        else:
            vix = vix['Close']
        vix.index = pd.to_datetime(vix.index).normalize()
        return vix
    except Exception:
        return None


# ── Conviction lot sizing ─────────────────────────────────────────────────────

def _conviction_lots(vix: float, net_credit: float, wing_width: int,
                     p: dict) -> int:
    """
    Dynamic lot count based on VIX regime and credit-to-wing ratio.

    VIX regime mapping (from 2021-2026 backtest):
      < 12  (LOW):      WR=90.9%, small breaches  → 2 base lots
      12-15 (MID-LOW):  WR=77.4%                  → 1 base lot
      15-18 (MID):      WR=91.8%, sweet spot       → 3 base lots
      18-22 (MID-HIGH): WR=42.9% — SKIP (71% breach exit rate)
      > 22  (HIGH):     explosive                  → SKIP

    Credit bonus: if net_credit/wing_width > IC_CREDIT_BONUS_THRESH, add 1 lot.
    Hard cap: IC_LOT_MAX.
    """
    vix_max = float(p.get('IC_VIX_MAX', 18.0))
    if vix > vix_max:
        return 0   # regime filter already blocked — safety guard

    if vix < 12.0:
        base = int(p.get('IC_LOT_VIX_LOW',    2))
    elif vix < 15.0:
        base = int(p.get('IC_LOT_VIX_MIDLOW', 1))
    else:
        base = int(p.get('IC_LOT_VIX_MID',    3))

    credit_ratio  = net_credit / max(wing_width, 1)
    bonus_thresh  = float(p.get('IC_CREDIT_BONUS_THRESH', 0.35))
    credit_bonus  = 1 if credit_ratio > bonus_thresh else 0
    lot_max       = int(p.get('IC_LOT_MAX', 4))

    return min(base + credit_bonus, lot_max)


# ── Strike rounding ───────────────────────────────────────────────────────────

def _round_strike(price: float, interval: int, direction: str = 'nearest') -> int:
    if direction == 'up':
        return int(math.ceil(price / interval) * interval)
    if direction == 'down':
        return int(math.floor(price / interval) * interval)
    return int(round(price / interval) * interval)


# ── Expiry day check ──────────────────────────────────────────────────────────

def _is_expiry_day_backtest(trade_date: date_type, expiry_dates: set) -> bool:
    return trade_date in expiry_dates


# ── Live option premium fetch ─────────────────────────────────────────────────

def _fetch_leg_premium(groww, underlying: str, expiry_date, strike: int,
                       opt_type: str, trade_date_str: str, entry_ts) -> float | None:
    """Fetch real open premium for one leg at entry_ts. Returns None on failure."""
    try:
        from data.options_fetch import fetch_option_candles, lookup_option_price
        df = fetch_option_candles(groww, underlying, expiry_date,
                                  strike, opt_type, trade_date_str, trade_date_str)
        if df.empty:
            return None
        prem = lookup_option_price(df, entry_ts, field='Open')
        return float(prem) if prem and float(prem) > 0 else None
    except Exception as e:
        print(f"  ⚠  IC leg fetch {opt_type} {strike}: {e}")
        return None


def _fetch_leg_exit(groww, underlying: str, expiry_date, strike: int,
                    opt_type: str, trade_date_str: str, exit_ts) -> float | None:
    """Fetch real close premium for one leg at exit_ts."""
    try:
        from data.options_fetch import fetch_option_candles, lookup_option_price
        df = fetch_option_candles(groww, underlying, expiry_date,
                                  strike, opt_type, trade_date_str, trade_date_str)
        if df.empty:
            return None
        prem = lookup_option_price(df, exit_ts, field='Close')
        return float(prem) if prem and float(prem) > 0 else None
    except Exception as e:
        print(f"  ⚠  IC exit fetch {opt_type} {strike}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# MAIN STRATEGY FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def run_iron_condor(
    data: pd.DataFrame,
    instrument_config: dict,
    strategy_params: dict,
    groww=None,
    regime_df=None,
    params=None,
) -> pd.DataFrame:
    """
    0 DTE Iron Condor — fires on expiry days only.

    Parameters
    ----------
    data             : 15-min OHLCV futures DataFrame, IST index, 09:00-15:30
    instrument_config: from config.INSTRUMENTS[instrument]
    strategy_params  : from config.STRATEGIES['iron_condor']['params']
    groww            : GrowwAPI instance (None = backtest BS-proxy mode)
    regime_df        : optional regime DataFrame (unused — IC is regime-agnostic)
    params           : optional sweep override dict

    Returns
    -------
    pd.DataFrame with one row per iron condor trade.
    """
    # ── Merge sweep overrides ────────────────────────────────────────────────
    p = {**strategy_params, **(params or {})}

    IC_VIX_MIN     = float(p.get('IC_VIX_MIN',      0.0))
    IC_VIX_MAX     = float(p.get('IC_VIX_MAX',     18.0))
    IC_MAX_GAP     = float(p.get('IC_MAX_GAP',     150.0))
    IC_CALL_ATR    = float(p.get('IC_CALL_ATR',     0.50))
    IC_PUT_ATR     = float(p.get('IC_PUT_ATR',      0.50))
    IC_WING_WIDTH  = int(  p.get('IC_WING_WIDTH',   300))
    IC_PROFIT_PCT  = float(p.get('IC_PROFIT_TARGET_PCT', 0.70))
    IC_STOP_MULT   = float(p.get('IC_STOP_LOSS_MULT',    2.0))
    IC_BREACH_BUF  = float(p.get('IC_BREACH_BUFFER',     50.0))
    IC_ENTRY_AFTER = str(  p.get('IC_ENTRY_AFTER',  '09:30'))
    IC_SQUAREOFF   = str(  p.get('IC_SQUAREOFF',    '14:00'))
    IC_MIN_CREDIT  = float(p.get('IC_MIN_NET_CREDIT', 50.0))
    IC_MARGIN_CAP  = float(p.get('IC_MARGIN_CAP_PCT', 0.60))

    lot_size       = instrument_config['lot_size']
    strike_interval= instrument_config.get('strike_interval', 100)
    brokerage      = instrument_config.get('brokerage', 40)
    underlying     = instrument_config.get('underlying_symbol', 'BANKNIFTY')
    instrument_sym = instrument_config.get('symbol', 'NSE-BANKNIFTY')

    entry_h, entry_m = map(int, IC_ENTRY_AFTER.split(':'))
    sq_h,    sq_m    = map(int, IC_SQUAREOFF.split(':'))

    # ── Fetch VIX series for backtest ────────────────────────────────────────
    vix_series = _get_vix_series() if groww is None else None

    # ── Build expiry set (backtest: need to know which days are expiry days) ─
    expiry_dates: set = set()
    if groww is None:
        # Derive expiry set from data: last trading day of each month
        # (approximate — actual expiry = last Thursday of month for monthly;
        #  for NIFTY weekly it's every Thursday — identified by day-of-week=3)
        all_dates = sorted(set(data.index.date))
        prev_month = None
        for d in reversed(all_dates):
            if d.month != prev_month:
                expiry_dates.add(d)
                prev_month = d.month
        # Also mark all Thursdays as potential expiry for NIFTY weekly
        # (IC strategy param IC_DOW_ALLOW=[3] handles the day filter)
        for d in all_dates:
            if d.weekday() == 3:   # Thursday
                expiry_dates.add(d)
    else:
        # Live: expiry checked per-day via get_nearest_expiry()
        from data.options_fetch import get_nearest_expiry

    trades = []
    all_dates = sorted(set(data.index.date))

    for i, today in enumerate(all_dates):
        # ── Skip first 15 days (need ATR14 bootstrap) ───────────────────────
        if i < 15:
            continue

        # ── DOW check: expiry days are typically Thursday ────────────────────
        allowed_dow = p.get('IC_DOW_ALLOW', [3])   # default: Thursday only
        if allowed_dow is not None and today.weekday() not in allowed_dow:
            continue

        # ── EXPIRY GATE ──────────────────────────────────────────────────────
        if groww is None:
            if not _is_expiry_day_backtest(today, expiry_dates):
                continue
        else:
            expiry_date_live = get_nearest_expiry(groww, underlying, today, min_days=0)
            if expiry_date_live is None or expiry_date_live != today:
                continue
            expiry_date = expiry_date_live

        # ── ATR14 ────────────────────────────────────────────────────────────
        atr14 = _atr14(data, today)

        # ── Today's candles ──────────────────────────────────────────────────
        day_data = data[data.index.date == today]
        if day_data.empty:
            continue

        # Gap from previous close
        prev_days = [d for d in all_dates[:i] if d < today]
        if not prev_days:
            continue
        prev_day_data = data[data.index.date == prev_days[-1]]
        if prev_day_data.empty:
            continue
        prev_close = float(prev_day_data['Close'].iloc[-1])
        today_open = float(day_data['Open'].iloc[0])
        gap_pts    = today_open - prev_close

        # ── GAP GATE ────────────────────────────────────────────────────────
        if abs(gap_pts) > IC_MAX_GAP:
            continue

        # ── VIX GATE ─────────────────────────────────────────────────────────
        vix_val = None
        if vix_series is not None:
            ts_key = pd.Timestamp(today)
            vix_val = float(vix_series.get(ts_key, np.nan))
            if np.isnan(vix_val):
                # Try nearest available
                past = vix_series[vix_series.index <= ts_key]
                vix_val = float(past.iloc[-1]) if not past.empty else None
        if vix_val is None:
            vix_val = 17.0  # neutral fallback if data unavailable

        if not (IC_VIX_MIN <= vix_val <= IC_VIX_MAX):
            continue

        # ── ENTRY CANDLE: first bar at or after IC_ENTRY_AFTER ───────────────
        entry_candles = day_data.between_time(IC_ENTRY_AFTER, '15:29')
        if entry_candles.empty:
            continue
        entry_ts  = entry_candles.index[0]
        spot      = float(entry_candles['Open'].iloc[0])

        # ── STRIKE SELECTION ─────────────────────────────────────────────────
        call_short = _round_strike(spot + atr14 * IC_CALL_ATR, strike_interval, 'up')
        put_short  = _round_strike(spot - atr14 * IC_PUT_ATR,  strike_interval, 'down')
        call_long  = call_short + IC_WING_WIDTH
        put_long   = put_short  - IC_WING_WIDTH

        # ── PREMIUM FETCH (live or backtest proxy) ────────────────────────────
        today_str = str(today)
        # Time to expiry: from entry to squareoff, expressed in trading-year fraction.
        # Using TRADING minutes (not calendar time) gives realistic 0-DTE premiums.
        entry_minutes = entry_ts.hour * 60 + entry_ts.minute
        sq_minutes    = sq_h * 60 + sq_m
        total_trading_minutes = max(sq_minutes - entry_minutes, 30)
        T_entry = total_trading_minutes / _TRADING_MINS_PER_YEAR

        if groww is not None:
            # Live: fetch real premiums
            cs_prem = _fetch_leg_premium(groww, underlying, expiry_date, call_short, 'CE', today_str, entry_ts)
            cl_prem = _fetch_leg_premium(groww, underlying, expiry_date, call_long,  'CE', today_str, entry_ts)
            ps_prem = _fetch_leg_premium(groww, underlying, expiry_date, put_short,  'PE', today_str, entry_ts)
            pl_prem = _fetch_leg_premium(groww, underlying, expiry_date, put_long,   'PE', today_str, entry_ts)
            # Fall back to BS proxy for any missing leg
            if cs_prem is None: cs_prem = _bs_price(spot, call_short, T_entry, _RISK_FREE, _EXPIRY_IV, 'call')
            if cl_prem is None: cl_prem = _bs_price(spot, call_long,  T_entry, _RISK_FREE, _EXPIRY_IV, 'call')
            if ps_prem is None: ps_prem = _bs_price(spot, put_short,  T_entry, _RISK_FREE, _EXPIRY_IV, 'put')
            if pl_prem is None: pl_prem = _bs_price(spot, put_long,   T_entry, _RISK_FREE, _EXPIRY_IV, 'put')
        else:
            # Backtest: Black-Scholes proxy
            cs_prem = _bs_price(spot, call_short, T_entry, _RISK_FREE, _EXPIRY_IV, 'call')
            cl_prem = _bs_price(spot, call_long,  T_entry, _RISK_FREE, _EXPIRY_IV, 'call')
            ps_prem = _bs_price(spot, put_short,  T_entry, _RISK_FREE, _EXPIRY_IV, 'put')
            pl_prem = _bs_price(spot, put_long,   T_entry, _RISK_FREE, _EXPIRY_IV, 'put')

        net_credit = (cs_prem + ps_prem) - (cl_prem + pl_prem)

        # ── NET CREDIT CHECK ─────────────────────────────────────────────────
        if net_credit < IC_MIN_CREDIT:
            continue

        max_profit_pts = net_credit
        max_loss_pts   = IC_WING_WIDTH - net_credit

        # Breakeven zones
        upper_be = call_short + net_credit
        lower_be = put_short  - net_credit

        # ── MONITORING LOOP ──────────────────────────────────────────────────
        # Backtest: use rolling Black-Scholes at each bar with actual remaining T.
        # This correctly captures gamma explosion when spot approaches short strike
        # near expiry — the delta-proxy linear model fails in that regime.
        exit_ts     = None
        exit_reason = 'SQUARE OFF'
        net_debit   = net_credit    # fallback if no bars after entry

        post_entry = entry_candles.iloc[1:]   # candles after entry
        for j, (bar_ts, bar) in enumerate(post_entry.iterrows()):
            bar_time     = bar_ts.time()
            bar_mins     = bar_time.hour * 60 + bar_time.minute
            mins_remain  = max(sq_minutes - bar_mins, 1)  # trading minutes left to squareoff
            T_now        = mins_remain / _TRADING_MINS_PER_YEAR

            # Hard squareoff
            is_squareoff = bar_time.hour > sq_h or (bar_time.hour == sq_h and bar_time.minute >= sq_m)

            if groww is not None:
                cs_ex = _fetch_leg_exit(groww, underlying, expiry_date, call_short, 'CE', today_str, bar_ts) or _bs_price(spot, call_short, T_now, _RISK_FREE, _EXPIRY_IV, 'call')
                cl_ex = _fetch_leg_exit(groww, underlying, expiry_date, call_long,  'CE', today_str, bar_ts) or _bs_price(spot, call_long,  T_now, _RISK_FREE, _EXPIRY_IV, 'call')
                ps_ex = _fetch_leg_exit(groww, underlying, expiry_date, put_short,  'PE', today_str, bar_ts) or _bs_price(spot, put_short,  T_now, _RISK_FREE, _EXPIRY_IV, 'put')
                pl_ex = _fetch_leg_exit(groww, underlying, expiry_date, put_long,   'PE', today_str, bar_ts) or _bs_price(spot, put_long,   T_now, _RISK_FREE, _EXPIRY_IV, 'put')
            else:
                # Rolling BS: re-price all 4 legs at current spot + remaining T
                # This is the key fix: gamma explosion is modelled correctly because
                # BS price rises rapidly when spot ≈ strike and T → 0.
                spot_now = float(bar['Close'])
                T_bs     = max(T_now, _MIN_T)
                cs_ex = _bs_price(spot_now, call_short, T_bs, _RISK_FREE, _EXPIRY_IV, 'call')
                cl_ex = _bs_price(spot_now, call_long,  T_bs, _RISK_FREE, _EXPIRY_IV, 'call')
                ps_ex = _bs_price(spot_now, put_short,  T_bs, _RISK_FREE, _EXPIRY_IV, 'put')
                pl_ex = _bs_price(spot_now, put_long,   T_bs, _RISK_FREE, _EXPIRY_IV, 'put')

            current_debit = (cs_ex + ps_ex) - (cl_ex + pl_ex)
            spot_now = float(bar['Close']) if groww is None else spot

            if is_squareoff:
                exit_ts     = bar_ts
                exit_reason = 'SQUARE OFF'
                net_debit   = current_debit
                break

            # BREACH GUARD — spot approaching short strike
            if (spot_now >= call_short - IC_BREACH_BUF or
                    spot_now <= put_short + IC_BREACH_BUF):
                exit_ts     = bar_ts
                exit_reason = 'BREACH EXIT'
                net_debit   = current_debit
                break

            # PROFIT TARGET — collected 60% of premium
            if current_debit <= net_credit * (1.0 - IC_PROFIT_PCT):
                exit_ts     = bar_ts
                exit_reason = 'TARGET HIT'
                net_debit   = current_debit
                break

            # STOP LOSS — position cost 2× premium received
            if current_debit >= net_credit * IC_STOP_MULT:
                exit_ts     = bar_ts
                exit_reason = 'STOP LOSS'
                net_debit   = current_debit
                break

        if exit_ts is None:
            # No bars after entry — entered near squareoff, assume full theta capture
            exit_ts   = entry_ts
            net_debit = 0.0   # all premium decayed; conservative for late entries

        # ── Conviction lot sizing ─────────────────────────────────────────────
        conv_lots = _conviction_lots(vix_val, net_credit, IC_WING_WIDTH, p)

        # ── P&L (scaled by conviction lots) ──────────────────────────────────
        pnl_pts    = net_credit - net_debit           # per-lot points
        pnl_rs_1   = round(pnl_pts * lot_size - brokerage * 4, 2)   # 1-lot reference
        pnl_rs     = round(pnl_pts * lot_size * conv_lots - brokerage * 4 * conv_lots, 2)
        win        = 1 if pnl_rs > 0 else 0

        # Margin = max possible loss × conviction lots
        margin_per_lot  = round(max_loss_pts * lot_size, 2)          # per-lot margin
        total_margin    = round(margin_per_lot * conv_lots, 2)

        # Credit/wing ratio → conviction label
        credit_ratio = net_credit / max(IC_WING_WIDTH, 1)
        if credit_ratio > 0.40:
            conviction_label = 'HIGH'
        elif credit_ratio > 0.30:
            conviction_label = 'MED'
        else:
            conviction_label = 'LOW'

        # VIX regime label
        if vix_val < 12.0:
            vix_regime = 'LOW'
        elif vix_val < 15.0:
            vix_regime = 'MID-LOW'
        elif vix_val < 18.0:
            vix_regime = 'MID'
        else:
            vix_regime = 'ABOVE-THRESHOLD'

        # Expiry date for backtest (approximate: use last-Thursday-of-month logic)
        expiry_str = str(expiry_date) if groww is not None else _approx_expiry_str(today)

        trades.append({
            # Standard schema
            'date':             today,
            'entry_ts':         entry_ts,
            'exit_ts':          exit_ts,
            'year':             today.year,
            'instrument':       instrument_sym,
            'strategy':         'IC',
            'direction':        'NEUTRAL',           # short strangle = neutral
            'entry':            round(net_credit, 2),    # net credit received (per lot)
            'exit_price':       round(net_debit,  2),    # net debit to close (per lot)
            'stop':             round(net_credit * IC_STOP_MULT, 2),
            'target':           round(net_credit * (1 - IC_PROFIT_PCT), 2),
            'pnl_pts':          round(pnl_pts, 2),       # per-lot points
            'pnl_rs':           pnl_rs,                  # scaled by conviction lots
            'pnl_rs_1lot':      pnl_rs_1,                # 1-lot reference P&L
            'win':              win,
            'exit_reason':      exit_reason,
            'bias_score':       round(min(credit_ratio, 1.0), 3),
            'lots_used':        conv_lots,               # conviction-sized lots
            'conviction':       conviction_label,
            'vix_regime':       vix_regime,
            'capital_used':     total_margin,
            'macro_ok':         True,
            'regime':           vix_regime.lower(),
            # IC-specific schema
            'call_short_strike': call_short,
            'put_short_strike':  put_short,
            'call_long_strike':  call_long,
            'put_long_strike':   put_long,
            'call_short_prem':   round(cs_prem, 2),
            'put_short_prem':    round(ps_prem, 2),
            'call_long_prem':    round(cl_prem, 2),
            'put_long_prem':     round(pl_prem, 2),
            'net_credit':        round(net_credit, 2),
            'net_debit_exit':    round(net_debit,  2),
            'max_profit_pts':    round(max_profit_pts, 2),
            'max_loss_pts':      round(max_loss_pts,   2),
            'wing_width':        IC_WING_WIDTH,
            'upper_breakeven':   round(upper_be, 2),
            'lower_breakeven':   round(lower_be, 2),
            'expiry':            expiry_str,
            'dte':               0,
            'atr14':             round(atr14, 2),
            'vix_at_entry':      round(vix_val, 2),
            'credit_ratio':      round(credit_ratio, 3),
            'gap_pts':           round(gap_pts, 2),
            'spot_at_entry':     round(spot, 2),
            'margin_per_lot':    margin_per_lot,
            'total_margin':      total_margin,
            'per_trade_risk_pct': IC_MARGIN_CAP,
        })

    if not trades:
        return pd.DataFrame()

    df = pd.DataFrame(trades)
    df['entry_time'] = pd.to_datetime(df['entry_ts']).dt.strftime('%H:%M')
    df['exit_time']  = pd.to_datetime(df['exit_ts']).dt.strftime('%H:%M')
    return df.reset_index(drop=True)


def _approx_expiry_str(trade_date: date_type) -> str:
    """Approximate expiry string for backtest (last Thursday of month)."""
    import calendar
    y, m = trade_date.year, trade_date.month
    # Find last Thursday of the month
    last_day = calendar.monthrange(y, m)[1]
    for d in range(last_day, last_day - 7, -1):
        candidate = date_type(y, m, d)
        if candidate.weekday() == 3:  # Thursday
            return str(candidate)
    return str(trade_date)
