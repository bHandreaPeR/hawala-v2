# ============================================================
# strategies/expiry_spread.py — Hawala v2  Expiry Day Directional Spread
# ============================================================
# Short-premium, defined-risk directional credit spread.
# Fires ONLY on expiry day (0 DTE) after the ORB window settles.
#
# Structure (direction driven by opening gap — "V3" signal)
# ---------
#   Gap UP  (gap > ES_GAP_THRESHOLD):  BULL PUT SPREAD
#     SELL OTM Put + BUY further-OTM Put  (collect premium on downside)
#
#   Gap DOWN (gap < -ES_GAP_THRESHOLD): BEAR CALL SPREAD
#     SELL OTM Call + BUY further-OTM Call (collect premium on upside)
#
#   Flat (|gap| ≤ threshold):  SKIP — no directional conviction
#
# Advantages over Iron Condor
#   • 2 legs → half the brokerage (₹80 vs ₹160 per lot)
#   • Directional conviction → higher win rate on correct side
#   • Simpler exit: only one short-strike breach to watch
#   • Lower margin: ~half the SPAN requirement
#
# Entry gates (ALL must pass)
#   1. Expiry day only  (backtest: all expiry-DOW days; live: get_nearest_expiry)
#   2. |gap| > ES_GAP_THRESHOLD  — directional signal required
#   3. VIX in [ES_VIX_MIN, ES_VIX_MAX]  — sell premium in right IV regime
#   4. net_credit ≥ ES_MIN_NET_CREDIT  — enough premium to justify margin
#
# Exit (first condition met)
#   • 70% profit collected        (TARGET HIT)
#   • Loss = 2× net credit        (STOP LOSS)
#   • Spot within buffer of short  (BREACH EXIT)
#   • 14:00 IST squareoff         (SQUARE OFF)
#
# Sizing
#   Backtest: ES_FIXED_LOT=True → fixed 1 lot per trade (clean WR comparison)
#   Live:     conviction sizing via VIX scalar (same logic as iron_condor)
#
# Instruments supported
#   BANKNIFTY  (monthly expiry, Thursday, DOW=3)
#   NIFTY      (weekly expiry,  Thursday, DOW=3)
#   SENSEX     (weekly expiry,  Friday,   DOW=4 — set expiry_dow=4 in config)
# ============================================================

from __future__ import annotations

import math
from datetime import date as date_type
from math import exp, log, sqrt

import numpy as np
import pandas as pd
from scipy.stats import norm


# ── Historical lot-size lookup ────────────────────────────────────────────────

def _lot_size_for_date(trade_date, instrument_config: dict) -> int:
    """Return the correct lot size for a given trade date from lot_size_history."""
    history = instrument_config.get('lot_size_history', [])
    for start_s, end_s, size in history:
        start_d = pd.Timestamp(start_s).date()
        end_d   = pd.Timestamp(end_s).date()
        if start_d <= trade_date <= end_d:
            return size
    return instrument_config.get('lot_size', 30)


# ── Black-Scholes helpers (backtest proxy) ────────────────────────────────────

_RISK_FREE = 0.065        # Indian risk-free rate
_EXPIRY_IV = 0.26         # IV on expiry day (matches iron_condor convention)
_TRADING_MINS_PER_YEAR = 252 * 6.5 * 60   # 98,280 — trading minutes per year
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

_VIX_CACHE: pd.Series | None | bool = False   # False = not yet fetched

def _get_vix_series() -> pd.Series | None:
    """Fetch India VIX history via yfinance (module-level cached — fetched once per process)."""
    global _VIX_CACHE
    if _VIX_CACHE is not False:   # already fetched (result is either Series or None)
        return _VIX_CACHE
    try:
        import yfinance as yf
        vix_raw = yf.download('^INDIAVIX', period='6y', auto_adjust=False, progress=False)
        if vix_raw is None or vix_raw.empty:
            _VIX_CACHE = None
            return None
        # Handle MultiIndex columns (yfinance ≥0.2.x ticker-level MultiIndex)
        if isinstance(vix_raw.columns, pd.MultiIndex):
            lvl0 = vix_raw.columns.get_level_values(0)
            if 'Close' in lvl0:
                vix = vix_raw['Close']
                if isinstance(vix, pd.DataFrame):
                    vix = vix.iloc[:, 0]
            else:
                _VIX_CACHE = None
                return None
        else:
            if 'Close' not in vix_raw.columns:
                _VIX_CACHE = None
                return None
            vix = vix_raw['Close']
        vix = vix.squeeze()
        if not isinstance(vix, pd.Series):
            _VIX_CACHE = None
            return None
        vix.index = pd.to_datetime(vix.index).normalize()
        _VIX_CACHE = vix.dropna()
        return _VIX_CACHE
    except Exception:
        _VIX_CACHE = None
        return None


# ── Strike rounding ───────────────────────────────────────────────────────────

def _round_strike(price: float, interval: int, direction: str = 'nearest') -> int:
    if direction == 'up':
        return int(math.ceil(price / interval) * interval)
    if direction == 'down':
        return int(math.floor(price / interval) * interval)
    return int(round(price / interval) * interval)


# ── Expiry day check (backtest) ───────────────────────────────────────────────

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
        print(f"  ⚠  ES leg fetch {opt_type} {strike}: {e}")
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
        print(f"  ⚠  ES exit fetch {opt_type} {strike}: {e}")
        return None


# ── Approximate expiry string (backtest) ──────────────────────────────────────

def _approx_expiry_str(trade_date: date_type, expiry_dow: int = 3) -> str:
    """Approximate expiry string: last occurrence of expiry_dow in month."""
    import calendar
    y, m = trade_date.year, trade_date.month
    last_day = calendar.monthrange(y, m)[1]
    for d in range(last_day, last_day - 7, -1):
        candidate = date_type(y, m, d)
        if candidate.weekday() == expiry_dow:
            return str(candidate)
    return str(trade_date)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN STRATEGY FUNCTION
# ══════════════════════════════════════════════════════════════════════════════

def run_expiry_spread(
    data: pd.DataFrame,
    instrument_config: dict,
    strategy_params: dict,
    groww=None,
    regime_df=None,
    params=None,
) -> pd.DataFrame:
    """
    Expiry Day Directional Credit Spread — BULL PUT or BEAR CALL based on gap direction.

    Parameters
    ----------
    data             : 15-min OHLCV futures DataFrame, IST index, 09:00-15:30
    instrument_config: from config.INSTRUMENTS[instrument]
    strategy_params  : from config.STRATEGIES['expiry_spread']['params']
    groww            : GrowwAPI instance (None = backtest BS-proxy mode)
    regime_df        : optional regime DataFrame (unused)
    params           : optional sweep override dict

    Returns
    -------
    pd.DataFrame with one row per spread trade.
    """
    # ── Merge sweep overrides ────────────────────────────────────────────────
    p = {**strategy_params, **(params or {})}

    ES_VIX_MIN       = float(p.get('ES_VIX_MIN',           0.0))
    ES_VIX_MAX       = float(p.get('ES_VIX_MAX',          18.0))
    ES_GAP_THRESHOLD = float(p.get('ES_GAP_THRESHOLD',    30.0))
    ES_MAX_GAP       = float(p.get('ES_MAX_GAP',          400.0))
    ES_CALL_ATR      = float(p.get('ES_CALL_ATR',          0.50))
    ES_PUT_ATR       = float(p.get('ES_PUT_ATR',           0.50))
    ES_WING_WIDTH    = int(  p.get('ES_WING_WIDTH',         200))
    ES_PROFIT_PCT    = float(p.get('ES_PROFIT_TARGET_PCT', 0.70))
    ES_STOP_MULT     = float(p.get('ES_STOP_LOSS_MULT',    2.0))
    ES_BREACH_BUF    = float(p.get('ES_BREACH_BUFFER',    30.0))
    ES_ENTRY_AFTER   = str(  p.get('ES_ENTRY_AFTER',     '09:30'))
    ES_SQUAREOFF     = str(  p.get('ES_SQUAREOFF',        '14:00'))
    ES_MIN_CREDIT    = float(p.get('ES_MIN_NET_CREDIT',   30.0))
    ES_FIXED_LOT     = bool( p.get('ES_FIXED_LOT',         True))   # 1-lot for backtest
    ES_LOT_MAX       = int(  p.get('ES_LOT_MAX',            10))
    ES_LOT_MIN       = int(  p.get('ES_LOT_MIN',             1))

    lot_size        = instrument_config['lot_size']       # current (fallback)
    strike_interval = instrument_config.get('strike_interval', 100)
    brokerage       = instrument_config.get('brokerage',   40)
    underlying      = instrument_config.get('underlying_symbol', 'BANKNIFTY')
    instrument_sym  = instrument_config.get('symbol', 'NSE-BANKNIFTY')
    expiry_dow      = instrument_config.get('expiry_dow',    3)    # 3=Thu, 4=Fri

    entry_h, entry_m = map(int, ES_ENTRY_AFTER.split(':'))
    sq_h,    sq_m    = map(int, ES_SQUAREOFF.split(':'))

    # ── Fetch VIX series for backtest ────────────────────────────────────────
    vix_series = _get_vix_series() if groww is None else None

    # ── Build expiry set (backtest) ───────────────────────────────────────────
    monthly_only = instrument_config.get('monthly_only', False)

    # Prefer injected real Groww calendar (accurate DOW + holidays)
    _injected_expiry = p.get('_expiry_dates', None)

    if groww is None:
        if _injected_expiry is not None:
            # Real calendar from Groww API — most accurate, handles all DOW changes
            expiry_dates: set = set(_injected_expiry)
        else:
            # Fallback: approximate from expiry_dow (used when no API available)
            import calendar as _cal
            all_dates_full   = sorted(set(data.index.date))
            trading_date_set = set(all_dates_full)
            expiry_dates     = set()

            if monthly_only:
                # Last occurrence of expiry_dow in each calendar month
                years  = range(all_dates_full[0].year, all_dates_full[-1].year + 1)
                months = range(1, 13)
                for y in years:
                    for m in months:
                        last_day = _cal.monthrange(y, m)[1]
                        for day in range(last_day, last_day - 7, -1):
                            try:
                                candidate = date_type(y, m, day)
                            except ValueError:
                                continue
                            if (candidate.weekday() == expiry_dow
                                    and candidate in trading_date_set):
                                expiry_dates.add(candidate)
                                break
            else:
                # All trading days matching expiry_dow (NIFTY / SENSEX weekly)
                for d in all_dates_full:
                    if d.weekday() == expiry_dow:
                        expiry_dates.add(d)
    else:
        from data.options_fetch import get_nearest_expiry

    trades = []
    all_dates = sorted(set(data.index.date))

    # Pre-group data by date for O(1) lookup (avoids 30K scan per day per combo)
    _day_cache_param = p.get('_day_cache', None)
    if _day_cache_param:
        day_groups = _day_cache_param
    else:
        day_groups = {d: grp for d, grp in data.groupby(data.index.date)}

    _atr_cache = p.get('_atr_cache', None)

    for i, today in enumerate(all_dates):
        # Need ATR14 bootstrap — skip first 15 days
        if i < 15:
            continue

        # ── EXPIRY GATE ──────────────────────────────────────────────────────
        if groww is None:
            if not _is_expiry_day_backtest(today, expiry_dates):
                continue
        else:
            expiry_date_live = get_nearest_expiry(
                groww, underlying, today, min_days=0,
                exchange=instrument_config.get('exchange', 'NSE')
            )
            if expiry_date_live is None or expiry_date_live != today:
                continue
            expiry_date = expiry_date_live

        # ── Per-date lot size (SEBI revisions) ───────────────────────────────
        lot_size = _lot_size_for_date(today, instrument_config)

        # ── ATR14 (use pre-computed cache if provided via params) ────────────
        atr14 = (_atr_cache.get(today, _atr14(data, today))
                 if _atr_cache else _atr14(data, today))

        # ── Today's candles ──────────────────────────────────────────────────
        day_data = day_groups.get(today)
        if day_data is None or day_data.empty:
            continue

        # ── Gap (V3 direction signal) ─────────────────────────────────────────
        prev_days = [d for d in all_dates[:i] if d < today]
        if not prev_days:
            continue
        prev_day_data = day_groups.get(prev_days[-1])
        if prev_day_data is None or prev_day_data.empty:
            continue
        prev_close = float(prev_day_data['Close'].iloc[-1])
        today_open = float(day_data['Open'].iloc[0])
        gap_pts    = today_open - prev_close

        # ── DIRECTION GATE ────────────────────────────────────────────────────
        if gap_pts > ES_GAP_THRESHOLD:
            direction    = 'BULL'     # gap up → sell put spread (downside premium)
            opt_type_short = 'put'
        elif gap_pts < -ES_GAP_THRESHOLD:
            direction    = 'BEAR'     # gap down → sell call spread (upside premium)
            opt_type_short = 'call'
        else:
            continue    # flat open — no directional conviction, skip

        # ── MAX GAP SANITY (extreme gaps can skew BS pricing) ────────────────
        if abs(gap_pts) > ES_MAX_GAP:
            continue

        # ── VIX GATE ─────────────────────────────────────────────────────────
        vix_val = None
        if vix_series is not None:
            ts_key  = pd.Timestamp(today)
            vix_val = float(vix_series.get(ts_key, np.nan))
            if np.isnan(vix_val):
                past    = vix_series[vix_series.index <= ts_key]
                vix_val = float(past.iloc[-1]) if not past.empty else None
        if vix_val is None:
            vix_val = 17.0   # neutral fallback

        if not (ES_VIX_MIN <= vix_val <= ES_VIX_MAX):
            continue

        # ── ENTRY CANDLE ──────────────────────────────────────────────────────
        entry_candles = day_data.between_time(ES_ENTRY_AFTER, '15:29')
        if entry_candles.empty:
            continue
        entry_ts = entry_candles.index[0]
        spot     = float(entry_candles['Open'].iloc[0])

        # ── STRIKE SELECTION ─────────────────────────────────────────────────
        if direction == 'BULL':
            # Short put: below spot (OTM)
            short_strike = _round_strike(spot - atr14 * ES_PUT_ATR,
                                         strike_interval, 'down')
            long_strike  = short_strike - ES_WING_WIDTH   # protective wing
        else:  # BEAR
            # Short call: above spot (OTM)
            short_strike = _round_strike(spot + atr14 * ES_CALL_ATR,
                                         strike_interval, 'up')
            long_strike  = short_strike + ES_WING_WIDTH   # protective wing

        # Safety: strike must be positive and non-zero
        if short_strike <= 0 or long_strike <= 0:
            continue

        # ── TIME TO EXPIRY (trading minutes → year fraction) ─────────────────
        entry_minutes         = entry_ts.hour * 60 + entry_ts.minute
        sq_minutes            = sq_h * 60 + sq_m
        total_trading_minutes = max(sq_minutes - entry_minutes, 30)
        T_entry               = total_trading_minutes / _TRADING_MINS_PER_YEAR

        # ── PREMIUM FETCH (live or BS backtest proxy) ─────────────────────────
        today_str = str(today)
        if groww is not None:
            short_prem = _fetch_leg_premium(groww, underlying, expiry_date,
                                            short_strike, opt_type_short.upper(),
                                            today_str, entry_ts)
            long_prem  = _fetch_leg_premium(groww, underlying, expiry_date,
                                            long_strike,  opt_type_short.upper(),
                                            today_str, entry_ts)
            if short_prem is None:
                short_prem = _bs_price(spot, short_strike, T_entry, _RISK_FREE,
                                       _EXPIRY_IV, opt_type_short)
            if long_prem is None:
                long_prem  = _bs_price(spot, long_strike,  T_entry, _RISK_FREE,
                                       _EXPIRY_IV, opt_type_short)
        else:
            short_prem = _bs_price(spot, short_strike, T_entry, _RISK_FREE,
                                   _EXPIRY_IV, opt_type_short)
            long_prem  = _bs_price(spot, long_strike,  T_entry, _RISK_FREE,
                                   _EXPIRY_IV, opt_type_short)

        net_credit = short_prem - long_prem   # always positive (short closer to ATM)

        # ── NET CREDIT CHECK ─────────────────────────────────────────────────
        if net_credit < ES_MIN_CREDIT:
            continue

        max_profit_pts = net_credit
        max_loss_pts   = ES_WING_WIDTH - net_credit

        # Breakeven: the spot level at which the spread is exactly at loss
        if direction == 'BULL':
            breakeven = short_strike - net_credit   # put spread: BE = short - credit
        else:
            breakeven = short_strike + net_credit   # call spread: BE = short + credit

        # ── MONITORING LOOP ───────────────────────────────────────────────────
        exit_ts     = None
        exit_reason = 'SQUARE OFF'
        net_debit   = net_credit   # fallback: position decays to full profit

        post_entry = entry_candles.iloc[1:]
        for j, (bar_ts, bar) in enumerate(post_entry.iterrows()):
            bar_time    = bar_ts.time()
            bar_mins    = bar_time.hour * 60 + bar_time.minute
            mins_remain = max(sq_minutes - bar_mins, 1)
            T_now       = mins_remain / _TRADING_MINS_PER_YEAR

            is_squareoff = (bar_time.hour > sq_h or
                            (bar_time.hour == sq_h and bar_time.minute >= sq_m))

            if groww is not None:
                sh_ex = (_fetch_leg_exit(groww, underlying, expiry_date,
                                         short_strike, opt_type_short.upper(),
                                         today_str, bar_ts)
                         or _bs_price(spot, short_strike, T_now, _RISK_FREE,
                                      _EXPIRY_IV, opt_type_short))
                lg_ex = (_fetch_leg_exit(groww, underlying, expiry_date,
                                         long_strike, opt_type_short.upper(),
                                         today_str, bar_ts)
                         or _bs_price(spot, long_strike,  T_now, _RISK_FREE,
                                      _EXPIRY_IV, opt_type_short))
            else:
                spot_now = float(bar['Close'])
                T_bs     = max(T_now, _MIN_T)
                sh_ex = _bs_price(spot_now, short_strike, T_bs, _RISK_FREE,
                                  _EXPIRY_IV, opt_type_short)
                lg_ex = _bs_price(spot_now, long_strike,  T_bs, _RISK_FREE,
                                  _EXPIRY_IV, opt_type_short)

            current_debit = sh_ex - lg_ex
            spot_now = float(bar['Close']) if groww is None else spot

            if is_squareoff:
                exit_ts     = bar_ts
                exit_reason = 'SQUARE OFF'
                net_debit   = current_debit
                break

            # BREACH GUARD — spot approaching short strike
            if direction == 'BULL':
                breach = spot_now <= short_strike + ES_BREACH_BUF
            else:
                breach = spot_now >= short_strike - ES_BREACH_BUF

            if breach:
                exit_ts     = bar_ts
                exit_reason = 'BREACH EXIT'
                net_debit   = current_debit
                break

            # PROFIT TARGET — collected ES_PROFIT_PCT of premium
            if current_debit <= net_credit * (1.0 - ES_PROFIT_PCT):
                exit_ts     = bar_ts
                exit_reason = 'TARGET HIT'
                net_debit   = current_debit
                break

            # STOP LOSS — position cost ES_STOP_MULT × premium received
            if current_debit >= net_credit * ES_STOP_MULT:
                exit_ts     = bar_ts
                exit_reason = 'STOP LOSS'
                net_debit   = current_debit
                break

        if exit_ts is None:
            exit_ts   = entry_ts
            net_debit = 0.0   # entered near squareoff, assume full decay

        # ── Lot sizing ────────────────────────────────────────────────────────
        if ES_FIXED_LOT:
            lots_used = 1
        else:
            # VIX-scalar sizing (live / compounding mode)
            equity_now = float(p.get('_equity', 0.0))
            if vix_val < 12.0:
                scalar = float(p.get('ES_VIX_SCALAR_LOW',    0.50))
            elif vix_val < 15.0:
                scalar = float(p.get('ES_VIX_SCALAR_MIDLOW', 0.70))
            else:
                scalar = float(p.get('ES_VIX_SCALAR_MID',    1.00))
            if equity_now > 0:
                risk_pct    = float(p.get('ES_RISK_PER_TRADE_PCT', 0.05))
                margin_1lot = ES_WING_WIDTH * lot_size
                raw         = (equity_now * risk_pct * scalar) / max(margin_1lot, 1)
                lots_used   = max(ES_LOT_MIN, min(int(raw), ES_LOT_MAX))
            else:
                lots_used = ES_LOT_MIN

        # ── P&L ───────────────────────────────────────────────────────────────
        pnl_pts   = net_credit - net_debit                     # per-lot points
        pnl_rs_1  = round(pnl_pts * lot_size - brokerage * 2, 2)   # 1-lot ref
        pnl_rs    = round(pnl_pts * lot_size * lots_used
                          - brokerage * 2 * lots_used, 2)
        win       = 1 if pnl_rs > 0 else 0

        margin_per_lot = round(max_loss_pts * lot_size, 2)
        total_margin   = round(margin_per_lot * lots_used, 2)

        # VIX regime label
        if vix_val < 12.0:
            vix_regime = 'LOW'
        elif vix_val < 15.0:
            vix_regime = 'MID-LOW'
        elif vix_val < 18.0:
            vix_regime = 'MID'
        else:
            vix_regime = 'ABOVE-THRESHOLD'

        credit_ratio    = net_credit / max(ES_WING_WIDTH, 1)
        expiry_str      = (str(expiry_date) if groww is not None
                           else _approx_expiry_str(today, expiry_dow))

        trades.append({
            # Standard schema
            'date':              today,
            'entry_ts':          entry_ts,
            'exit_ts':           exit_ts,
            'year':              today.year,
            'instrument':        instrument_sym,
            'strategy':          'EXPIRY_SPREAD',
            'direction':         direction,
            'entry':             round(net_credit, 2),   # credit received
            'exit_price':        round(net_debit,  2),   # debit to close
            'stop':              round(net_credit * ES_STOP_MULT, 2),
            'target':            round(net_credit * (1 - ES_PROFIT_PCT), 2),
            'pnl_pts':           round(pnl_pts, 2),
            'pnl_rs':            pnl_rs,
            'pnl_rs_1lot':       pnl_rs_1,
            'win':               win,
            'exit_reason':       exit_reason,
            # Spread-specific schema
            'spread_type':       ('Bull Put Spread' if direction == 'BULL'
                                  else 'Bear Call Spread'),
            'opt_type':          opt_type_short,
            'short_strike':      short_strike,
            'long_strike':       long_strike,
            'short_prem':        round(short_prem, 2),
            'long_prem':         round(long_prem,  2),
            'net_credit':        round(net_credit,  2),
            'net_debit_exit':    round(net_debit,   2),
            'max_profit_pts':    round(max_profit_pts, 2),
            'max_loss_pts':      round(max_loss_pts,   2),
            'wing_width':        ES_WING_WIDTH,
            'breakeven':         round(breakeven, 2),
            'expiry':            expiry_str,
            'dte':               0,
            'atr14':             round(atr14, 2),
            'vix_at_entry':      round(vix_val, 2),
            'vix_regime':        vix_regime,
            'gap_pts':           round(gap_pts, 2),
            'spot_at_entry':     round(spot, 2),
            'bias_score':        round(min(credit_ratio, 1.0), 3),
            'lots_used':         lots_used,
            'margin_per_lot':    margin_per_lot,
            'total_margin':      total_margin,
            'per_trade_risk_pct': 0.05,
            'macro_ok':          True,
            'regime':            vix_regime.lower(),
        })

    if not trades:
        return pd.DataFrame()

    df = pd.DataFrame(trades)
    df['entry_time'] = pd.to_datetime(df['entry_ts']).dt.strftime('%H:%M')
    df['exit_time']  = pd.to_datetime(df['exit_ts']).dt.strftime('%H:%M')
    return df.reset_index(drop=True)
