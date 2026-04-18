# ============================================================
# backtest/options_layer.py — ATM Options Simulation Layer
# ============================================================
# Post-processing step that adds ATM options P&L columns to
# any futures-based trade log. Does NOT modify strategy logic.
#
# For each trade:
#   LONG  signal → buy ATM Call (CE) — profits from up move
#   SHORT signal → buy ATM Put  (PE) — profits from down move
#
# Usage:
#   from backtest.options_layer import add_options_simulation
#   trade_log = add_options_simulation(trade_log, groww, instrument='BANKNIFTY')
#
# New columns added:
#   opt_contract, opt_strike, opt_expiry, opt_dte,
#   opt_entry_px, opt_exit_px, opt_pnl_pts, opt_pnl_rs
# ============================================================

import time
import pandas as pd
import numpy as np
from datetime import datetime


def add_options_simulation(trade_log: pd.DataFrame,
                            groww,
                            instrument: str = 'BANKNIFTY') -> pd.DataFrame:
    """
    For each trade in trade_log, fetch the ATM option that would have been
    traded and compute entry/exit premium and P&L.

    Args:
        trade_log  : Output from run_backtest() — must have entry_ts, exit_ts,
                     direction, entry (futures price) columns.
        groww      : Authenticated GrowwAPI instance.
        instrument : Key from config.INSTRUMENTS.

    Returns:
        trade_log with additional opt_* columns appended.
    """
    from config import INSTRUMENTS
    from data.contract_resolver import (build_options_symbol, get_atm_strike,
                                         get_weekly_expiry)

    if trade_log.empty:
        return trade_log

    cfg               = INSTRUMENTS[instrument]
    underlying_symbol = cfg['underlying_symbol']
    lot_size          = cfg['lot_size']
    strike_interval   = cfg['strike_interval']
    brokerage         = cfg['brokerage']

    trade_log = trade_log.copy()

    # Initialise new columns
    for col in ['opt_contract', 'opt_expiry', 'opt_strike', 'opt_dte',
                'opt_entry_px', 'opt_exit_px', 'opt_pnl_pts', 'opt_pnl_rs']:
        trade_log[col] = None

    # Cache: (trade_date, strike, expiry, opt_type) → DataFrame of candles
    _candle_cache: dict = {}

    total = len(trade_log)
    print(f"\n── Options layer: processing {total} trades ──")

    for idx, row in trade_log.iterrows():
        try:
            trade_date = row['date']
            direction  = row.get('direction', 'LONG')
            entry_px   = row.get('entry')
            entry_ts   = row.get('entry_ts')
            exit_ts    = row.get('exit_ts')

            if entry_px is None or entry_ts is None or exit_ts is None:
                continue

            # ── Determine option type and ATM strike ──────────────────────────
            opt_type = 'CE' if direction == 'LONG' else 'PE'
            strike   = get_atm_strike(float(entry_px), strike_interval)

            # ── Find nearest weekly expiry ────────────────────────────────────
            expiry = get_weekly_expiry(trade_date, underlying_symbol, groww)
            if expiry is None:
                continue

            dte = (expiry - trade_date).days

            # ── Build options symbol ──────────────────────────────────────────
            opt_symbol = build_options_symbol(
                'NSE', underlying_symbol, expiry, strike, opt_type
            )

            # ── Fetch option candles (cached) ─────────────────────────────────
            cache_key = opt_symbol
            if cache_key not in _candle_cache:
                opt_data = _fetch_option_candles(
                    groww, opt_symbol,
                    trade_date.strftime('%Y-%m-%d 00:00:00'),
                    trade_date.strftime('%Y-%m-%d 23:59:59'),
                )
                _candle_cache[cache_key] = opt_data
                time.sleep(0.2)
            else:
                opt_data = _candle_cache[cache_key]

            if opt_data.empty:
                continue

            # ── Get entry and exit premiums ───────────────────────────────────
            entry_premium = _price_at_ts(opt_data, entry_ts, side='open')
            exit_premium  = _price_at_ts(opt_data, exit_ts,  side='close')

            if entry_premium is None or exit_premium is None:
                continue

            pnl_pts = exit_premium - entry_premium
            pnl_rs  = round(pnl_pts * lot_size - brokerage, 2)

            trade_log.at[idx, 'opt_contract']  = opt_symbol
            trade_log.at[idx, 'opt_expiry']    = expiry
            trade_log.at[idx, 'opt_strike']    = strike
            trade_log.at[idx, 'opt_dte']       = dte
            trade_log.at[idx, 'opt_entry_px']  = round(entry_premium, 2)
            trade_log.at[idx, 'opt_exit_px']   = round(exit_premium, 2)
            trade_log.at[idx, 'opt_pnl_pts']   = round(pnl_pts, 2)
            trade_log.at[idx, 'opt_pnl_rs']    = pnl_rs

        except Exception as e:
            # Don't let one trade failure abort the whole run
            pass

    filled = trade_log['opt_contract'].notna().sum()
    print(f"  ✅ Options data: {filled}/{total} trades have option P&L")

    if filled > 0:
        valid = trade_log[trade_log['opt_pnl_rs'].notna()]
        print(f"  Options P&L (buying ATM {'{CE/PE}'}): "
              f"₹{valid['opt_pnl_rs'].sum():,.0f} total | "
              f"₹{valid['opt_pnl_rs'].mean():,.0f} avg | "
              f"{(valid['opt_pnl_rs'] > 0).mean()*100:.1f}% win")

    return trade_log


def _fetch_option_candles(groww, symbol: str,
                           start_str: str, end_str: str,
                           max_retries: int = 4) -> pd.DataFrame:
    """
    Fetch one day's 15-min candles for an options contract.
    Retries with exponential backoff on rate-limit errors.
    Returns DataFrame indexed by datetime, or empty DataFrame on failure.
    """
    _seg_fno = getattr(groww, 'SEGMENT_FNO',
               getattr(groww, 'SEGMENT_FO', 'FNO'))
    delay = 1.0

    for attempt in range(max_retries):
        try:
            result = groww.get_historical_candles(
                exchange        = groww.EXCHANGE_NSE,
                segment         = _seg_fno,
                groww_symbol    = symbol,
                start_time      = start_str,
                end_time        = end_str,
                candle_interval = groww.CANDLE_INTERVAL_MIN_15,
            )

            if isinstance(result, dict):
                candles = result.get('candles', result.get('data', []))
            elif isinstance(result, list):
                candles = result
            else:
                return pd.DataFrame()

            if not candles:
                return pd.DataFrame()

            # FNO candles: [ISO_ts, O, H, L, C, Vol, OI]
            std_names = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Oi']
            if isinstance(candles[0], (list, tuple)):
                n  = len(candles[0])
                df = pd.DataFrame(candles, columns=std_names[:n])
            else:
                df = pd.DataFrame(candles)
                df.columns = [c.capitalize() for c in df.columns]

            df.index = pd.to_datetime(df['Timestamp'], errors='coerce')
            df.index = df.index.tz_localize(None)

            for col in ['Open', 'High', 'Low', 'Close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')

            return df.sort_index()

        except Exception as e:
            msg = str(e).lower()
            if 'rate limit' in msg or 'rate_limit' in msg or '429' in msg:
                wait = delay * (2 ** attempt)
                print(f"      ⏳ Rate limited — waiting {wait:.0f}s")
                time.sleep(wait)
            else:
                return pd.DataFrame()

    return pd.DataFrame()


def _price_at_ts(opt_data: pd.DataFrame,
                  target_ts,
                  side: str = 'close') -> float | None:
    """
    Get the open or close price at or nearest to target_ts.
    Falls back to the nearest candle within a 30-minute window.
    """
    if opt_data.empty:
        return None

    target = pd.Timestamp(target_ts)
    col    = 'Open' if side == 'open' else 'Close'

    if col not in opt_data.columns:
        return None

    # Exact match first
    if target in opt_data.index:
        val = opt_data.at[target, col]
        return float(val) if pd.notna(val) else None

    # Nearest candle within ±30 min
    diff = (opt_data.index - target).total_seconds().abs()
    nearest_idx = diff.argmin()
    if diff.iloc[nearest_idx] <= 1800:  # 30 minutes
        val = opt_data.iloc[nearest_idx][col]
        return float(val) if pd.notna(val) else None

    return None


def print_options_comparison(trade_log: pd.DataFrame) -> None:
    """
    Print a side-by-side comparison of futures P&L vs options P&L.
    """
    if 'opt_pnl_rs' not in trade_log.columns:
        print("No options data. Run add_options_simulation() first.")
        return

    has_opt = trade_log['opt_pnl_rs'].notna()
    df = trade_log[has_opt].copy()

    if df.empty:
        print("No trades with options data.")
        return

    print(f"\n{'='*65}")
    print(f"  FUTURES vs OPTIONS — SIDE-BY-SIDE")
    print(f"{'='*65}")
    print(f"  {'Metric':<25} {'Futures':>15} {'Options (ATM)':>15}")
    print(f"  {'-'*55}")

    fut_total = df['pnl_rs'].sum()
    opt_total = df['opt_pnl_rs'].sum()
    fut_wr    = (df['pnl_rs'] > 0).mean() * 100
    opt_wr    = (df['opt_pnl_rs'] > 0).mean() * 100
    fut_avg   = df['pnl_rs'].mean()
    opt_avg   = df['opt_pnl_rs'].mean()

    print(f"  {'Trades':<25} {len(df):>15} {len(df):>15}")
    print(f"  {'Win Rate':<25} {fut_wr:>14.1f}% {opt_wr:>14.1f}%")
    print(f"  {'Total P&L':<25} ₹{fut_total:>13,.0f} ₹{opt_total:>13,.0f}")
    print(f"  {'Avg P&L / trade':<25} ₹{fut_avg:>13,.0f} ₹{opt_avg:>13,.0f}")

    if 'year' in df.columns:
        print(f"\n  Year-by-year:")
        print(f"  {'Year':<6} {'Fut P&L':>12} {'Opt P&L':>12} {'Opt/Fut':>8}")
        print(f"  {'-'*42}")
        for yr in sorted(df['year'].unique()):
            y = df[df['year'] == yr]
            fp = y['pnl_rs'].sum()
            op = y['opt_pnl_rs'].sum()
            ratio = op / fp if fp != 0 else float('nan')
            print(f"  {yr:<6} ₹{fp:>10,.0f} ₹{op:>10,.0f} {ratio:>7.2f}x")
