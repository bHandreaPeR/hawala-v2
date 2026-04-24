# ============================================================
# backtest/walk_forward.py — Rolling Walk-Forward Validator
# ============================================================
# Tests whether calibrated strategy params survive rolling OOS
# windows, revealing parameter stability across regimes.
#
# Usage:
#   python backtest/walk_forward.py
#
# Reports per-window OOS Sharpe and a stability score
# (% of OOS windows with positive Sharpe).
# ============================================================

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import math
import pickle
import numpy as np
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

from config import INSTRUMENTS, STRATEGIES
from strategies.orb import run_orb


# ── Sharpe helper ─────────────────────────────────────────────────────────────

def sharpe(pnl_series: pd.Series, periods_per_year: int = 252) -> float:
    if len(pnl_series) < 3 or pnl_series.std() == 0:
        return 0.0
    return (pnl_series.mean() / pnl_series.std()) * math.sqrt(periods_per_year)


# ── Walk-forward engine ───────────────────────────────────────────────────────

def walk_forward(strategy_fn,
                 data: pd.DataFrame,
                 inst_cfg: dict,
                 strategy_params: dict,
                 train_months: int = 6,
                 oos_months: int   = 1,
                 label: str        = 'Strategy') -> pd.DataFrame:
    """
    Roll a train/OOS window across data, measuring OOS Sharpe each step.

    Parameters
    ----------
    strategy_fn     : function with signature (data, inst_cfg, params) → trades_df
    data            : full OHLCV DataFrame
    inst_cfg        : instrument config dict
    strategy_params : default params (used for both train and OOS — fixed params mode)
    train_months    : rolling training window length (months)
    oos_months      : OOS window length (months)
    label           : display name for reporting

    Returns
    -------
    DataFrame with one row per OOS window: [window, oos_start, oos_end,
    oos_trades, oos_wr, oos_pnl, oos_sharpe]
    """
    dates_all = sorted(set(data.index.date))
    if not dates_all:
        return pd.DataFrame()

    start_date = dates_all[0]
    end_date   = dates_all[-1]

    results = []
    window_n = 0

    # Advance OOS start month by month
    oos_start = pd.Timestamp(start_date) + relativedelta(months=train_months)

    while True:
        oos_end = oos_start + relativedelta(months=oos_months) - relativedelta(days=1)
        if oos_end.date() > end_date:
            break

        train_start = oos_start - relativedelta(months=train_months)

        # Slice data
        oos_data = data[
            (data.index.date >= oos_start.date()) &
            (data.index.date <= oos_end.date())
        ]
        if oos_data.empty:
            oos_start += relativedelta(months=oos_months)
            continue

        # Run strategy on OOS window (fixed params — testing stability, not refitting)
        try:
            trades = strategy_fn(oos_data, inst_cfg, strategy_params)
        except Exception as e:
            trades = pd.DataFrame()

        if trades.empty:
            results.append({
                'window':     window_n,
                'oos_start':  oos_start.date(),
                'oos_end':    oos_end.date(),
                'oos_trades': 0,
                'oos_wr':     np.nan,
                'oos_pnl':    0,
                'oos_sharpe': 0,
            })
        else:
            daily_pnl = trades.groupby('date')['pnl_rs'].sum()
            all_dates = pd.date_range(oos_start, oos_end, freq='B').date
            daily_pnl = daily_pnl.reindex(all_dates, fill_value=0)

            results.append({
                'window':     window_n,
                'oos_start':  oos_start.date(),
                'oos_end':    oos_end.date(),
                'oos_trades': len(trades),
                'oos_wr':     trades['win'].mean() * 100,
                'oos_pnl':    trades['pnl_rs'].sum(),
                'oos_sharpe': round(sharpe(daily_pnl), 3),
            })

        window_n   += 1
        oos_start  += relativedelta(months=oos_months)

    return pd.DataFrame(results)


def print_wf_report(wf_df: pd.DataFrame, label: str = 'Strategy'):
    if wf_df.empty:
        print("  No walk-forward windows generated.")
        return

    pos_sharpe = (wf_df['oos_sharpe'] > 0).sum()
    stability  = pos_sharpe / len(wf_df) * 100
    avg_sharpe = wf_df['oos_sharpe'].mean()
    total_pnl  = wf_df['oos_pnl'].sum()

    print(f"\n{'='*68}")
    print(f"  WALK-FORWARD RESULTS — {label}")
    print(f"{'='*68}")
    print(f"  {'Window':<8} {'OOS Period':<25} {'Trades':>7} {'WR%':>6} "
          f"{'P&L':>10} {'Sharpe':>8}")
    print(f"  {'─'*66}")
    for _, row in wf_df.iterrows():
        wr_str = f"{row['oos_wr']:.1f}%" if not pd.isna(row['oos_wr']) else "   —"
        print(f"  {int(row['window']):<8} "
              f"{str(row['oos_start'])} → {str(row['oos_end'])}  "
              f"{int(row['oos_trades']):>7}  {wr_str:>6}  "
              f"₹{row['oos_pnl']:>8,.0f}  {row['oos_sharpe']:>7.3f}")
    print(f"  {'─'*66}")
    print(f"  Windows         : {len(wf_df)}")
    print(f"  Positive Sharpe : {pos_sharpe}/{len(wf_df)}  ({stability:.0f}% stable)")
    print(f"  Avg OOS Sharpe  : {avg_sharpe:.3f}")
    print(f"  Total OOS P&L   : ₹{total_pnl:,.0f}")
    if stability >= 60 and avg_sharpe > 0.1:
        print(f"  ✅ Strategy STABLE — params survive rolling OOS")
    elif stability >= 40:
        print(f"  ⚠  Strategy MARGINAL — params partially stable")
    else:
        print(f"  ❌ Strategy UNSTABLE — params do not generalize")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN — Run walk-forward on ORB with calibrated params
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import os, sys
    from dotenv import load_dotenv
    load_dotenv('token.env')
    TOKEN       = os.getenv('GROWW_API_KEY', '').strip()
    TOTP_SECRET = os.getenv('GROWW_TOTP_SECRET', '').strip()
    if not TOKEN:
        sys.exit("❌  GROWW_API_KEY not found in token.env")
    if not TOTP_SECRET:
        sys.exit("❌  GROWW_TOTP_SECRET not found in token.env")
    import pyotp
    from growwapi import GrowwAPI
    from data.fetch import fetch_instrument

    access_token = GrowwAPI.get_access_token(api_key=TOKEN, totp=pyotp.TOTP(TOTP_SECRET).now())
    groww    = GrowwAPI(access_token)
    print("✅  Groww authenticated")

    INSTRUMENT = 'BANKNIFTY'
    inst_cfg   = INSTRUMENTS[INSTRUMENT]
    orb_params = {**STRATEGIES['orb']['params'], **inst_cfg.get('strategy_params', {})}

    # Load or fetch full 2024-2025 dataset
    cache_path = f'trade_logs/_data_cache_{INSTRUMENT}_2024-01-01_2025-12-31_wf.pkl'
    if os.path.exists(cache_path):
        print(f"📦  Loading combined cache...")
        with open(cache_path, 'rb') as f:
            data_full = pickle.load(f)
    else:
        print("🌐  Fetching 2024-2025 data for walk-forward...")
        d24 = fetch_instrument(INSTRUMENT, '2024-01-01', '2024-12-31', groww=groww, use_futures=True)
        d25 = fetch_instrument(INSTRUMENT, '2025-01-01', '2025-12-31', groww=groww, use_futures=True)
        data_full = pd.concat([d24, d25]).sort_index()
        data_full = data_full[~data_full.index.duplicated(keep='first')]
        with open(cache_path, 'wb') as f:
            pickle.dump(data_full, f)
        print(f"💾  Cached combined dataset ({len(data_full):,} candles)")

    print(f"\n  Data: {data_full.index[0].date()} → {data_full.index[-1].date()}")
    print(f"  Running walk-forward: 6-month train, 1-month OOS rolling windows")

    wf_df = walk_forward(
        strategy_fn     = run_orb,
        data            = data_full,
        inst_cfg        = inst_cfg,
        strategy_params = orb_params,
        train_months    = 6,
        oos_months      = 1,
        label           = 'ORB / BANKNIFTY (calibrated params)'
    )

    print_wf_report(wf_df, 'ORB / BANKNIFTY')
