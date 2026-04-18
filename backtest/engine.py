# ============================================================
# backtest/engine.py — Master Backtest Orchestrator
# ============================================================
# Single entry point for running any registered strategy.
# Handles: data fetch → strategy run → macro filter application.
#
# Usage:
#   from backtest.engine import run_backtest
#   trade_log = run_backtest('gap_fill', 'BANKNIFTY',
#                            '2022-01-01', '2024-12-31', groww=groww)
# ============================================================

import importlib
import pandas as pd


def run_backtest(strategy_name: str,
                 instrument: str,
                 start_date: str,
                 end_date: str,
                 groww=None,
                 apply_macros: bool = True,
                 fii_data: pd.DataFrame = None,
                 regime_df=None,
                 use_futures: bool = False) -> pd.DataFrame:
    """
    Master backtest entry point. Orchestrates:
      1. Data fetch for the instrument (spot or rolling futures)
      2. Strategy execution (plugin lookup via config.STRATEGIES)
      3. Contract metadata attachment (if use_futures=True)
      4. Macro filter application (optional)

    Args:
        strategy_name : Key in config.STRATEGIES, e.g. 'gap_fill', 'orb', 'vwap_reversion'
        instrument    : Key in config.INSTRUMENTS, e.g. 'BANKNIFTY', 'NIFTY'
        start_date    : 'YYYY-MM-DD'
        end_date      : 'YYYY-MM-DD'
        groww         : Authenticated GrowwAPI instance (required for data fetch)
        apply_macros  : If True, fetch VIX/S&P/FPI and apply macro filters
        fii_data      : Pre-fetched FII DataFrame (optional, used when apply_macros=True)
        regime_df     : Pre-fetched regime DataFrame (optional, passed to ORB/VWAP)
        use_futures   : If True, fetch rolling near-month futures instead of spot.
                        Adds contract, expiry_date, oi_at_entry columns to trade_log.

    Returns:
        pd.DataFrame: trade_log with standard schema + macro_ok column
                      (+ contract/expiry/oi columns when use_futures=True)
    """
    from config import INSTRUMENTS, STRATEGIES, MACRO, CAPITAL as CAPITAL_CFG
    from data.fetch import fetch_instrument

    # ── Validate inputs ───────────────────────────────────────────────────────
    if strategy_name not in STRATEGIES:
        raise ValueError(
            f"Unknown strategy '{strategy_name}'. "
            f"Registered: {list(STRATEGIES.keys())}"
        )
    if instrument not in INSTRUMENTS:
        raise ValueError(
            f"Unknown instrument '{instrument}'. "
            f"Registered: {list(INSTRUMENTS.keys())}"
        )

    instrument_config = INSTRUMENTS[instrument]
    strategy_cfg      = STRATEGIES[strategy_name]
    strategy_params   = strategy_cfg['params']

    # ── Step 1: Fetch data ────────────────────────────────────────────────────
    mode = "FUTURES" if use_futures else "SPOT"
    print(f"\n── Step 1: Fetch {instrument} {mode} data ──")
    data = fetch_instrument(instrument, start_date, end_date,
                            groww=groww, use_futures=use_futures)
    if data.empty:
        print("❌ No data — aborting backtest")
        return pd.DataFrame()

    # ── Step 2: Run strategy ──────────────────────────────────────────────────
    print(f"\n── Step 2: Run {strategy_name} strategy ──")
    module   = importlib.import_module(strategy_cfg['module'])
    run_fn   = getattr(module, strategy_cfg['function'])

    # Route kwargs by strategy type
    if strategy_name == 'gap_fill':
        trade_log = run_fn(data, instrument_config, strategy_params)
    elif strategy_name == 'orb':
        trade_log = run_fn(data, instrument_config, strategy_params,
                           regime_df=regime_df)
    elif strategy_name == 'vwap_reversion':
        trade_log = run_fn(data, instrument_config, strategy_params,
                           regime_df=regime_df)
    else:
        # Generic fallback — pass data, instrument_config, strategy_params
        trade_log = run_fn(data, instrument_config, strategy_params)

    if trade_log.empty:
        print(f"  ⚠ No trades found for {strategy_name} on {instrument}")
        return trade_log

    print(f"  ✅ {len(trade_log)} trades found")

    # ── Step 2b: Attach contract metadata (futures mode only) ─────────────────
    if use_futures and 'Contract' in data.columns:
        print(f"\n── Step 2b: Attach futures contract metadata ──")
        trade_log = _attach_contract_metadata(
            trade_log, data,
            instrument_config = instrument_config,
            capital           = CAPITAL_CFG.get('starting', 1_00_000),
        )

    # ── Step 3: Apply macro filters ───────────────────────────────────────────
    if apply_macros:
        print(f"\n── Step 3: Apply macro filters ──")
        from macro.filters import (fetch_india_vix, fetch_sp500_returns,
                                    apply_macro_filters)

        india_vix = fetch_india_vix(start_date, end_date)
        sp_ret    = fetch_sp500_returns(start_date, end_date)

        trade_log = apply_macro_filters(
            trade_log,
            india_vix    = india_vix,
            sp_ret       = sp_ret,
            fii_data     = fii_data,
            vix_threshold= MACRO['vix_threshold'],
            sp_threshold = MACRO['sp_threshold'],
            fpi_threshold= MACRO['fpi_threshold'],
            min_filters  = MACRO['min_filters'],
        )
        # Always overwrite macro_ok from trade_ok (strategies pre-set macro_ok=True
        # which would otherwise shadow the filter result via the rename guard)
        if 'trade_ok' in trade_log.columns:
            trade_log['macro_ok'] = trade_log['trade_ok']
        elif 'macro_ok' not in trade_log.columns:
            trade_log['macro_ok'] = True

        blocked = (~trade_log['macro_ok']).sum()
        print(f"  Macro gate: {blocked}/{len(trade_log)} trades blocked")
    else:
        if 'macro_ok' not in trade_log.columns:
            trade_log['macro_ok'] = True

    return trade_log


def _lot_size_for_date(trade_date, instrument_config: dict) -> int:
    """
    Return the correct lot size for a given trade date, using lot_size_history
    if present in the instrument config.

    BankNifty changed from 25 → 15 lots on 20-Nov-2023 per SEBI revision.
    Without this, P&L for pre-Nov-2023 trades would be understated.
    """
    from datetime import date as date_type
    history = instrument_config.get('lot_size_history')
    if not history:
        return instrument_config.get('lot_size', 15)

    if isinstance(trade_date, date_type):
        d = trade_date
    else:
        d = pd.Timestamp(trade_date).date()

    for start_str, end_str, size in history:
        start = pd.Timestamp(start_str).date()
        end   = pd.Timestamp(end_str).date()
        if start <= d <= end:
            return size

    return instrument_config.get('lot_size', 15)  # fallback to current


def _attach_contract_metadata(trade_log: pd.DataFrame,
                               futures_data: pd.DataFrame,
                               instrument_config: dict = None,
                               capital: float = 1_00_000) -> pd.DataFrame:
    """
    For each trade, look up which futures contract was active on that trade date
    from the futures_data DataFrame (which has Contract and Expiry columns per candle).

    Adds to trade_log:
        contract      (str)   e.g. 'NSE-BANKNIFTY-28Apr22-FUT'
        expiry_date   (date)  expiry date of that contract
        oi_at_entry   (float) open interest at the entry candle (or 0 if unavailable)
        lot_size      (int)   contracts per lot on that trade date (time-adjusted)
        lots_traded   (int)   number of lots traded (from capital / margin_per_lot)
        pnl_rs        (float) recalculated using correct lot_size × lots_traded
    """
    # Build date → (contract, expiry) lookup from the futures data
    date_to_contract = {}
    date_to_expiry   = {}
    for ts, row in futures_data[['Contract', 'Expiry']].iterrows():
        d = ts.date()
        if d not in date_to_contract:
            date_to_contract[d] = row['Contract']
            date_to_expiry[d]   = row['Expiry']

    # Build entry_ts → OI lookup
    oi_lookup = {}
    if 'Oi' in futures_data.columns:
        for ts, oi_val in futures_data['Oi'].items():
            oi_lookup[ts] = oi_val

    trade_log = trade_log.copy()
    trade_log['contract']    = trade_log['date'].map(date_to_contract).fillna('')
    trade_log['expiry_date'] = trade_log['date'].map(date_to_expiry)

    # OI at entry timestamp
    if 'entry_ts' in trade_log.columns:
        trade_log['oi_at_entry'] = trade_log['entry_ts'].map(oi_lookup).fillna(0)
    else:
        trade_log['oi_at_entry'] = 0

    # Lot size (time-adjusted) and lots traded
    if instrument_config is not None:
        margin     = instrument_config.get('margin_per_lot', 75_000)
        brokerage  = instrument_config.get('brokerage', 40)
        max_pct    = 0.90
        deployable = capital * max_pct

        def _enrich(row):
            ls  = _lot_size_for_date(row['date'], instrument_config)
            lt  = max(int(deployable // margin), 1)
            pnl = round(row.get('pnl_pts', 0) * ls * lt - brokerage, 2) \
                  if row.get('pnl_pts') is not None else row.get('pnl_rs', 0)
            return pd.Series({'lot_size': ls, 'lots_traded': lt, 'pnl_rs': pnl})

        enriched = trade_log.apply(_enrich, axis=1)
        trade_log['lot_size']    = enriched['lot_size']
        trade_log['lots_traded'] = enriched['lots_traded']
        trade_log['pnl_rs']      = enriched['pnl_rs']
        trade_log['win']         = (trade_log['pnl_rs'] > 0).astype(int)
    else:
        trade_log['lot_size']    = 15
        trade_log['lots_traded'] = 1

    n_matched = (trade_log['contract'] != '').sum()
    print(f"  Contract metadata attached: {n_matched}/{len(trade_log)} trades matched")
    if instrument_config is not None:
        ls_counts = trade_log.groupby('lot_size').size()
        for ls, cnt in ls_counts.items():
            print(f"    lot_size={ls}: {cnt} trades")

    return trade_log


def print_strategy_report(trade_log: pd.DataFrame,
                           strategy_name: str = '') -> None:
    """
    Print a concise single-strategy backtest report.
    Works for any strategy output following the standard schema.
    """
    if trade_log.empty:
        print("No trades to report.")
        return

    title = strategy_name or trade_log['strategy'].iloc[0]
    print(f"\n{'='*55}")
    print(f"  {title} BACKTEST REPORT")
    print(f"{'='*55}")
    print(f"  Total trades  : {len(trade_log)}")
    print(f"  Win rate      : {trade_log['win'].mean()*100:.1f}%")
    print(f"  Total P&L     : ₹{trade_log['pnl_rs'].sum():,.0f}")
    print(f"  Avg P&L/trade : ₹{trade_log['pnl_rs'].mean():,.0f}")
    print(f"  Avg bias score: {trade_log['bias_score'].mean():.3f}")

    if 'macro_ok' in trade_log.columns:
        filtered = trade_log[trade_log['macro_ok']]
        print(f"\n  After macro filter ({len(filtered)} trades):")
        print(f"    Win rate : {filtered['win'].mean()*100:.1f}%")
        print(f"    Total P&L: ₹{filtered['pnl_rs'].sum():,.0f}")

    print(f"\n  Year-by-year:")
    print(f"  {'Year':<6} {'Trades':>7} {'WinRate':>8} {'TotalP&L':>12} {'Avg/Trade':>10}")
    print(f"  {'-'*46}")
    for yr in sorted(trade_log['year'].unique()):
        y  = trade_log[trade_log['year'] == yr]
        wr = y['win'].mean() * 100
        pl = y['pnl_rs'].sum()
        ap = y['pnl_rs'].mean()
        print(f"  {yr:<6} {len(y):>7}  {wr:>7.1f}%  ₹{pl:>10,.0f}  ₹{ap:>8,.0f}")

    print(f"\n  Exit breakdown:")
    print(trade_log['exit_reason'].value_counts().to_string())
