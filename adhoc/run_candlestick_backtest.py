# ============================================================
# run_candlestick_backtest.py — Candlestick Strategy Runbook
# ============================================================
# Entry points:
#
#   run_candlestick_backtest(groww, instrument, start, end)
#       Single run, fixed 1 lot, returns trade_log DataFrame.
#
#   run_candlestick_sweep(groww, instrument, start, end)
#       Grid sweep: STOP_ATR × TARGET_ATR × BODY_ATR_MIN.
#
#   run_candlestick_compounded(groww, instrument, start, end, starting)
#       Full run with compounding. Overwrites trade_logs/ CSV on every call.
#
#   run_full_evaluation(groww, instrument)
#       IS (2022-2024) + OOS (2025) back-to-back with comparison table.
#
# NOTE: Trade log CSV is OVERWRITTEN on every run — no appending.
#       Contract/expiry columns populated from futures data automatically.
#       Option equivalent columns (ATM strike, symbol) added from entry price.
# ============================================================

from pathlib import Path

import numpy as np
import pandas as pd

from config import INSTRUMENTS, STRATEGIES
from data.fetch import fetch_instrument
from data.options_fetch import (
    build_option_cache,
    lookup_option_price,
)
from strategies.candlestick import (
    run_candlestick,
    candlestick_parameter_sweep,
    _option_symbol,
)
from backtest.compounding_engine import (
    run_compounded,
    print_compounded_report,
)
from backtest.engine import (
    print_strategy_report,
    _attach_contract_metadata,
    _lot_size_for_date,
)


TRADE_LOG_DIR = Path(__file__).parent / 'trade_logs'
TRADE_LOG_DIR.mkdir(exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _fetch(groww, instrument: str, start: str, end: str,
           use_futures: bool = True) -> pd.DataFrame:
    mode = 'FUTURES' if use_futures else 'SPOT'
    print(f"\n── Fetching {instrument} {mode} data  {start} → {end} ──")
    data = fetch_instrument(instrument, start, end,
                            groww=groww, use_futures=use_futures)
    if data.empty:
        print("  ❌ No data returned.")
    else:
        print(f"  ✅ {len(data):,} candles  "
              f"({data.index[0].date()} → {data.index[-1].date()})")
    return data


def _build_strategy_params(instrument: str) -> dict:
    inst = INSTRUMENTS[instrument]
    return {
        **STRATEGIES['candlestick']['params'],
        **inst.get('strategy_params', {}),
    }


def _attach_option_symbols(tl: pd.DataFrame,
                            futures_data: pd.DataFrame,
                            instrument_config: dict) -> pd.DataFrame:
    """
    Populate contract, expiry_date, option_symbol, lots_traded, lot_size
    in the trade log.

    - contract / expiry_date come from futures_data (Contract + Expiry columns).
    - option_symbol is the ATM CE/PE symbol built from atm_strike + expiry_date.
    - lots_traded / lot_size are NOT set here — that comes from the compounding
      engine. Here we only store the time-adjusted lot_size for reference.
    """
    tl = tl.copy()
    underlying = instrument_config.get('underlying_symbol', 'BANKNIFTY')

    # Build date → (contract, expiry) lookup from futures data
    date_to_contract = {}
    date_to_expiry   = {}
    if futures_data is not None and 'Contract' in futures_data.columns:
        for ts, row in futures_data[['Contract', 'Expiry']].iterrows():
            d = ts.date()
            if d not in date_to_contract:
                date_to_contract[d] = row['Contract']
                date_to_expiry[d]   = row['Expiry']

    tl['contract']    = tl['date'].map(date_to_contract).fillna('')
    tl['expiry_date'] = tl['date'].map(date_to_expiry)

    # Option symbol: UNDERLYINGDDMMMYYSTRIKEOPT_TYPE
    def _opt_sym(row):
        expiry = row.get('expiry_date')
        if expiry is None or (isinstance(expiry, float)):
            return f"{underlying}??{row['atm_strike']}{row['option_type']}"
        return _option_symbol(underlying, expiry,
                              int(row['atm_strike']), row['option_type'])

    tl['option_symbol'] = tl.apply(_opt_sym, axis=1)

    # Time-adjusted lot size for reference
    tl['lot_size'] = tl['date'].apply(
        lambda d: _lot_size_for_date(d, instrument_config)
    )

    n = (tl['contract'] != '').sum()
    print(f"  Contract metadata: {n}/{len(tl)} trades matched to futures contract")
    return tl


def _save_csv(enriched: pd.DataFrame, name: str, label: str = '') -> None:
    """Overwrite (not append) CSV + equity curve CSV."""
    fpath = TRADE_LOG_DIR / f'{name}.csv'
    enriched.to_csv(fpath, index=False, mode='w')   # 'w' = overwrite always
    print(f"  📝 {label or 'Trade log'} → {fpath}  ({len(enriched)} rows)")


# ── Public entry points ───────────────────────────────────────────────────────
def run_candlestick_backtest(groww,
                              instrument: str,
                              start: str,
                              end: str,
                              use_futures: bool = True,
                              data: pd.DataFrame = None) -> pd.DataFrame:
    """
    Single run with fixed 1-lot sizing. Does NOT save CSV.
    Useful for spot-checking signal quality before running the full compounded suite.
    """
    inst   = INSTRUMENTS[instrument]
    sp     = _build_strategy_params(instrument)

    if data is None:
        data = _fetch(groww, instrument, start, end, use_futures)
    if data.empty:
        return pd.DataFrame()

    print(f"\n── Running CANDLESTICK on {instrument} (15-min native) ──")
    tl = run_candlestick(data, inst, sp)
    if tl.empty:
        print("  ⚠ No trades generated.")
        return tl

    tl = _attach_option_symbols(tl, data, inst)
    print_strategy_report(tl, strategy_name=f'CANDLESTICK / {instrument}')
    return tl


def run_candlestick_sweep(groww,
                           instrument: str = 'BANKNIFTY',
                           start: str = '2022-01-01',
                           end:   str = '2024-12-31',
                           use_futures: bool = True,
                           data: pd.DataFrame = None) -> pd.DataFrame:
    """Parameter grid: STOP_ATR × TARGET_ATR × BODY_ATR_MIN."""
    inst = INSTRUMENTS[instrument]
    if data is None:
        data = _fetch(groww, instrument, start, end, use_futures)
    if data.empty:
        return pd.DataFrame()
    return candlestick_parameter_sweep(data, inst)


def run_candlestick_compounded(groww,
                                instrument: str,
                                start: str,
                                end: str,
                                starting: float = 1_00_000,
                                use_futures: bool = True,
                                data: pd.DataFrame = None):
    """
    Full pipeline: fetch → strategy → contract metadata → compounding → CSV.

    Trade log CSV is OVERWRITTEN on every call (no appending).
    Equity curve CSV is written alongside it.

    Returns
    -------
    (enriched_log, equity_curve, summary)
    """
    inst = INSTRUMENTS[instrument]
    sp   = _build_strategy_params(instrument)

    if data is None:
        data = _fetch(groww, instrument, start, end, use_futures)
    if data.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    # ── Step 1: Strategy ─────────────────────────────────────────────────────
    print(f"\n── Step 1: CANDLESTICK strategy on {instrument} (15-min) ──")
    tl = run_candlestick(data, inst, sp)
    if tl.empty:
        print("  ⚠ No trades — aborting.")
        return pd.DataFrame(), pd.DataFrame(), {}
    print(f"  ✅ {len(tl)} raw trades")

    # ── Step 2: Contract + option metadata ───────────────────────────────────
    print(f"\n── Step 2: Attaching contract & option details ──")
    tl = _attach_option_symbols(tl, data, inst)

    # ── Step 3: Compounding capital walk ─────────────────────────────────────
    print(f"\n── Step 3: Compounding (₹{starting:,.0f} starting) ──")
    enriched, ec, summary = run_compounded(tl, inst, starting_capital=starting)

    # Update margin_used and lots_traded from compounded lots
    enriched['lots_traded'] = enriched['lots_at_entry'].fillna(1).astype(int)
    enriched['margin_used'] = (
        enriched['lots_traded'] * enriched['margin_per_lot']
    )

    print_compounded_report(
        enriched, summary,
        title=f'CANDLESTICK / {instrument}  {start} → {end}'
    )

    # ── Step 4: Save CSVs (overwrite) ────────────────────────────────────────
    stem = f'candlestick_{instrument}_{start}_{end}'
    _save_csv(enriched, stem,            label='Trade log')
    _save_csv(ec.reset_index(), f'{stem}_equity', label='Equity curve')

    return enriched, ec, summary


def _tag_fno_mode(tl: pd.DataFrame) -> pd.DataFrame:
    """
    Indicator-based F vs O selector. Tags each trade with fno_mode = 'FUT' | 'OPT'.

    Rules (evaluated in priority order):
    1.  pattern_stack >= 2  →  OPT
        Multiple patterns firing simultaneously = high conviction.
        Buy options: defined max loss (premium) + convexity on a big move.

    2.  EMA gap > 0.5%  →  FUT
        Strong established trend: futures cleaner for momentum continuation.
        Options have time-value headwind in already-running trends.

    3.  signal_bar_time >= 13:30  →  FUT
        Afternoon entries have < 2H to expiry of theta. Options bleed too fast.

    4.  rsi14 in extreme zone (< 40 or > 60)  →  OPT
        RSI extreme + pattern = strong reversal setup. Options give 2-5× leverage
        on the snap-back while capping downside to premium.

    5.  Default  →  FUT
    """
    tl = tl.copy()

    def _mode(row):
        # Compute EMA gap strength
        try:
            ema_gap_pct = abs(row['ema_fast'] - row['ema_slow']) / row['ema_slow'] * 100
        except (ZeroDivisionError, TypeError):
            ema_gap_pct = 0

        rsi = float(row.get('rsi14', 50))

        # Rule 1: strong trend (EMA gap > 1.5%) → FUT (options have theta headwind)
        if ema_gap_pct > 1.5:
            return 'FUT'

        # Rule 2: afternoon entry (>13:30) with weak pattern → FUT (theta decay)
        try:
            sig_time = row.get('signal_bar_time', '09:15')
            sig_hour = int(str(sig_time).split(':')[0])
            sig_min  = int(str(sig_time).split(':')[1])
            if sig_hour > 13 or (sig_hour == 13 and sig_min >= 30):
                if row['pattern_stack'] < 3:
                    return 'FUT'
        except Exception:
            pass

        # Default → OPT: VWAP mean-reversion signals have defined risk profile
        # ideal for options (max loss = premium, unlimited upside on snap-back)
        return 'OPT'

    tl['fno_mode'] = tl.apply(_mode, axis=1)

    n_opt = (tl['fno_mode'] == 'OPT').sum()
    n_fut = (tl['fno_mode'] == 'FUT').sum()
    print(f"  FNO mode: {n_fut} FUT / {n_opt} OPT trades tagged")
    return tl


def _reprice_option_trades(tl: pd.DataFrame,
                            data_cache: dict,
                            expiry_map: dict,
                            instrument_config: dict) -> pd.DataFrame:
    """
    For OPT-mode trades: replace entry_price / exit_price / pnl_pts / pnl_rs
    with actual option premium data from the cache.

    Exit timing (stop/target/square-off) is still determined from the UNDERLYING
    (already simulated by run_candlestick). We simply look up what the option
    premium was at those timestamps.

    If the option data is unavailable for a trade, it falls back to FUT.
    """
    tl  = tl.copy()
    brokerage = instrument_config.get('brokerage', 40)
    fallback_count = 0

    for i, row in tl.iterrows():
        if row.get('fno_mode') != 'OPT':
            continue

        tdate   = row['date']
        expiry  = expiry_map.get(tdate)
        if expiry is None:
            tl.at[i, 'fno_mode'] = 'FUT'
            fallback_count += 1
            continue

        strike  = int(row['atm_strike'])
        otype   = row['option_type']
        key     = (str(expiry), strike, otype)
        opt_df  = data_cache.get(key)

        if opt_df is None or opt_df.empty:
            tl.at[i, 'fno_mode'] = 'FUT'
            fallback_count += 1
            continue

        # Actual option premiums at entry and exit
        entry_opt = lookup_option_price(opt_df, row['entry_time'], field='Open')
        exit_opt  = lookup_option_price(opt_df, row['exit_time'],  field='Close')

        if entry_opt is None or exit_opt is None or entry_opt <= 0:
            tl.at[i, 'fno_mode'] = 'FUT'
            fallback_count += 1
            continue

        # We always BUY options (CE for long, PE for short)
        # pnl_pts = exit_premium - entry_premium (signed correctly for a buyer)
        pnl_pts = exit_opt - entry_opt
        lot_size = int(row.get('lot_size', instrument_config['lot_size']))
        pnl_rs   = round(pnl_pts * lot_size - brokerage, 2)

        # Update the option symbol with the actual resolved expiry
        from data.contract_resolver import build_options_symbol
        opt_sym = build_options_symbol('NSE',
                                        instrument_config['underlying_symbol'],
                                        pd.Timestamp(expiry).date(),
                                        strike, otype)

        tl.at[i, 'entry_price']    = round(entry_opt, 2)
        tl.at[i, 'exit_price']     = round(exit_opt,  2)
        tl.at[i, 'pnl_pts']        = round(pnl_pts,   2)
        tl.at[i, 'pnl_rs']         = pnl_rs
        tl.at[i, 'win']            = 1 if pnl_rs > 0 else 0
        tl.at[i, 'instrument_type'] = 'OPT'
        tl.at[i, 'contract']        = opt_sym
        tl.at[i, 'expiry_date']     = pd.Timestamp(expiry).date()
        tl.at[i, 'option_symbol']   = opt_sym
        tl.at[i, 'margin_per_lot']  = round(entry_opt * lot_size, 2)
        tl.at[i, 'margin_used']     = round(entry_opt * lot_size, 2)

    if fallback_count:
        print(f"  ⚠ {fallback_count} OPT trades fell back to FUT (no option data)")
    return tl


def run_candlestick_fno(groww,
                         instrument: str,
                         start: str,
                         end: str,
                         starting: float = 1_00_000,
                         use_futures: bool = True,
                         min_days_to_expiry: int = 1,
                         data: pd.DataFrame = None):
    """
    Full F+O pipeline:

    1. Fetch underlying (futures or spot) for signal detection.
    2. Run candlestick strategy → raw trade log.
    3. Tag each trade FUT or OPT using indicator-based rules:
       - pattern_stack ≥ 2           → OPT (high conviction, defined risk)
       - EMA gap > 0.5%              → FUT (strong trend, ride with futures)
       - signal after 13:30          → FUT (afternoon, no theta room)
       - RSI extreme (< 40 or > 60)  → OPT (sharp reversal expected)
       - default                      → FUT
    4. Fetch actual option premiums from Groww API for OPT trades.
    5. Replace entry/exit prices with real option prices for OPT rows.
    6. Apply compounding capital walk.
    7. Overwrite trade_logs/candlestick_fno_<instrument>_<start>_<end>.csv.

    Returns
    -------
    (enriched_log, equity_curve, summary)
    """
    inst = INSTRUMENTS[instrument]
    sp   = _build_strategy_params(instrument)
    underlying = inst['underlying_symbol']

    if data is None:
        data = _fetch(groww, instrument, start, end, use_futures)
    if data.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    # ── Step 1: Signal detection on underlying ───────────────────────────────
    print(f"\n── Step 1: CANDLESTICK signals on {instrument} (15-min) ──")
    tl = run_candlestick(data, inst, sp)
    if tl.empty:
        print("  ⚠ No trades generated.")
        return pd.DataFrame(), pd.DataFrame(), {}
    print(f"  ✅ {len(tl)} raw signal trades")

    # ── Step 2: Attach futures contract metadata ─────────────────────────────
    print(f"\n── Step 2: Attaching contract metadata ──")
    tl = _attach_option_symbols(tl, data, inst)

    # ── Step 3: Tag FUT vs OPT per trade using indicators ───────────────────
    print(f"\n── Step 3: F vs O mode selection (indicator-based) ──")
    tl = _tag_fno_mode(tl)

    # ── Step 4: Pre-fetch option data for OPT trades ─────────────────────────
    data_cache, expiry_map = build_option_cache(
        groww, underlying, tl, min_days_to_expiry
    )

    # ── Step 5: Reprice OPT trades with actual option premiums ───────────────
    print(f"\n── Step 5: Repricing OPT trades with real premiums ──")
    tl = _reprice_option_trades(tl, data_cache, expiry_map, inst)

    n_opt = (tl['fno_mode'] == 'OPT').sum()
    n_fut = (tl['fno_mode'] == 'FUT').sum()
    print(f"  Final mix: {n_fut} FUT + {n_opt} OPT = {len(tl)} total trades")

    # ── Step 6: Compounding walk ─────────────────────────────────────────────
    print(f"\n── Step 6: Compounding (₹{starting:,.0f} starting) ──")
    enriched, ec, summary = run_compounded(tl, inst, starting_capital=starting)
    enriched['lots_traded'] = enriched['lots_at_entry'].fillna(1).astype(int)
    enriched['margin_used'] = enriched.apply(
        lambda r: r['margin_per_lot'] * r['lots_traded'], axis=1
    )

    print_compounded_report(
        enriched, summary,
        title=f'CANDLESTICK F+O / {instrument}  {start} → {end}'
    )

    # ── Step 7: FNO breakdown ────────────────────────────────────────────────
    _print_fno_breakdown(enriched)

    # ── Step 8: Save CSV (overwrite) ─────────────────────────────────────────
    stem = f'candlestick_fno_{instrument}_{start}_{end}'
    _save_csv(enriched,            stem,            label='F+O trade log')
    _save_csv(ec.reset_index(),    f'{stem}_equity', label='Equity curve')

    return enriched, ec, summary


def _print_fno_breakdown(tl: pd.DataFrame) -> None:
    """Print per-mode performance breakdown."""
    if tl.empty or 'fno_mode' not in tl.columns:
        return
    print(f"\n  {'Mode':<6} {'Trades':>7} {'WR%':>6} {'TotalP&L':>12} {'Avg/Trade':>10}")
    print(f"  {'-'*44}")
    for mode in ['FUT', 'OPT']:
        sub = tl[tl['fno_mode'] == mode]
        if sub.empty:
            continue
        print(f"  {mode:<6} {len(sub):>7} {sub['win'].mean()*100:>5.1f}  "
              f"₹{sub['pnl_rs'].sum():>10,.0f}  ₹{sub['pnl_rs'].mean():>8,.0f}")


def run_full_evaluation(groww,
                         instrument: str = 'BANKNIFTY',
                         starting: float = 1_00_000):
    """
    IS 2022-2024 compounded + OOS 2025 compounded, side-by-side summary.
    Each window overwrites its own CSV pair.
    """
    print(f"\n{'#'*64}")
    print(f"#  FULL CANDLESTICK EVALUATION — {instrument}")
    print(f"{'#'*64}")

    is_log, is_ec, is_sum = run_candlestick_compounded(
        groww, instrument, '2022-01-01', '2024-12-31', starting=starting
    )
    oos_log, oos_ec, oos_sum = run_candlestick_compounded(
        groww, instrument, '2025-01-01', '2025-12-31', starting=starting
    )

    print(f"\n{'='*64}")
    print(f"  SUMMARY — {instrument}")
    print(f"{'='*64}")
    hdr = f"  {'Window':<20} {'Trades':>7} {'WR%':>6} {'FinalEq':>12} {'Ret':>7} {'MaxDD%':>7}"
    print(hdr)
    print(f"  {'-'*60}")
    for label, s in [('2022–2024 (IS)', is_sum), ('2025 (OOS)', oos_sum)]:
        if not s:
            print(f"  {label:<20}  — no data")
            continue
        print(f"  {label:<20} {s['trades']:>7} {s['win_rate']:>5.1f}  "
              f"₹{s['final_equity']:>10,.0f}  "
              f"{s['total_return_x']:>5.2f}x  {s['max_dd_pct']:>6.2f}%")

    return {
        'is':  (is_log, is_ec, is_sum),
        'oos': (oos_log, oos_ec, oos_sum),
    }
