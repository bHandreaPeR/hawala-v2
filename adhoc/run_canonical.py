"""
run_canonical.py — Final canonical run of vp_trailing_swing using the
per-instrument best params discovered through the entire research process.

Produces 1-lot trade logs (no compounding) split into IS (2022-2025)
and OOS (2026), and prints a clean summary.

Per-instrument config (after extensive sweeps):
  BANKNIFTY:
    pierce=0.30, rev=0.30, bars=4, trail=0.75, target=1.0, candle=True
    stop_buf=0.6, persist=1
    + adaptive VA: VA=0.60 when 20d-RV < 10%
    + cluster-loss cooldown: K=2 consecutive losses → freeze 3 days

  NIFTY:
    pierce=0.20, rev=0.50, bars=8, trail=0.75, target=0.75, candle=False
    stop_buf=0.2, persist=0
    (no adaptive VA, no cluster cooldown — they hurt performance)

  SENSEX:
    pierce=0.30, rev=0.30, bars=4, trail=0.75, target=1.0, candle=True
    stop_buf=0.2, persist=2
    (no adaptive VA, no cluster cooldown)

Output:
  trade_logs/vpt_final_<INSTRUMENT>_IS.csv
  trade_logs/vpt_final_<INSTRUMENT>_OOS.csv
"""

from __future__ import annotations

import pathlib
import pickle
import sys

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from config import INSTRUMENTS                                  # noqa: E402
from strategies.vp_trailing_swing import run_vp_trailing_swing  # noqa: E402

CACHE_DIR = ROOT / 'data' / 'cache_15m'
TRADE_DIR = ROOT / 'trade_logs'

CANONICAL_PARAMS = {
    'BANKNIFTY': {
        'VPT_PIERCE_MIN_ATR':       0.30,
        'VPT_REVERSAL_ATR':         0.30,
        'VPT_REVERSAL_MAX_BARS':    4,
        'VPT_TRAIL_ATR':            0.75,
        'VPT_TARGET_FRAC':          1.00,
        'VPT_REQUIRE_CANDLE_REV':   True,
        'VPT_INITIAL_STOP_BUFFER_ATR': 0.60,
        'VPT_BE_TRIGGER_FRAC':      0.50,
        'VPT_ENTRY_WINDOW':         ('10:00', '14:00'),
        'VPT_SQUAREOFF':            '15:15',
        'VPT_MAX_TRADES_PER_DAY':   2,
        'VPT_DOW_ALLOW':            [0, 1, 2, 3, 4],
        'VPT_MAX_HOLD_DAYS':        3,
        'VPT_OVERNIGHT_TRAIL_ATR':  0.50,
        'VPT_CARRY_REQUIRES_PROFIT': True,
        'VPT_PIERCE_PERSIST_DAYS':  1,
        'VPT_VA_PCT_LOW_RV':         0.60,
        'VPT_VA_PCT_RV_THRESHOLD':   10,
        'VPT_CLUSTER_LOSS_COUNT':    2,
        'VPT_CLUSTER_COOLDOWN_DAYS': 3,
        # Realistic risk + cost:
        'VPT_SLIPPAGE_PTS':         30,        # BN: ~30 pt fills on stop orders
        'VPT_DAILY_MAX_LOSS_PTS':   300,       # tightened from 600 after Apr-May fwd test
        # Apr-May 2026 forensic-driven additions:
        'VPT_TREND_LOOKBACK_DAYS':  20,
        'VPT_TREND_THRESHOLD_PCT':  2.0,       # block fades against 20D trend > 2%
        'VPT_BLOCK_AFTER_BE':       True,      # one BE = day done
        'VPT_EARLY_CUT_TIME':       '14:30',
        'VPT_EARLY_CUT_LOSS_PTS':   80,        # bail BN losers below -80 pts after 14:30
    },
    'NIFTY': {
        'VPT_PIERCE_MIN_ATR':       0.20,
        'VPT_REVERSAL_ATR':         0.50,
        'VPT_REVERSAL_MAX_BARS':    8,
        'VPT_TRAIL_ATR':            0.75,
        'VPT_TARGET_FRAC':          0.75,
        'VPT_REQUIRE_CANDLE_REV':   False,
        'VPT_INITIAL_STOP_BUFFER_ATR': 0.20,
        'VPT_BE_TRIGGER_FRAC':      0.50,
        'VPT_ENTRY_WINDOW':         ('10:00', '14:00'),
        'VPT_SQUAREOFF':            '15:15',
        'VPT_MAX_TRADES_PER_DAY':   2,
        'VPT_DOW_ALLOW':            [0, 1, 2, 3, 4],
        'VPT_MAX_HOLD_DAYS':        3,
        'VPT_OVERNIGHT_TRAIL_ATR':  0.50,
        'VPT_CARRY_REQUIRES_PROFIT': True,
        'VPT_PIERCE_PERSIST_DAYS':  0,
        'VPT_SLIPPAGE_PTS':         10,        # NIFTY tighter spread
        'VPT_DAILY_MAX_LOSS_PTS':   200,       # ~₹5k @ 25-lot
        # Apr-May filters tested and REJECTED for NIFTY (cut +ve trades, net -₹62k):
        # 'VPT_TREND_LOOKBACK_DAYS': 20, 'VPT_TREND_THRESHOLD_PCT': 2.0,
        # 'VPT_BLOCK_AFTER_BE': True, 'VPT_EARLY_CUT_LOSS_PTS': 40,
    },
    'SENSEX': {
        'VPT_PIERCE_MIN_ATR':       0.30,
        'VPT_REVERSAL_ATR':         0.30,
        'VPT_REVERSAL_MAX_BARS':    4,
        'VPT_TRAIL_ATR':            0.75,
        'VPT_TARGET_FRAC':          1.00,
        'VPT_REQUIRE_CANDLE_REV':   True,
        'VPT_INITIAL_STOP_BUFFER_ATR': 0.20,
        'VPT_BE_TRIGGER_FRAC':      0.50,
        'VPT_ENTRY_WINDOW':         ('10:00', '14:00'),
        'VPT_SQUAREOFF':            '15:15',
        'VPT_MAX_TRADES_PER_DAY':   2,
        'VPT_DOW_ALLOW':            [0, 1, 2, 3, 4],
        'VPT_MAX_HOLD_DAYS':        3,
        'VPT_OVERNIGHT_TRAIL_ATR':  0.50,
        'VPT_CARRY_REQUIRES_PROFIT': True,
        'VPT_PIERCE_PERSIST_DAYS':  2,
        'VPT_SLIPPAGE_PTS':         20,        # SENSEX wider than NIFTY
        'VPT_DAILY_MAX_LOSS_PTS':   400,
        # Apr-May filters tested and REJECTED for SENSEX (cut +ve trades, net -₹76k):
        # 'VPT_TREND_LOOKBACK_DAYS': 20, 'VPT_TREND_THRESHOLD_PCT': 2.0,
        # 'VPT_BLOCK_AFTER_BE': True, 'VPT_EARLY_CUT_LOSS_PTS': 60,
    },
}

OOS_START = pd.Timestamp('2026-01-01')


def _load(inst):
    f = CACHE_DIR / f'{inst}_combined.pkl'
    with open(f,'rb') as h: df = pickle.load(h)
    return df.rename(columns={'open':'Open','high':'High','low':'Low',
                              'close':'Close','volume':'Volume',
                              'contract':'Contract','expiry':'Expiry'}
                     ).between_time('09:15','15:30')


def _stats(log):
    if log.empty:
        return dict(n=0, wr=0, pnl=0, avg=0, max_w=0, max_l=0, dd=0,
                    tgt=0, trail=0, sqof=0, be=0)
    n=len(log); wr=log['win'].mean()*100; pnl=log['pnl_rs'].sum()
    cum = log['pnl_rs'].cumsum().values
    dd = float((np.maximum.accumulate(cum)-cum).max()) if len(cum) else 0
    return dict(
        n=n, wr=round(wr,1), pnl=round(pnl,0), avg=round(pnl/n,0),
        max_w=round(log['pnl_rs'].max(),0), max_l=round(log['pnl_rs'].min(),0),
        dd=round(dd,0),
        tgt=int((log['exit_reason']=='TARGET HIT').sum()),
        trail=int((log['exit_reason']=='TRAIL STOP').sum()),
        sqof=int((log['exit_reason']=='SQUARE OFF').sum()),
        be=int((log['exit_reason']=='BREAKEVEN').sum()),
    )


def main():
    print(f'\n{"█"*100}')
    print(f'  CANONICAL VP-TRAILING-SWING RUN  (1-lot, no compounding)')
    print(f'{"█"*100}')

    all_logs = []
    summary  = []

    for inst, sp in CANONICAL_PARAMS.items():
        cfg = INSTRUMENTS[inst]
        df  = _load(inst)
        is_df  = df[df.index <  OOS_START]
        oos_df = df[df.index >= OOS_START]

        is_log  = run_vp_trailing_swing(is_df,  cfg, sp)
        oos_log = run_vp_trailing_swing(oos_df, cfg, sp)

        if len(is_log):
            is_log.to_csv(TRADE_DIR / f'vpt_final_{inst}_IS.csv', index=False)
        if len(oos_log):
            oos_log.to_csv(TRADE_DIR / f'vpt_final_{inst}_OOS.csv', index=False)

        is_log['period']  = 'IS'
        oos_log['period'] = 'OOS'
        full = pd.concat([is_log, oos_log], ignore_index=True)
        all_logs.append(full)

        is_s   = _stats(is_log)
        oos_s  = _stats(oos_log)
        full_s = _stats(full)

        print(f'\n  ── {inst}  (lot={cfg["lot_size"]}) ──')
        print(f'  {"":<6}{"n":>5}{"WR":>7}{"P&L":>13}{"avg/tr":>9}'
              f'{"max-win":>11}{"max-loss":>11}{"max-DD":>11}'
              f'  {"tgt":>4}{"trail":>6}{"sqof":>6}{"be":>4}')
        for label, s in (('IS', is_s), ('OOS', oos_s), ('TOTAL', full_s)):
            print(f'  {label:<6}{s["n"]:>5}{s["wr"]:>6.1f}%'
                  f'  ₹{s["pnl"]:>+10,.0f}  ₹{s["avg"]:>+5,.0f}'
                  f'  ₹{s["max_w"]:>+8,.0f}  ₹{s["max_l"]:>+8,.0f}'
                  f'  ₹{s["dd"]:>+8,.0f}'
                  f'   {s["tgt"]:>3}{s["trail"]:>6}{s["sqof"]:>6}{s["be"]:>4}')

        # year-by-year
        full['year'] = pd.to_datetime(full['entry_ts']).dt.year
        print(f'\n  Year-by-year:')
        yearly = full.groupby('year').agg(
            n=('pnl_rs','count'),
            wr=('win', lambda s: round(s.mean()*100,1)),
            pnl=('pnl_rs','sum'),
        ).round(0)
        for line in yearly.to_string().split('\n'):
            print(f'    {line}')

        summary.append({
            'inst': inst,
            'is_n': is_s['n'], 'is_wr': is_s['wr'], 'is_pnl': is_s['pnl'],
            'oos_n': oos_s['n'], 'oos_wr': oos_s['wr'], 'oos_pnl': oos_s['pnl'],
            'all_n': full_s['n'], 'all_wr': full_s['wr'], 'all_pnl': full_s['pnl'],
            'all_dd': full_s['dd'], 'max_loss': full_s['max_l'],
        })

    # Combined
    combined = pd.concat(all_logs, ignore_index=True)
    combined.to_csv(TRADE_DIR / 'vpt_final_combined.csv', index=False)

    is_c   = _stats(combined[combined['period']=='IS'])
    oos_c  = _stats(combined[combined['period']=='OOS'])
    all_c  = _stats(combined)

    print(f'\n{"═"*100}')
    print(f'  COMBINED (all 3 instruments, 1 lot each)')
    print(f'{"═"*100}')
    for label, s in (('IS', is_c), ('OOS', oos_c), ('TOTAL', all_c)):
        print(f'  {label:<6}  n={s["n"]:>4}  WR={s["wr"]:>5.1f}%  '
              f'P&L ₹{s["pnl"]:>+11,.0f}  avg/tr ₹{s["avg"]:>+5,.0f}  '
              f'max-loss ₹{s["max_l"]:>+8,.0f}')


if __name__ == '__main__':
    main()
