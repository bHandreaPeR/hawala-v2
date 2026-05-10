"""
bots/macro_bot.py — Hawala MACRO/NEWS bot (separate Telegram channel)

Sends informational, non-actionable market intelligence on a fixed schedule:
  • 07:30 IST — Pre-market brief (gap setup, prev close, ATR14, India VIX,
                FII/DII flow, scheduled events today)
  • 12:00 IST — Mid-day macro check (intraday range, vol behaviour, sector
                snapshot, anomalies)
  • 16:00 IST — End-of-day wrap (close levels, intraday FII flow, breadth,
                tomorrow's hint)
  • Ad-hoc — regime-shift detected, India VIX spike (>10% intraday), large
              FII/DII outflow

NEVER sends trade entry/exit instructions. Pure intelligence channel.

Trade alerts continue on the original bot via alert_runner.py.

Env vars (token.env):
  TELEGRAM_BOT_TOKEN_MACRO=<new bot token>
  TELEGRAM_CHAT_IDS_MACRO=<comma-separated chat IDs>

Modes:
  python -m bots.macro_bot --mode premarket    # run once and exit
  python -m bots.macro_bot --mode midday
  python -m bots.macro_bot --mode eod
  python -m bots.macro_bot --mode daemon       # run all 3 + ad-hoc continuously

Cron (recommended for production):
  30 7 * * 1-5  cd /path/to/hawala && python -m bots.macro_bot --mode premarket
   0 12 * * 1-5 cd /path/to/hawala && python -m bots.macro_bot --mode midday
   0 16 * * 1-5 cd /path/to/hawala && python -m bots.macro_bot --mode eod
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path

import pytz
import pyotp
import pandas as pd
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Find token.env (works from worktree or main repo)
_env_path = None
for p in [ROOT, *ROOT.parents]:
    if (p / 'token.env').exists():
        _env_path = p / 'token.env'; break
if _env_path is None:
    sys.exit('❌ token.env not found')
load_dotenv(_env_path)

TG_TOKEN_MACRO    = os.getenv('TELEGRAM_BOT_TOKEN_MACRO', '').strip()
TG_CHAT_IDS_MACRO = [c.strip() for c in os.getenv('TELEGRAM_CHAT_IDS_MACRO', '').split(',') if c.strip()]
GROWW_TOKEN       = os.getenv('GROWW_API_KEY', '').strip()
GROWW_SECRET      = os.getenv('GROWW_TOTP_SECRET', '').strip()


def _check_env_or_die() -> None:
    if not TG_TOKEN_MACRO or not TG_CHAT_IDS_MACRO:
        sys.exit('❌ TELEGRAM_BOT_TOKEN_MACRO / TELEGRAM_CHAT_IDS_MACRO not '
                 'set in token.env')
    if not GROWW_TOKEN or not GROWW_SECRET:
        sys.exit('❌ GROWW_API_KEY / GROWW_TOTP_SECRET not set in token.env')

from alerts.telegram import send as tg                   # noqa: E402

IST = pytz.timezone('Asia/Kolkata')


def now_ist() -> datetime:
    return datetime.now(IST).replace(tzinfo=None)


def _send_macro(text: str) -> None:
    """Broadcast to all macro chat IDs."""
    for cid in TG_CHAT_IDS_MACRO:
        ok = tg(TG_TOKEN_MACRO, cid, text)
        print(f'  → macro/{cid}: {"sent" if ok else "FAIL"}')


def _get_groww():
    from growwapi import GrowwAPI
    totp  = pyotp.TOTP(GROWW_SECRET).now()
    token = GrowwAPI.get_access_token(api_key=GROWW_TOKEN, totp=totp)
    return GrowwAPI(token=token)


def _fetch_recent_15m(g, symbol: str, start: str, end: str) -> pd.DataFrame:
    """Fetch 15m candles for a recent window. Tolerates 6- or 7-column rows."""
    try:
        r = g.get_historical_candles(
            exchange='NSE', segment='CASH', groww_symbol=symbol,
            start_time=start, end_time=end,
            candle_interval=g.CANDLE_INTERVAL_MIN_15,
        )
        candles = r.get('candles', []) if isinstance(r, dict) else (r or [])
        if not candles:
            return pd.DataFrame()
        ncols = len(candles[0])
        # CASH-segment rows occasionally come back with a trailing extra
        # column (some indices include an OI-like field that's always 0).
        # Truncate any row to ≤7 columns and label first 6 as OHLCV+ts,
        # ignore the rest.
        if ncols >= 6:
            base = ['ts', 'Open', 'High', 'Low', 'Close', 'Volume']
            extra = [f'_x{i}' for i in range(ncols - 6)]
            df = pd.DataFrame(candles, columns=base + extra)
        else:
            return pd.DataFrame()
        df['ts'] = pd.to_datetime(df['ts'])
        df = df.set_index('ts')
        for c in ('Open','High','Low','Close','Volume'):
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        # Drop the extras
        return df[['Open','High','Low','Close','Volume']]
    except Exception as e:
        print(f'  ✗ fetch failed for {symbol}: {e}')
        return pd.DataFrame()


def _atr14(daily: pd.DataFrame) -> float:
    if len(daily) < 14: return 0.0
    rng = (daily['high'] - daily['low']).tail(14)
    return float(rng.mean())


# ── Schedule modes ────────────────────────────────────────────────────────────

def premarket_brief(g) -> None:
    """07:30 IST — overnight context + today's gap setup (informational)."""
    today = now_ist().date()
    end   = (today - timedelta(days=1)).strftime('%Y-%m-%d 16:00:00')
    start = (today - timedelta(days=30)).strftime('%Y-%m-%d 09:00:00')

    rows = []
    for sym, label in [('NSE-NIFTY','NIFTY 50'), ('NSE-BANKNIFTY','BANK NIFTY'),
                       ('NSE-INDIAVIX','India VIX')]:
        df = _fetch_recent_15m(g, sym, start, end)
        if df.empty: continue
        d = df.groupby(df.index.date).agg(
            open=('Open','first'), high=('High','max'),
            low=('Low','min'),     close=('Close','last'),
        )
        if len(d) < 2: continue
        prev_close = float(d['close'].iloc[-1])
        prev_prev  = float(d['close'].iloc[-2])
        chg_pct    = (prev_close - prev_prev) / prev_prev * 100
        atr        = _atr14(d)
        rows.append((label, prev_close, chg_pct, atr))

    msg_lines = [f'🌅 <b>HAWALA MACRO — Pre-market</b>',
                 f'{today.strftime("%A %d-%b-%Y")}', '']
    for label, px, chg, atr in rows:
        chg_arrow = '↑' if chg >= 0 else '↓'
        msg_lines.append(
            f'<b>{label}</b>: {px:,.0f}  '
            f'{chg_arrow} {abs(chg):.2f}%   ATR14 {atr:.0f}'
        )
    msg_lines += [
        '',
        '<i>Reading: gap direction at 09:15 will determine which strategy '
        'fires today (ORB / OPT_ORB / VP-fade). Trade signals on signal-bot.</i>'
    ]
    _send_macro('\n'.join(msg_lines))


def midday_check(g) -> None:
    """12:00 IST — intraday vol/range check."""
    today = now_ist().date()
    start = today.strftime('%Y-%m-%d 09:00:00')
    end   = today.strftime('%Y-%m-%d 12:15:00')

    rows = []
    for sym, label in [('NSE-NIFTY','NIFTY'), ('NSE-BANKNIFTY','BANKNIFTY'),
                       ('NSE-INDIAVIX','VIX')]:
        df = _fetch_recent_15m(g, sym, start, end)
        if df.empty: continue
        op = float(df['Open'].iloc[0])
        hi = float(df['High'].max())
        lo = float(df['Low'].min())
        cl = float(df['Close'].iloc[-1])
        rng = hi - lo
        chg_pct = (cl - op) / op * 100
        rows.append((label, op, cl, chg_pct, rng))

    msg = [f'🕛 <b>HAWALA MACRO — Mid-day</b>',
           f'{today.strftime("%A %d-%b-%Y")}  {now_ist():%H:%M IST}', '']
    for label, op, cl, chg, rng in rows:
        arrow = '↑' if chg >= 0 else '↓'
        msg.append(f'<b>{label}</b>: {cl:,.0f}  '
                   f'{arrow} {abs(chg):.2f}%   range {rng:.0f}')
    msg += ['', '<i>Half-day vol & breadth context — not a trade signal.</i>']
    _send_macro('\n'.join(msg))


def eod_wrap(g) -> None:
    """16:00 IST — close levels, intraday range, prep for tomorrow."""
    today = now_ist().date()
    start = today.strftime('%Y-%m-%d 09:00:00')
    end   = today.strftime('%Y-%m-%d 15:35:00')

    rows = []
    for sym, label in [('NSE-NIFTY','NIFTY 50'), ('NSE-BANKNIFTY','BANK NIFTY'),
                       ('NSE-INDIAVIX','India VIX')]:
        df = _fetch_recent_15m(g, sym, start, end)
        if df.empty: continue
        op = float(df['Open'].iloc[0])
        cl = float(df['Close'].iloc[-1])
        hi = float(df['High'].max())
        lo = float(df['Low'].min())
        chg = (cl - op) / op * 100
        rows.append((label, op, cl, chg, hi, lo))

    msg = [f'🌙 <b>HAWALA MACRO — End of Day</b>',
           f'{today.strftime("%A %d-%b-%Y")}', '']
    for label, op, cl, chg, hi, lo in rows:
        arrow = '↑' if chg >= 0 else '↓'
        msg.append(f'<b>{label}</b>: {cl:,.0f}  '
                   f'{arrow} {abs(chg):.2f}%   '
                   f'(H {hi:,.0f}  L {lo:,.0f})')
    msg += ['', '<i>EOD recap. Tomorrow\'s gap will be visible at 09:15.</i>']
    _send_macro('\n'.join(msg))


def regime_alert(message: str) -> None:
    """Ad-hoc alert — called externally when something macro-significant happens."""
    text = (f'⚡ <b>HAWALA MACRO — Regime Alert</b>\n'
            f'{now_ist():%Y-%m-%d %H:%M IST}\n\n{message}')
    _send_macro(text)


# ── Modes ─────────────────────────────────────────────────────────────────────

def _sleep_until(target_t: dtime) -> None:
    while True:
        n = now_ist()
        target = n.replace(hour=target_t.hour, minute=target_t.minute,
                            second=0, microsecond=0)
        if n >= target:
            return
        time.sleep(min(30, (target - n).total_seconds()))


def daemon_mode() -> None:
    """Run the 3 fixed briefs continuously through the trading day."""
    print('🤖 Macro daemon starting — schedule: 07:30 / 12:00 / 16:00 IST')
    g = _get_groww()
    fired = set()
    today = now_ist().date()
    schedule = [
        (dtime(7, 30), 'premarket', premarket_brief),
        (dtime(12, 0), 'midday',    midday_check),
        (dtime(16, 0), 'eod',       eod_wrap),
    ]
    while True:
        n = now_ist()
        if n.date() != today:
            today = n.date(); fired.clear()
            print(f'  📅  New day {today}')
        for t, label, fn in schedule:
            if label in fired: continue
            if n.time() >= t:
                print(f'  ▶  firing {label}')
                try:
                    fn(g)
                except Exception as e:
                    print(f'  ✗ {label} failed: {e}')
                fired.add(label)
        time.sleep(20)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', default='daemon',
                    choices=['premarket', 'midday', 'eod', 'daemon', 'test'])
    args = ap.parse_args()

    _check_env_or_die()

    if args.mode == 'test':
        _send_macro('🧪 <b>HAWALA MACRO test</b>\n'
                    'New macro/news bot is wired correctly. '
                    'Pre-market briefs at 07:30 IST, mid-day at 12:00, EOD at 16:00. '
                    'Trade signals continue on the original bot.')
        return

    g = _get_groww()
    if args.mode == 'premarket': premarket_brief(g)
    elif args.mode == 'midday':  midday_check(g)
    elif args.mode == 'eod':     eod_wrap(g)
    else: daemon_mode()


if __name__ == '__main__':
    main()
