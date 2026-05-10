"""
alerts/vp_live_daemon.py — Live VP-trailing-swing signal daemon.

Polls the cached 15m futures feed every POLL_SECS during market hours,
re-runs the canonical VP strategy on the most-recent rolling window, and
fires a Telegram alert on the **original** trade-alert bot whenever a
new trade entry appears that we have not yet alerted on.

Runs alongside `alert_runner.py` (ORB / VWAP) — they share the same
TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_IDS so the user sees all signals in
one channel.

Modes:
    --mode test       Fire one sample alert and exit (smoke test)
    --mode oneshot    Re-run once, alert any unseen entries since the
                      state file's last cursor, persist cursor, exit
    --mode daemon     Loop until 15:30 IST, oneshot every POLL_SECS

State file: alerts/.vp_live_state.json
    { "BANKNIFTY": "2026-05-08T11:30:00", ... }
    A trade is alerted iff its entry_ts > stored cursor for its instrument.

The daemon does NOT execute trades. It only emits signal alerts. Live
exits are tracked separately by alert_runner's exit watcher (or
discretionary).
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import pickle
import sys
import time
from datetime import datetime, time as dtime, timedelta

import pandas as pd
import pytz

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from alerts.telegram import send as tg_send                       # noqa: E402
from alerts.vp_signal_alert import format_signal_alert            # noqa: E402
from config import INSTRUMENTS                                    # noqa: E402
from run_canonical import CANONICAL_PARAMS                        # noqa: E402
from strategies.vp_trailing_swing import run_vp_trailing_swing    # noqa: E402

IST          = pytz.timezone('Asia/Kolkata')
CACHE_DIR    = ROOT / 'data' / 'cache_15m'
STATE_FILE   = ROOT / 'alerts' / '.vp_live_state.json'
WINDOW_DAYS  = 30        # rolling window the strategy runs on
POLL_SECS    = 300       # 5 minutes (15m candles roll every 15)
MARKET_CLOSE = dtime(15, 30)
MARKET_OPEN  = dtime(9, 15)


# ─────────────────────────────────────────────────────────────────────────────
# Telegram credentials (ORIGINAL trade-alert bot — NOT the macro bot)
# ─────────────────────────────────────────────────────────────────────────────
def _load_creds() -> tuple[str, list[str]]:
    """Read TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_IDS from token.env."""
    env_file = ROOT / 'token.env'
    creds = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if '=' in line and not line.strip().startswith('#'):
                k, v = line.split('=', 1)
                creds[k.strip()] = v.strip()
    token  = creds.get('TELEGRAM_BOT_TOKEN', os.environ.get('TELEGRAM_BOT_TOKEN', ''))
    chat_s = creds.get('TELEGRAM_CHAT_IDS', os.environ.get('TELEGRAM_CHAT_IDS', ''))
    chat_ids = [c.strip() for c in chat_s.split(',') if c.strip()]
    return token, chat_ids


# ─────────────────────────────────────────────────────────────────────────────
# State persistence
# ─────────────────────────────────────────────────────────────────────────────
def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────
def _load(inst: str, window_days: int = WINDOW_DAYS) -> pd.DataFrame:
    """Load the most recent `window_days` of 15m bars for `inst`."""
    f = CACHE_DIR / f'{inst}_combined.pkl'
    if not f.exists():
        return pd.DataFrame()
    with open(f, 'rb') as h:
        df = pickle.load(h)
    df = df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low',
                            'close': 'Close', 'volume': 'Volume',
                            'contract': 'Contract', 'expiry': 'Expiry'}
                   ).between_time('09:15', '15:30')
    cutoff = df.index.max() - timedelta(days=window_days)
    return df[df.index >= cutoff]


# ─────────────────────────────────────────────────────────────────────────────
# Core: detect newly-emitted entries since state cursor
# ─────────────────────────────────────────────────────────────────────────────
def _scan_instrument(inst: str, state: dict) -> list[dict]:
    """Run strategy on rolling window, return rows whose entry_ts is newer
    than state[inst]."""
    cfg = INSTRUMENTS.get(inst)
    sp  = CANONICAL_PARAMS.get(inst)
    if cfg is None or sp is None:
        return []

    df = _load(inst)
    if df.empty:
        return []

    log = run_vp_trailing_swing(df, cfg, sp)
    if log.empty:
        return []

    # Cursor: alert any entry strictly after this timestamp.
    # On first run (no cursor), seed at the latest bar so we don't
    # spam historical entries.
    cursor = state.get(inst)
    if cursor:
        cursor_ts = pd.Timestamp(cursor)
    else:
        cursor_ts = df.index.max()
        state[inst] = str(cursor_ts)

    log['entry_ts'] = pd.to_datetime(log['entry_ts'])
    fresh = log[log['entry_ts'] > cursor_ts].sort_values('entry_ts')
    return fresh.to_dict(orient='records')


# ─────────────────────────────────────────────────────────────────────────────
# Alert formatting (lightweight, since this is a position not a signal)
# ─────────────────────────────────────────────────────────────────────────────
def _format_entry_alert(row: dict, inst: str) -> str:
    direction = row.get('direction', '?')
    entry     = float(row.get('entry', 0))
    stop      = float(row.get('stop', 0))
    target    = float(row.get('target', 0))
    atr14     = float(row.get('atr14', 0))
    vah       = float(row.get('vah', 0))
    val       = float(row.get('val', 0))
    poc       = float(row.get('poc', 0))
    contract  = row.get('contract', '')
    ts        = pd.Timestamp(row.get('entry_ts'))

    emoji = '🟢⬆️' if direction == 'LONG' else '🔴⬇️'
    risk  = abs(entry - stop)
    reward = abs(target - entry)
    rr    = round(reward / risk, 2) if risk > 0 else 0

    return (
        f'{emoji} <b>{inst}</b> VP-TRAIL  <i>{direction}</i>  '
        f'@ <code>{entry:,.0f}</code>\n'
        f'<b>{ts:%Y-%m-%d %H:%M}</b>  ·  {contract}\n'
        f'<b>Stop</b>  <code>{stop:,.0f}</code>  '
        f'(risk {risk:,.0f} pts)\n'
        f'<b>Target</b> <code>{target:,.0f}</code>  '
        f'(reward {reward:,.0f} pts, R:R {rr})\n'
        f'<b>VA</b> {val:,.0f} – <b>POC</b> {poc:,.0f} – {vah:,.0f}  '
        f'·  ATR14 {atr14:,.0f}\n'
        f'<i>Trailing chandelier active. Multi-day hold if EOD-profitable.</i>'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Top-level orchestration
# ─────────────────────────────────────────────────────────────────────────────
def oneshot() -> int:
    """Scan all 3 instruments once. Returns count of alerts sent."""
    token, chat_ids = _load_creds()
    if not token or not chat_ids:
        print('  ⚠ TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_IDS missing — skip')
        return 0

    state = _load_state()
    sent  = 0

    for inst in ('BANKNIFTY', 'NIFTY', 'SENSEX'):
        try:
            new_entries = _scan_instrument(inst, state)
        except Exception as e:
            print(f'  ⚠ {inst}: scan failed: {e}')
            continue

        for row in new_entries:
            text = _format_entry_alert(row, inst)
            ok = False
            for cid in chat_ids:
                if tg_send(token, cid, text):
                    ok = True
            if ok:
                sent += 1
                state[inst] = str(pd.Timestamp(row['entry_ts']))
                print(f'  ✓ alert: {inst} {row["direction"]} @ {row["entry"]} '
                      f'({row["entry_ts"]})')

    _save_state(state)
    return sent


def daemon() -> None:
    """Loop until 15:30 IST, oneshot every POLL_SECS."""
    print('  ▶ VP live daemon started')
    while True:
        now_ist = datetime.now(IST).time()
        if now_ist > MARKET_CLOSE:
            print('  ▶ Market closed (>15:30 IST) — daemon exiting')
            return
        if now_ist < MARKET_OPEN:
            time.sleep(POLL_SECS)
            continue

        try:
            n = oneshot()
            if n > 0:
                print(f'  ◆ {n} alerts sent at {datetime.now(IST):%H:%M:%S}')
        except Exception as e:
            print(f'  ⚠ oneshot failed: {e}')

        time.sleep(POLL_SECS)


def test() -> None:
    """Send a fake VP entry alert to verify telegram wiring."""
    token, chat_ids = _load_creds()
    if not token or not chat_ids:
        print('  ⚠ TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_IDS missing')
        return

    sample = {
        'entry_ts': pd.Timestamp.now(),
        'direction': 'LONG',
        'entry': 51200,
        'stop':  50950,
        'target':51800,
        'atr14': 320,
        'vah':   51850,
        'val':   51100,
        'poc':   51500,
        'contract': 'NSE-BANKNIFTY-29May26-FUT',
    }
    text = '🧪 <b>SMOKE TEST</b>\n' + _format_entry_alert(sample, 'BANKNIFTY')
    for cid in chat_ids:
        ok = tg_send(token, cid, text)
        print(f'  → {cid}: {"OK" if ok else "FAIL"}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=('test', 'oneshot', 'daemon'),
                    default='oneshot')
    args = ap.parse_args()

    if   args.mode == 'test':    test()
    elif args.mode == 'oneshot': oneshot()
    elif args.mode == 'daemon':  daemon()


if __name__ == '__main__':
    main()
