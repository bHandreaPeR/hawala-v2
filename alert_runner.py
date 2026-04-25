# ============================================================
# alert_runner.py — Hawala v2 Live Alert Runner
# ============================================================
# Sends Telegram alerts for entry / exit signals in real time.
# Run each market day:
#   caffeinate -i python alert_runner.py
#
# Requires in token.env:
#   GROWW_API_KEY=...
#   TELEGRAM_BOT_TOKEN=...
#   TELEGRAM_CHAT_ID=...
#
# One-time Telegram setup:
#   1. Open Telegram → @BotFather → /newbot → copy token
#   2. Message your bot once, then visit:
#      https://api.telegram.org/bot<TOKEN>/getUpdates
#   3. Copy "id" from the "chat" block → TELEGRAM_CHAT_ID
# ============================================================

import os, sys, time
from datetime import datetime, date, timedelta
from datetime import time as dtime

import pytz
import pyotp
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv('token.env')
GROWW_TOKEN = os.getenv('GROWW_API_KEY', '').strip()
GROWW_SECRET = os.getenv('GROWW_TOTP_SECRET', '').strip()
TG_TOKEN    = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT_IDS = os.getenv('TELEGRAM_CHAT_IDS', '').split(',')
TG_CHAT_IDS = [cid.strip() for cid in TG_CHAT_IDS if cid.strip()]

for _name, _val in [('GROWW_API_KEY', GROWW_TOKEN),
                    ('GROWW_TOTP_SECRET', GROWW_SECRET),
                    ('TELEGRAM_BOT_TOKEN', TG_TOKEN),
                    ('TELEGRAM_CHAT_IDS', TG_CHAT_IDS)]:
    if not _val:
        sys.exit(f"❌  {_name} not found in token.env")

from growwapi import GrowwAPI

totp_gen = pyotp.TOTP(GROWW_SECRET)
current_totp = totp_gen.now()

access_token = GrowwAPI.get_access_token(api_key=GROWW_TOKEN, totp=current_totp)
groww = GrowwAPI(access_token)
print("✅  Groww authenticated")

from config import INSTRUMENTS, STRATEGIES
from data.fetch import fetch_instrument
from data.options_fetch import get_nearest_expiry, fetch_option_candles, lookup_option_price
from alerts.telegram import send as tg
from strategies.iron_condor import _bs_price as _ic_bs_price, _conviction_lots as _ic_conviction_lots

IST        = pytz.timezone('Asia/Kolkata')
INSTRUMENT = 'BANKNIFTY'
inst_cfg   = INSTRUMENTS[INSTRUMENT]
orb_p      = STRATEGIES['orb']['params']
vwap_p     = STRATEGIES['vwap_reversion']['params']
opt_p      = STRATEGIES['options_orb']['params']
ic_p       = STRATEGIES['iron_condor']['params']

LOT_SIZE        = inst_cfg['lot_size']
MIN_GAP         = inst_cfg['min_gap']
MAX_GAP         = inst_cfg['max_gap']
OPT_GAP_MIN     = opt_p['OPTIONS_GAP_MIN']
BUFFER          = orb_p['ORB_BREAKOUT_BUFFER']
DOW_ALLOW       = orb_p['ORB_DOW_ALLOW']
BAND_PCT        = vwap_p['VWAP_BAND_PCT']
STOP_ATR_VWAP   = vwap_p['VWAP_STOP_ATR']
TGT_ATR_VWAP    = vwap_p['VWAP_TARGET_ATR']
STOP_ATR_ORB    = orb_p['ORB_STOP_ATR']
TGT_ATR_ORB     = orb_p['ORB_TARGET_ATR']
OPT_TGT_MULT    = opt_p['OPTIONS_TARGET_MULT']
OPT_STP_MULT    = opt_p['OPTIONS_STOP_MULT']
OPT_SQUAREOFF   = dtime(*[int(x) for x in opt_p['OPTIONS_SQUAREOFF'].split(':')])
IC_SQUAREOFF    = dtime(*[int(x) for x in ic_p['IC_SQUAREOFF'].split(':')])
STRIKE_INTERVAL = inst_cfg['strike_interval']
UNDERLYING      = inst_cfg['underlying_symbol']

_DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

TRADE_LOG_DIR = pathlib.Path('trade_logs')
TRADE_LOG_DIR.mkdir(exist_ok=True)

import pathlib, csv as _csv

def _log_trade(row: dict) -> None:
    """Append a completed trade row to the live trade log CSV."""
    log_path = TRADE_LOG_DIR / 'live_trades.csv'
    fieldnames = ['date', 'weekday', 'strategy', 'ticker', 'direction',
                  'entry', 'entry_time', 'exit', 'exit_time', 'exit_reason',
                  'pnl_pts', 'pnl_rs', 'lots', 'lot_size', 'gap_pts', 'atr14']
    write_header = not log_path.exists()
    with open(log_path, 'a', newline='') as f:
        w = _csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        if write_header:
            w.writeheader()
        w.writerow(row)
    print(f"  📝 Trade logged → {log_path.name}")


# ── Utilities ─────────────────────────────────────────────────────────────────

def now_ist() -> datetime:
    return datetime.now(IST)


def sleep_until(target: dtime, poll_secs: int = 20) -> None:
    while True:
        now = now_ist()
        if now.time() >= target:
            return
        target_dt = IST.localize(datetime.combine(now.date(), target))
        secs_left = max(0, (target_dt - now).total_seconds())
        print(f"  ⏳  Waiting until {target} IST ({secs_left:.0f}s) ...")
        time.sleep(min(secs_left, poll_secs))


def fetch_today(today_str: str) -> pd.DataFrame:
    try:
        return fetch_instrument(INSTRUMENT, today_str, today_str,
                                groww=groww, use_futures=True)
    except Exception as e:
        print(f"  ⚠  fetch_today error: {e}")
        return pd.DataFrame()


def compute_atr14(hist: pd.DataFrame, today: date) -> float:
    hist_dates = sorted(set(hist.index.date))
    past = [d for d in hist_dates if d < today][-14:]
    ranges = []
    for d in past:
        day = hist[hist.index.date == d]
        if not day.empty:
            ranges.append(float(day['High'].max()) - float(day['Low'].min()))
    return float(np.mean(ranges)) if ranges else 300.0


def compute_vwap(df: pd.DataFrame) -> pd.Series:
    tp  = (df['High'] + df['Low'] + df['Close']) / 3
    vol = df['Volume'].replace(0, np.nan).ffill().fillna(1)
    return (tp * vol).cumsum() / vol.cumsum()


# ── Morning gap report ────────────────────────────────────────────────────────

def morning_report(hist: pd.DataFrame, today: date) -> dict:
    today_str  = str(today)
    hist_dates = sorted(set(hist.index.date))
    prev_dates = [d for d in hist_dates if d < today]
    if not prev_dates:
        print("  ⚠  No historical close available.")
        return {}

    prev_close = float(hist[hist.index.date == prev_dates[-1]]['Close'].iloc[-1])
    atr14      = compute_atr14(hist, today)

    sleep_until(dtime(9, 15))

    today_data   = pd.DataFrame()
    first_candle = pd.DataFrame()
    for attempt in range(12):
        today_data   = fetch_today(today_str)
        first_candle = today_data.between_time('09:15', '09:15') if not today_data.empty else pd.DataFrame()
        if not first_candle.empty:
            break
        print(f"  ⏳  Waiting for 09:15 candle (attempt {attempt+1})...")
        time.sleep(30)

    if first_candle.empty:
        for chat_id in TG_CHAT_IDS:
            tg(TG_TOKEN, chat_id, "⚠️ <b>HAWALA</b>\nCould not fetch today's opening candle — possible holiday or data issue.")
        return {}

    today_open = float(first_candle['Open'].iloc[0])
    gap_pts    = today_open - prev_close
    abs_gap    = abs(gap_pts)
    gap_arrow  = "↑" if gap_pts > 0 else "↓"
    dow        = today.weekday()
    dow_ok     = dow in DOW_ALLOW

    if abs_gap < MIN_GAP:
        strategy    = 'VWAP_REV'
        strat_label = 'VWAP Reversion'
        watch_note  = f'Entry window: 10:00–13:30  |  Band: {BAND_PCT*100:.2f}%'
        sq_time     = '14:45'
    elif abs_gap <= OPT_GAP_MIN:
        if not dow_ok:
            strategy    = 'SKIP_DOW'
            strat_label = f'ORB Futures — DOW SKIP ({_DAY_NAMES[dow]})'
            watch_note  = 'Mon/Thu excluded by DOW filter — no trade today'
            sq_time     = '—'
        else:
            strategy    = 'ORB'
            strat_label = 'ORB Futures'
            watch_note  = f'Entry window: 10:00–15:10  |  Buffer: {BUFFER} pts'
            sq_time     = '15:15'
    elif abs_gap <= MAX_GAP:
        if not dow_ok:
            strategy    = 'SKIP_DOW'
            strat_label = f'OPT_ORB — DOW SKIP ({_DAY_NAMES[dow]})'
            watch_note  = 'Mon/Thu excluded by DOW filter — no trade today'
            sq_time     = '—'
        else:
            strategy    = 'OPT_ORB'
            strat_label = 'Options ORB'
            watch_note  = f'Entry window: 10:00–{OPT_SQUAREOFF}  |  ATM CE/PE buy'
            sq_time     = OPT_SQUAREOFF.strftime('%H:%M')
    else:
        strategy    = 'SKIP_GAP'
        strat_label = 'No trade (gap too large)'
        watch_note  = f'Gap {abs_gap:.0f} pts > MAX_GAP {MAX_GAP} — skip'
        sq_time     = '—'

    msg = (
        f"📊 <b>HAWALA — {_DAY_NAMES[dow]} {today.strftime('%d-%b-%Y')}</b>\n\n"
        f"Gap: <b>{gap_pts:+.0f} pts</b>  {gap_arrow}\n"
        f"Prev close: {prev_close:.0f}  |  ATR14: {atr14:.0f}\n\n"
        f"Strategy: <b>{strat_label}</b>\n"
        f"{watch_note}\n"
        f"Squareoff: {sq_time}"
    )
    for chat_id in TG_CHAT_IDS:
            tg(TG_TOKEN, chat_id, msg)
    print(f"  📤  Gap report sent: {gap_pts:+.0f} pts → {strategy}")

    return {
        'strategy':   strategy,
        'gap_pts':    gap_pts,
        'today_open': today_open,
        'prev_close': prev_close,
        'atr14':      atr14,
        'dow':        dow,
    }


# ── ORB entry watcher (used for both ORB and OPT_ORB) ────────────────────────

def watch_orb_entry(today_str: str, gap_info: dict) -> dict | None:
    gap_pts = gap_info['gap_pts']
    gap_dir = 1 if gap_pts > 0 else -1
    is_opt  = gap_info['strategy'] == 'OPT_ORB'
    sq_time = OPT_SQUAREOFF if is_opt else dtime(15, 10)

    sleep_until(dtime(9, 31))

    orb_data    = fetch_today(today_str)
    orb_candles = orb_data.between_time('09:15', '09:30') if not orb_data.empty else pd.DataFrame()
    if orb_candles.empty:
        for chat_id in TG_CHAT_IDS:
            tg(TG_TOKEN, chat_id, "⚠️ <b>HAWALA</b>\nNo ORB candles found — skipping today.")
        return None

    orb_high = float(orb_candles['High'].max())
    orb_low  = float(orb_candles['Low'].min())
    prev_close = gap_info['prev_close']

    for _, c in orb_candles.iterrows():
        cl = float(c['Close'])
        if gap_dir == 1 and cl <= prev_close:
            for chat_id in TG_CHAT_IDS:
                tg(TG_TOKEN, chat_id, "⚠️ <b>HAWALA</b>\nGap filled during ORB window — no trade today.")
            return None
        if gap_dir == -1 and cl >= prev_close:
            for chat_id in TG_CHAT_IDS:
                tg(TG_TOKEN, chat_id, "⚠️ <b>HAWALA</b>\nGap filled during ORB window — no trade today.")
            return None

    print(f"  ORB range locked: Low {orb_low:.0f}  High {orb_high:.0f}")

    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id, 
       f"👀 <b>HAWALA — watching for breakout</b>\n"
       f"ORB range: {orb_low:.0f} – {orb_high:.0f}\n"
       f"{'Watching CE (gap up)' if gap_dir==1 else 'Watching PE (gap down)'}\n"
       f"Entry fires after {BUFFER} pt buffer breach")

    sleep_until(dtime(10, 0))

    seen_ts = set()
    while now_ist().time() < sq_time:
        today_data = fetch_today(today_str)
        if today_data.empty:
            time.sleep(60)
            continue

        post_orb = today_data.between_time('10:00', '15:10')
        for fidx, frow in post_orb.iterrows():
            if fidx in seen_ts:
                continue
            seen_ts.add(fidx)

            if fidx.time() >= sq_time:
                for chat_id in TG_CHAT_IDS:
                    tg(TG_TOKEN, chat_id,
                       f"⏹ <b>HAWALA {gap_info['strategy']}</b>\n"
                       f"Squareoff time reached — no breakout entry today.")
                return None

            c_close = float(frow['Close'])
            if gap_dir == 1 and c_close > orb_high + BUFFER:
                return {'entry_fut': c_close, 'entry_ts': fidx,
                        'gap_dir': gap_dir, 'orb_high': orb_high, 'orb_low': orb_low}
            if gap_dir == -1 and c_close < orb_low - BUFFER:
                return {'entry_fut': c_close, 'entry_ts': fidx,
                        'gap_dir': gap_dir, 'orb_high': orb_high, 'orb_low': orb_low}

        time.sleep(60)

    return None


# ── VWAP entry watcher ────────────────────────────────────────────────────────

def watch_vwap_entry(today_str: str, gap_info: dict) -> dict | None:
    sleep_until(dtime(10, 0))

    in_setup  = False
    setup_dir = None
    seen_ts   = set()

    while now_ist().time() < dtime(13, 30):
        today_data = fetch_today(today_str)
        if today_data.empty:
            time.sleep(60)
            continue

        session = today_data.between_time('09:15', '15:30').copy()
        session['vwap'] = compute_vwap(session)

        for fidx, frow in session.iterrows():
            t = fidx.time()
            if t < dtime(10, 0):
                continue
            if t > dtime(13, 30):
                break
            if fidx in seen_ts:
                continue
            seen_ts.add(fidx)

            c_close = float(frow['Close'])
            c_vwap  = float(frow['vwap'])
            dev_pct = (c_close - c_vwap) / c_vwap

            if not in_setup:
                if dev_pct >= BAND_PCT:
                    in_setup = True; setup_dir = -1
                elif dev_pct <= -BAND_PCT:
                    in_setup = True; setup_dir = 1
            else:
                if setup_dir == -1 and c_close <= c_vwap:
                    return {'entry': c_close, 'direction': -1, 'entry_ts': fidx,
                            'dev_pct': dev_pct, 'vwap': c_vwap}
                if setup_dir == 1 and c_close >= c_vwap:
                    return {'entry': c_close, 'direction': 1, 'entry_ts': fidx,
                            'dev_pct': dev_pct, 'vwap': c_vwap}
                if setup_dir == -1 and dev_pct <= -BAND_PCT:
                    setup_dir = 1
                elif setup_dir == 1 and dev_pct >= BAND_PCT:
                    setup_dir = -1

        time.sleep(60)
    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id,
           "⏹ <b>HAWALA VWAP</b>\n13:30 reached without a valid reversion entry — no trade today.")
    return None


# ── Entry alert senders ───────────────────────────────────────────────────────

def send_orb_entry(entry_info: dict, gap_info: dict) -> dict:
    atr14     = gap_info['atr14']
    ef        = entry_info['entry_fut']
    gdir      = entry_info['gap_dir']
    stop      = ef - atr14 * STOP_ATR_ORB * gdir
    target    = ef + atr14 * TGT_ATR_ORB  * gdir
    direction = 'LONG' if gdir == 1 else 'SHORT'
    t         = entry_info['entry_ts'].strftime('%H:%M')
    msg = (
        f"🟢 <b>ENTRY — ORB Futures</b>\n\n"
        f"BANKNIFTY {direction}\n"
        f"Entry: <b>₹{ef:.0f}</b>\n"
        f"Stop:   ₹{stop:.0f}  (−{atr14*STOP_ATR_ORB:.0f} pts)\n"
        f"Target: ₹{target:.0f}  (+{atr14*TGT_ATR_ORB:.0f} pts)\n\n"
        f"Gap: {gap_info['gap_pts']:+.0f} pts  |  ATR14: {atr14:.0f}\n"
        f"Time: {t}"
    )
    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id, msg)
    return {'stop': stop, 'target': target, 'direction': gdir, 'entry': ef}


def send_opt_entry(entry_info: dict, gap_info: dict, opt_info: dict) -> dict:
    ef     = entry_info['entry_fut']
    gdir   = entry_info['gap_dir']
    prem   = opt_info['premium']
    stop   = prem * OPT_STP_MULT
    target = prem * OPT_TGT_MULT
    t      = entry_info['entry_ts'].strftime('%H:%M')
    msg = (
        f"🟢 <b>ENTRY — OPT_ORB</b>\n\n"
        f"BANKNIFTY {opt_info['opt_type']} {opt_info['strike']}\n"
        f"Expiry: {opt_info['expiry']}  (DTE {opt_info.get('dte','?')})\n"
        f"Futures @ ₹{ef:.0f}\n\n"
        f"Premium: <b>₹{prem:.0f}</b>\n"
        f"Stop:   ₹{stop:.0f}  (50% of entry)\n"
        f"Target: ₹{target:.0f}  (2× entry)\n\n"
        f"Gap: {gap_info['gap_pts']:+.0f} pts  |  ATR14: {gap_info['atr14']:.0f}\n"
        f"Time: {t}"
    )
    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id, msg)
    return {'stop': stop, 'target': target}


def send_vwap_entry(entry_info: dict, gap_info: dict) -> dict:
    atr14     = gap_info['atr14']
    entry     = entry_info['entry']
    direction = entry_info['direction']
    stop_pts  = atr14 * STOP_ATR_VWAP
    tgt_pts   = atr14 * TGT_ATR_VWAP
    stop      = entry - stop_pts * direction
    target    = entry + tgt_pts  * direction
    side      = 'LONG' if direction == 1 else 'SHORT'
    t         = entry_info['entry_ts'].strftime('%H:%M')
    msg = (
        f"🟢 <b>ENTRY — VWAP Reversion</b>\n\n"
        f"BANKNIFTY {side}\n"
        f"Entry: <b>₹{entry:.0f}</b>  (VWAP: {entry_info['vwap']:.0f})\n"
        f"Stop:   ₹{stop:.0f}  (−{stop_pts:.0f} pts)\n"
        f"Target: ₹{target:.0f}  (+{tgt_pts:.0f} pts)\n\n"
        f"ATR14: {atr14:.0f}\n"
        f"Time: {t}"
    )
    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id, msg)
    return {'stop': stop, 'target': target, 'direction': direction, 'entry': entry}


# ── Exit watchers ─────────────────────────────────────────────────────────────

def _fmt_exit(strategy: str, reason: str, exit_px: float, entry_px: float,
              direction: int, ts, entry_info: dict = None, gap_info: dict = None) -> None:
    pnl_pts = (exit_px - entry_px) * direction
    pnl_rs  = round(pnl_pts * LOT_SIZE - 40, 2)
    icon    = '🎯' if reason == 'TARGET HIT' else ('🛑' if reason == 'STOP LOSS' else '⏹')
    t       = ts.strftime('%H:%M') if hasattr(ts, 'strftime') else str(ts)
    sign    = '+' if pnl_rs >= 0 else ''
    msg = (
        f"{icon} <b>EXIT — {strategy}</b>\n\n"
        f"Reason: <b>{reason}</b>\n"
        f"Exit: ₹{exit_px:.0f}  (Entry: ₹{entry_px:.0f})\n"
        f"P&L est: {sign}₹{pnl_rs:,.0f}  ({pnl_pts:+.0f} pts × {LOT_SIZE} lots)\n"
        f"Time: {t}"
    )
    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id, msg)
    # Persist trade to CSV
    today = date.today()
    _log_trade({
        'date':        today.isoformat(),
        'weekday':     _DAY_NAMES[today.weekday()],
        'strategy':    strategy,
        'ticker':      f'BANKNIFTY FUT',
        'direction':   'LONG' if direction == 1 else 'SHORT',
        'entry':       round(entry_px, 2),
        'entry_time':  entry_info['entry_ts'].strftime('%H:%M') if entry_info and 'entry_ts' in entry_info else '',
        'exit':        round(exit_px, 2),
        'exit_time':   t,
        'exit_reason': reason,
        'pnl_pts':     round(pnl_pts, 2),
        'pnl_rs':      pnl_rs,
        'lots':        1,
        'lot_size':    LOT_SIZE,
        'gap_pts':     gap_info['gap_pts'] if gap_info else '',
        'atr14':       round(gap_info['atr14'], 0) if gap_info else '',
    })


def _fmt_opt_exit(reason: str, exit_prem: float, entry_prem: float,
                  opt_info: dict, ts, entry_info: dict = None, gap_info: dict = None) -> None:
    pnl_pts = exit_prem - entry_prem
    pnl_rs  = round(pnl_pts * LOT_SIZE - 40, 2)
    icon    = '🎯' if reason == 'TARGET HIT' else ('🛑' if reason == 'STOP LOSS' else '⏹')
    t       = ts.strftime('%H:%M') if hasattr(ts, 'strftime') else str(ts)
    sign    = '+' if pnl_rs >= 0 else ''
    msg = (
        f"{icon} <b>EXIT — OPT_ORB</b>\n\n"
        f"BANKNIFTY {opt_info['opt_type']} {opt_info['strike']}  (exp {opt_info['expiry']})\n"
        f"Reason: <b>{reason}</b>\n"
        f"Exit premium: ₹{exit_prem:.0f}  (Entry: ₹{entry_prem:.0f})\n"
        f"P&L est: {sign}₹{pnl_rs:,.0f}  ({pnl_pts:+.0f} pts × {LOT_SIZE} lots)\n"
        f"Time: {t}"
    )
    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id, msg)
    today = date.today()
    _log_trade({
        'date':        today.isoformat(),
        'weekday':     _DAY_NAMES[today.weekday()],
        'strategy':    'OPT_ORB',
        'ticker':      f"BANKNIFTY {opt_info['opt_type']} {opt_info['strike']} {opt_info['expiry']}",
        'direction':   opt_info['opt_type'],
        'entry':       round(entry_prem, 2),
        'entry_time':  entry_info['entry_ts'].strftime('%H:%M') if entry_info and 'entry_ts' in entry_info else '',
        'exit':        round(exit_prem, 2),
        'exit_time':   t,
        'exit_reason': reason,
        'pnl_pts':     round(pnl_pts, 2),
        'pnl_rs':      pnl_rs,
        'lots':        1,
        'lot_size':    LOT_SIZE,
        'gap_pts':     gap_info['gap_pts'] if gap_info else '',
        'atr14':       round(gap_info['atr14'], 0) if gap_info else '',
    })


def watch_exit_futures(today_str: str, strategy: str,
                       entry_info: dict, trade: dict, gap_info: dict) -> None:
    entry_ts  = entry_info.get('entry_ts')
    entry_px  = trade['entry']
    stop      = trade['stop']
    target    = trade['target']
    direction = trade['direction']
    sq_time   = OPT_SQUAREOFF if strategy == 'OPT_ORB' else (
                dtime(14, 45) if strategy == 'VWAP_REV' else dtime(15, 15))
    seen_ts   = set()

    while True:
        n = now_ist()
        today_data = fetch_today(today_str)
        if today_data.empty:
            time.sleep(60)
            continue

        post = today_data[today_data.index > entry_ts] if entry_ts is not None else today_data

        for eidx, erow in post.iterrows():
            if eidx in seen_ts:
                continue
            seen_ts.add(eidx)

            et     = eidx.time()
            e_high = float(erow['High'])
            e_low  = float(erow['Low'])
            ep     = float(erow['Close'])

            if et >= sq_time:
                _fmt_exit(strategy, 'SQUARE OFF', ep, entry_px, direction, eidx)
                return

            if direction == 1:
                if e_low <= stop:
                    _fmt_exit(strategy, 'STOP LOSS', stop, entry_px, direction, eidx)
                    return
                if e_high >= target:
                    _fmt_exit(strategy, 'TARGET HIT', target, entry_px, direction, eidx)
                    return
            else:
                if e_high >= stop:
                    _fmt_exit(strategy, 'STOP LOSS', stop, entry_px, direction, eidx)
                    return
                if e_low <= target:
                    _fmt_exit(strategy, 'TARGET HIT', target, entry_px, direction, eidx)
                    return

        time.sleep(60)


def watch_exit_options(today_str: str, entry_info: dict,
                       trade: dict, opt_info: dict) -> None:
    entry_ts   = entry_info.get('entry_ts')
    prem_entry = opt_info['premium']
    stop_prem  = trade['stop']
    tgt_prem   = trade['target']
    strike     = opt_info['strike']
    expiry     = opt_info['expiry']
    opt_type   = opt_info['opt_type']
    seen_ts    = set()

    while True:
        n = now_ist()

        if n.time() >= OPT_SQUAREOFF:
            ep = prem_entry * 0.9
            try:
                opt_df = fetch_option_candles(groww, UNDERLYING, expiry,
                                              strike, opt_type, today_str, today_str)
                if not opt_df.empty:
                    raw = lookup_option_price(opt_df, n, field='Open')
                    if raw:
                        ep = float(raw)
            except Exception:
                pass
            _fmt_opt_exit('SQUARE OFF', ep, prem_entry, opt_info, n)
            return

        try:
            opt_df = fetch_option_candles(groww, UNDERLYING, expiry,
                                          strike, opt_type, today_str, today_str)
            if not opt_df.empty:
                latest_ts = opt_df.index[-1]
                if latest_ts not in seen_ts:
                    seen_ts.add(latest_ts)
                    bar = lookup_option_price(opt_df, latest_ts, field=None)
                    if bar and isinstance(bar, dict):
                        bar_high = float(bar.get('High', 0))
                        bar_low  = float(bar.get('Low', float('inf')))
                        if bar_high >= tgt_prem:
                            _fmt_opt_exit('TARGET HIT', tgt_prem, prem_entry, opt_info, latest_ts)
                            return
                        if bar_low <= stop_prem:
                            _fmt_opt_exit('STOP LOSS', stop_prem, prem_entry, opt_info, latest_ts)
                            return
        except Exception as e:
            print(f"  ⚠  opt exit fetch error: {e}")

        time.sleep(60)


# ── Iron Condor live execution ────────────────────────────────────────────────

def _is_expiry_today() -> tuple[bool, object]:
    """Return (True, expiry_date) if today is a BANKNIFTY expiry day, else (False, None)."""
    today = date.today()
    try:
        exp = get_nearest_expiry(groww, UNDERLYING, today, min_days=0)
        if exp is not None and pd.Timestamp(exp).date() == today:
            return True, exp
    except Exception as e:
        print(f"  ⚠  Expiry check error: {e}")
    return False, None


def _fetch_ic_premium(expiry, strike: int, opt_type: str,
                      today_str: str, entry_ts) -> float:
    """Fetch option premium at entry_ts from Groww candles."""
    try:
        opt_df = fetch_option_candles(groww, UNDERLYING, expiry,
                                      strike, opt_type, today_str, today_str)
        if not opt_df.empty:
            px = lookup_option_price(opt_df, entry_ts, field='Open')
            if px and float(px) > 0:
                return float(px)
    except Exception as e:
        print(f"  ⚠  IC premium fetch {opt_type} {strike}: {e}")
    return 0.0


def _fetch_ic_exit_premium(expiry, strike: int, opt_type: str,
                           today_str: str, check_ts) -> float:
    """Fetch latest option premium for exit check."""
    try:
        opt_df = fetch_option_candles(groww, UNDERLYING, expiry,
                                      strike, opt_type, today_str, today_str)
        if not opt_df.empty:
            px = lookup_option_price(opt_df, check_ts, field='Open')
            if px:
                return float(px)
            # Fall back to last available close
            return float(opt_df['Close'].iloc[-1])
    except Exception:
        pass
    return 0.0


def _log_ic_trade(today: date, entry_ts, exit_ts, exit_reason: str,
                  call_short: int, put_short: int, call_long: int, put_long: int,
                  net_credit: float, net_debit: float, expiry, atr14: float,
                  vix: float, gap_pts: float, conv_lots: int = 1) -> None:
    """Log the IC trade to live_trades.csv (P&L scaled by conviction lots)."""
    pnl_pts = net_credit - net_debit
    pnl_rs  = round(pnl_pts * LOT_SIZE * conv_lots - 40 * 4 * conv_lots, 2)
    _log_trade({
        'date':        today.isoformat(),
        'weekday':     _DAY_NAMES[today.weekday()],
        'strategy':    'IC',
        'ticker':      (f'IC {call_short}CE/{put_short}PE '
                        f'[{call_long}CE/{put_long}PE wings]'),
        'direction':   'SHORT_STRANGLE',
        'entry':       round(net_credit, 2),
        'entry_time':  entry_ts.strftime('%H:%M') if hasattr(entry_ts, 'strftime') else str(entry_ts),
        'exit':        round(net_debit, 2),
        'exit_time':   exit_ts.strftime('%H:%M') if hasattr(exit_ts, 'strftime') else str(exit_ts),
        'exit_reason': exit_reason,
        'pnl_pts':     round(pnl_pts, 2),
        'pnl_rs':      pnl_rs,
        'lots':        conv_lots,
        'lot_size':    LOT_SIZE,
        'gap_pts':     round(gap_pts, 0),
        'atr14':       round(atr14, 0),
    })


def run_iron_condor_live(gap_info: dict, hist: pd.DataFrame) -> None:
    """
    Live Iron Condor execution on expiry day.
    Called from run_day() when today is a BANKNIFTY expiry day.
    Sends Telegram alerts for entry and exit; logs trade to live_trades.csv.
    """
    today      = date.today()
    today_str  = str(today)
    atr14      = gap_info['atr14']
    gap_pts    = gap_info['gap_pts']
    today_open = gap_info['today_open']

    # ── Consecutive loss guard ─────────────────────────────────────────────
    loss_limit = ic_p['IC_CONSECUTIVE_LOSS_LIMIT']
    log_path   = TRADE_LOG_DIR / 'live_trades.csv'
    if log_path.exists():
        try:
            prev = pd.read_csv(log_path)
            prev = prev[prev['strategy'] == 'IC'].tail(loss_limit)
            if len(prev) == loss_limit and (prev['pnl_rs'] < 0).all():
                msg = (f"🚧 <b>IRON CONDOR — Skipped</b>\n"
                       f"{loss_limit} consecutive losses — sitting out this expiry.\n"
                       f"Today: {today.strftime('%d %b %Y')}")
                for chat_id in TG_CHAT_IDS:
                    tg(TG_TOKEN, chat_id, msg)
                print(f"  🚧 IC: {loss_limit} consecutive losses — skip.")
                return
        except Exception:
            pass

    # ── Gap gate ───────────────────────────────────────────────────────────
    if abs(gap_pts) > ic_p['IC_MAX_GAP']:
        msg = (f"🚧 <b>IRON CONDOR — Gap too large</b>\n"
               f"Gap: {gap_pts:+.0f} pts > {ic_p['IC_MAX_GAP']} pts limit\n"
               f"Skipping IC on this expiry day.")
        for chat_id in TG_CHAT_IDS:
            tg(TG_TOKEN, chat_id, msg)
        print(f"  🚧 IC: gap {gap_pts:+.0f} > {ic_p['IC_MAX_GAP']} — skip.")
        return

    # ── VIX gate ───────────────────────────────────────────────────────────
    vix_val = None
    sig_path = TRADE_LOG_DIR / f'market_signal_{today_str}.json'
    if sig_path.exists():
        try:
            import json
            sig = json.loads(sig_path.read_text())
            vix_val = sig.get('hawala_signal', {}).get('india_vix')
            if vix_val:
                vix_val = float(vix_val)
        except Exception:
            pass
    if vix_val is None:
        try:
            import yfinance as yf
            vix_df  = yf.download('^INDIAVIX', period='2d', progress=False)
            vix_val = float(vix_df['Close'].iloc[-1]) if not vix_df.empty else None
        except Exception:
            pass

    if vix_val is not None:
        if vix_val < ic_p['IC_VIX_MIN'] or vix_val > ic_p['IC_VIX_MAX']:
            msg = (f"🚧 <b>IRON CONDOR — VIX out of range</b>\n"
                   f"VIX: {vix_val:.1f}  (allowed: {ic_p['IC_VIX_MIN']}–{ic_p['IC_VIX_MAX']})\n"
                   f"Skipping IC.")
            for chat_id in TG_CHAT_IDS:
                tg(TG_TOKEN, chat_id, msg)
            print(f"  🚧 IC: VIX {vix_val:.1f} out of band — skip.")
            return
    else:
        print("  ⚠  IC: VIX unavailable — proceeding without VIX gate.")
        vix_val = 0.0

    # ── Wait for entry window ──────────────────────────────────────────────
    entry_after = dtime(*[int(x) for x in ic_p['IC_ENTRY_AFTER'].split(':')])
    sleep_until(entry_after)

    today_data = fetch_today(today_str)
    entry_candles = (today_data.between_time(ic_p['IC_ENTRY_AFTER'],
                                             ic_p['IC_ENTRY_AFTER'])
                     if not today_data.empty else pd.DataFrame())

    if entry_candles.empty:
        # Use today_open as proxy spot
        spot = today_open
        entry_ts = now_ist()
    else:
        spot     = float(entry_candles['Open'].iloc[0])
        entry_ts = entry_candles.index[0]

    # ── Strike selection ───────────────────────────────────────────────────
    si          = inst_cfg['strike_interval']   # 100
    call_short  = int(np.ceil(spot + atr14 * ic_p['IC_CALL_ATR'] / si) * si)
    put_short   = int(np.floor(spot - atr14 * ic_p['IC_PUT_ATR'] / si) * si)
    call_long   = call_short + ic_p['IC_WING_WIDTH']
    put_long    = put_short  - ic_p['IC_WING_WIDTH']

    is_expiry, expiry_date = _is_expiry_today()
    expiry_str = str(expiry_date) if expiry_date else today_str

    # ── Fetch premiums ─────────────────────────────────────────────────────
    cs_prem = _fetch_ic_premium(expiry_date, call_short, 'CE', today_str, entry_ts)
    ps_prem = _fetch_ic_premium(expiry_date, put_short,  'PE', today_str, entry_ts)
    cl_prem = _fetch_ic_premium(expiry_date, call_long,  'CE', today_str, entry_ts)
    pl_prem = _fetch_ic_premium(expiry_date, put_long,   'PE', today_str, entry_ts)

    # BS proxy fallback for any leg with 0 premium
    T = max(1.0 / (365 * 6.5), (dtime(15, 30).hour * 60 - now_ist().hour * 60) / (365 * 24 * 60))
    r, sigma = 0.065, 0.26
    if cs_prem <= 0: cs_prem = _ic_bs_price(spot, call_short, T, r, sigma, 'call')
    if ps_prem <= 0: ps_prem = _ic_bs_price(spot, put_short,  T, r, sigma, 'put')
    if cl_prem <= 0: cl_prem = _ic_bs_price(spot, call_long,  T, r, sigma, 'call')
    if pl_prem <= 0: pl_prem = _ic_bs_price(spot, put_long,   T, r, sigma, 'put')

    net_credit     = (cs_prem + ps_prem) - (cl_prem + pl_prem)
    max_profit_pts = net_credit
    max_loss_pts   = ic_p['IC_WING_WIDTH'] - net_credit
    upper_be       = call_short + net_credit
    lower_be       = put_short  - net_credit

    # ── Min credit gate ────────────────────────────────────────────────────
    if net_credit < ic_p['IC_MIN_NET_CREDIT']:
        msg = (f"🚧 <b>IRON CONDOR — Credit too low</b>\n"
               f"Net credit: {net_credit:.0f} pts < {ic_p['IC_MIN_NET_CREDIT']} min\n"
               f"Skipping IC.")
        for chat_id in TG_CHAT_IDS:
            tg(TG_TOKEN, chat_id, msg)
        print(f"  🚧 IC: net credit {net_credit:.0f} < {ic_p['IC_MIN_NET_CREDIT']} — skip.")
        return

    # ── Conviction lot sizing ──────────────────────────────────────────────
    conv_lots    = _ic_conviction_lots(vix_val, net_credit, ic_p['IC_WING_WIDTH'], ic_p)
    credit_ratio = net_credit / max(ic_p['IC_WING_WIDTH'], 1)

    if vix_val < 12.0:
        vix_regime = 'LOW (<12)'
    elif vix_val < 15.0:
        vix_regime = 'MID-LOW (12-15)'
    else:
        vix_regime = 'MID (15-18)'

    if credit_ratio > 0.40:
        conviction_label = 'HIGH ✅ (credit bonus active)'
    elif credit_ratio > 0.35:
        conviction_label = 'MED-HIGH ✅ (credit bonus active)'
    else:
        conviction_label = 'MED'

    # ── Entry alert ────────────────────────────────────────────────────────
    target_debit      = net_credit * (1 - ic_p['IC_PROFIT_TARGET_PCT'])
    pnl_total_max     = round(max_profit_pts * LOT_SIZE * conv_lots, 0)
    loss_total_max    = round(max_loss_pts   * LOT_SIZE * conv_lots, 0)
    margin_total      = round(max_loss_pts   * LOT_SIZE * conv_lots, 0)
    entry_msg = (
        f"🦅 <b>IRON CONDOR — Expiry Day</b>\n\n"
        f"<b>BANKNIFTY</b>  |  Expiry: {expiry_str}\n"
        f"VIX: <b>{vix_val:.1f}</b>  [{vix_regime}]  |  ATR14: {atr14:.0f}\n"
        f"Spot: ₹{spot:.0f}  |  Gap: {gap_pts:+.0f} pts\n\n"
        f"SELL <b>{call_short} CE</b> @ {cs_prem:.0f}  |  BUY {call_long} CE @ {cl_prem:.0f}\n"
        f"SELL <b>{put_short} PE</b> @ {ps_prem:.0f}  |  BUY {put_long} PE @ {pl_prem:.0f}\n\n"
        f"Net Credit: <b>{net_credit:.0f} pts</b>  (credit/wing: {credit_ratio:.0%})\n"
        f"Break-even: {lower_be:.0f} – {upper_be:.0f}\n\n"
        f"🎯 <b>Conviction: {conviction_label}</b>\n"
        f"Lot size: <b>{conv_lots} lots</b> × {LOT_SIZE} units\n"
        f"Max Profit: ₹{pnl_total_max:,.0f}  |  Max Loss: ₹{loss_total_max:,.0f}\n"
        f"Margin deployed: ~₹{margin_total:,.0f}\n\n"
        f"Target: debit ≤ {target_debit:.0f} pts ({ic_p['IC_PROFIT_TARGET_PCT']*100:.0f}% collected)\n"
        f"Squareoff: {ic_p['IC_SQUAREOFF']} IST"
    )
    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id, entry_msg)
    print(f"  📤  IC entry sent: net_credit={net_credit:.0f}  VIX={vix_val:.1f}  "
          f"lots={conv_lots}  strikes {put_short}P/{call_short}C")

    # ── Monitoring loop ────────────────────────────────────────────────────
    profit_target_debit = net_credit * (1 - ic_p['IC_PROFIT_TARGET_PCT'])
    stop_loss_debit     = net_credit * ic_p['IC_STOP_LOSS_MULT']
    seen_ts             = set()
    exit_reason         = None
    net_debit_exit      = net_credit   # default: no change

    while now_ist().time() < IC_SQUAREOFF:
        time.sleep(60)
        n          = now_ist()
        today_data = fetch_today(today_str)

        if today_data.empty:
            continue

        # Latest futures spot
        latest_fut = today_data.iloc[-1]
        spot_now   = float(latest_fut['Close'])
        latest_ts  = today_data.index[-1]

        if latest_ts in seen_ts:
            continue
        seen_ts.add(latest_ts)

        # ── Breach guard (spot approaching short strike) ───────────────
        breach_buf = ic_p['IC_BREACH_BUFFER']
        if (spot_now >= call_short - breach_buf or
                spot_now <= put_short + breach_buf):
            # Fetch exit premiums
            cs_exit = _fetch_ic_exit_premium(expiry_date, call_short, 'CE', today_str, latest_ts)
            ps_exit = _fetch_ic_exit_premium(expiry_date, put_short,  'PE', today_str, latest_ts)
            cl_exit = _fetch_ic_exit_premium(expiry_date, call_long,  'CE', today_str, latest_ts)
            pl_exit = _fetch_ic_exit_premium(expiry_date, put_long,   'PE', today_str, latest_ts)
            net_debit_exit = (cs_exit + ps_exit) - (cl_exit + pl_exit)
            exit_reason    = 'BREACH EXIT'
            break

        # ── Fetch current net debit to close ──────────────────────────
        try:
            cs_exit = _fetch_ic_exit_premium(expiry_date, call_short, 'CE', today_str, latest_ts)
            ps_exit = _fetch_ic_exit_premium(expiry_date, put_short,  'PE', today_str, latest_ts)
            cl_exit = _fetch_ic_exit_premium(expiry_date, call_long,  'CE', today_str, latest_ts)
            pl_exit = _fetch_ic_exit_premium(expiry_date, put_long,   'PE', today_str, latest_ts)
            current_debit = (cs_exit + ps_exit) - (cl_exit + pl_exit)
        except Exception as e:
            print(f"  ⚠  IC monitor fetch error: {e}")
            continue

        if current_debit <= profit_target_debit:
            net_debit_exit = current_debit
            exit_reason    = 'TARGET HIT'
            break

        if current_debit >= stop_loss_debit:
            net_debit_exit = current_debit
            exit_reason    = 'STOP LOSS'
            break

    # ── Time squareoff ─────────────────────────────────────────────────────
    if exit_reason is None:
        exit_reason = 'SQUARE OFF'
        # Fetch final premiums at squareoff
        sq_ts = now_ist()
        cs_exit = _fetch_ic_exit_premium(expiry_date, call_short, 'CE', today_str, sq_ts)
        ps_exit = _fetch_ic_exit_premium(expiry_date, put_short,  'PE', today_str, sq_ts)
        cl_exit = _fetch_ic_exit_premium(expiry_date, call_long,  'CE', today_str, sq_ts)
        pl_exit = _fetch_ic_exit_premium(expiry_date, put_long,   'PE', today_str, sq_ts)
        net_debit_exit = (cs_exit + ps_exit) - (cl_exit + pl_exit)

    exit_ts  = now_ist()
    pnl_pts  = net_credit - net_debit_exit
    pnl_rs   = round(pnl_pts * LOT_SIZE * conv_lots - 40 * 4 * conv_lots, 2)
    icon     = '🎯' if pnl_rs > 0 else ('⏹' if exit_reason == 'SQUARE OFF' else '🛑')
    sign     = '+' if pnl_rs >= 0 else ''
    pct_coll = round((pnl_pts / net_credit * 100), 1) if net_credit else 0

    exit_msg = (
        f"{icon} <b>EXIT — Iron Condor ({exit_reason})</b>\n\n"
        f"Net debit to close: {net_debit_exit:.0f} pts\n"
        f"P&L: {sign}₹{pnl_rs:,.0f}  ({pct_coll:+.1f}% of credit)  ×{conv_lots} lots\n"
        f"Per lot: {sign}₹{round(pnl_pts*LOT_SIZE-40*4):,.0f}\n"
        f"Time: {exit_ts.strftime('%H:%M')} IST"
    )
    for chat_id in TG_CHAT_IDS:
        tg(TG_TOKEN, chat_id, exit_msg)
    print(f"  📤  IC exit sent: {exit_reason}  lots={conv_lots}  pnl=₹{pnl_rs:,.0f}")

    _log_ic_trade(today, entry_ts, exit_ts, exit_reason,
                  call_short, put_short, call_long, put_long,
                  net_credit, net_debit_exit, expiry_date, atr14, vix_val, gap_pts,
                  conv_lots=conv_lots)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_day() -> None:
    today     = date.today()
    today_str = str(today)
    dow       = today.weekday()

    print(f"\n{'='*60}")
    print(f"  HAWALA ALERT RUNNER — {_DAY_NAMES[dow]} {today}")
    print(f"{'='*60}")

    if dow >= 5:
        print("  Weekend — no market session.")
        return

    # Pre-load 30 calendar days of history for ATR14 + prev close
    hist_start = str(today - timedelta(days=30))
    print(f"  Loading history {hist_start} → {today_str} ...")
    hist = fetch_instrument(INSTRUMENT, hist_start, today_str,
                            groww=groww, use_futures=True)
    if hist.empty:
        print("  ❌  History fetch failed.")
        for chat_id in TG_CHAT_IDS:
            tg(TG_TOKEN, chat_id, "❌ <b>HAWALA</b>\nFailed to fetch historical data — runner aborted.")
        return

    # ── Expiry day check — Iron Condor takes priority ─────────────────────
    is_expiry, expiry_date = _is_expiry_today()
    if is_expiry and dow == 3:   # Thursday expiry day
        print(f"  📅  Expiry day detected ({expiry_date}) — routing to Iron Condor.")
        # Still need gap_info for ATR14, gap_pts, today_open
        gap_info = morning_report(hist, today)
        if gap_info:
            run_iron_condor_live(gap_info, hist)
        return

    gap_info = morning_report(hist, today)
    if not gap_info:
        return

    strategy = gap_info['strategy']
    if strategy in ('SKIP_DOW', 'SKIP_GAP'):
        # Log the no-trade day so the weekly report is complete
        _log_trade({
            'date':        today.isoformat(),
            'weekday':     _DAY_NAMES[dow],
            'strategy':    'NO_TRADE',
            'ticker':      'BANKNIFTY',
            'direction':   '—',
            'entry':       '',
            'entry_time':  '',
            'exit':        '',
            'exit_time':   '',
            'exit_reason': strategy,
            'pnl_pts':     0,
            'pnl_rs':      0,
            'lots':        0,
            'lot_size':    LOT_SIZE,
            'gap_pts':     gap_info.get('gap_pts', ''),
            'atr14':       round(gap_info.get('atr14', 0), 0),
        })
        return

    # ── ORB / OPT_ORB path ───────────────────────────────────────────────────
    if strategy in ('ORB', 'OPT_ORB'):
        entry_info = watch_orb_entry(today_str, gap_info)
        if entry_info is None:
            return

        if strategy == 'ORB':
            trade = send_orb_entry(entry_info, gap_info)
            watch_exit_futures(today_str, 'ORB', entry_info, trade, gap_info)

        else:  # OPT_ORB
            ef       = entry_info['entry_fut']
            gap_dir  = entry_info['gap_dir']
            opt_type = 'CE' if gap_dir == 1 else 'PE'
            strike   = int(round(ef / STRIKE_INTERVAL) * STRIKE_INTERVAL)

            expiry_date = None
            premium     = None
            dte         = None
            try:
                expiry_date = get_nearest_expiry(groww, UNDERLYING, today, min_days=0)
                if expiry_date is None:
                    expiry_date = get_nearest_expiry(groww, UNDERLYING, today, min_days=1)
                if expiry_date:
                    dte    = (pd.Timestamp(expiry_date).date() - today).days
                    opt_df = fetch_option_candles(groww, UNDERLYING, expiry_date,
                                                  strike, opt_type, today_str, today_str)
                    if not opt_df.empty:
                        premium = lookup_option_price(opt_df, entry_info['entry_ts'], field='Open')
                        if premium:
                            premium = float(premium)
            except Exception as e:
                print(f"  ⚠  Options fetch error: {e}")

            if not premium or premium <= 0:
                premium = gap_info['atr14'] * 0.15
                print(f"  ℹ  Using ATR proxy premium: ₹{premium:.0f}")

            opt_info = {
                'premium':  premium,
                'strike':   strike,
                'expiry':   str(expiry_date) if expiry_date else 'N/A',
                'opt_type': opt_type,
                'dte':      dte,
            }
            trade = send_opt_entry(entry_info, gap_info, opt_info)
            watch_exit_options(today_str, entry_info, trade, opt_info)

    # ── VWAP path ─────────────────────────────────────────────────────────────
    else:
        entry_info = watch_vwap_entry(today_str, gap_info)
        if entry_info is None:
            return
        trade = send_vwap_entry(entry_info, gap_info)
        watch_exit_futures(today_str, 'VWAP_REV', entry_info, trade, gap_info)


if __name__ == '__main__':
    run_day()
    print("\n✅  Alert runner finished for today.")
