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

IST        = pytz.timezone('Asia/Kolkata')
INSTRUMENT = 'BANKNIFTY'
inst_cfg   = INSTRUMENTS[INSTRUMENT]
orb_p      = STRATEGIES['orb']['params']
vwap_p     = STRATEGIES['vwap_reversion']['params']
opt_p      = STRATEGIES['options_orb']['params']

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
STRIKE_INTERVAL = inst_cfg['strike_interval']
UNDERLYING      = inst_cfg['underlying_symbol']

_DAY_NAMES = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']


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
              direction: int, ts) -> None:
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


def _fmt_opt_exit(reason: str, exit_prem: float, entry_prem: float,
                  opt_info: dict, ts) -> None:
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

    gap_info = morning_report(hist, today)
    if not gap_info:
        return

    strategy = gap_info['strategy']
    if strategy in ('SKIP_DOW', 'SKIP_GAP'):
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
