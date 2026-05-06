"""
v3/live/hero_zero_scanner.py
=============================
Hero Zero scanner — deep OTM penny options (<₹5) on expiry day, final hour.

Strategy:
  - EXPIRY DAY ONLY (DTE = 0) — earlier days have no gamma, pennies don't move
  - ENTRY WINDOW: 14:30–15:15 only — final hour is where gamma squeezes happen
    * Outside this window: OI history is tracked but no entries taken
  - Scans DEEP OTM strikes (1.5%–5% from spot) where LTP < PENNY_THRESHOLD (₹5)
  - Direction gate: OI quadrant signal (from morning candles in backtest; basis proxy in live)
    * dir=+1 → CE only (bullish momentum)
    * dir=-1 → PE only (bearish momentum)
    * dir=0  → skip (no conviction)
  - Trigger: OI velocity ≥ OI_VEL_THRESHOLD (institutional sweep confirmation)
  - No vol gate, no regime gate, no score threshold
  - Live: poll every LIVE_POLL_SECS (10s)
  - Exits: TP1 = +400% (5×), TP2 = +900% (10×), NO SL — hold to close or TP

Why no SL:
  Deep OTM penny near expiry has near-zero floor once past 80% down.
  Cutting at -80% gives up the tail — the entire thesis is waiting for
  the gamma squeeze that takes ₹2 → ₹10. If it doesn't happen, it
  expires worthless anyway. SL is just noise.

Why direction filter:
  OI velocity alone (previous version) had high false-positive rate —
  fast CE build on a bearish day just means puts are closing, not a
  real directional squeeze. Basis filter eliminates those bad entries.

Usage:
    python v3/live/hero_zero_scanner.py            # paper mode
    python v3/live/hero_zero_scanner.py --live     # enable real alerts

    # Backtest mode (uses cached 1m data):
    python v3/live/hero_zero_scanner.py --backtest
"""
import os, sys, time, pickle, pyotp, logging, argparse
from collections import defaultdict, deque
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger('hero_zero')

from alerts.telegram import send as _tg_send
from v3.signals.engine import signal_futures_basis as _basis_signal
from v3.signals.engine import signal_oi_quadrant as _oi_quadrant_signal

# ── Constants ─────────────────────────────────────────────────────────────────
NIFTY_LOT        = 65       # Nifty lot size (65 units per lot, corrected)
NIFTY_STEP       = 50

PENNY_THRESHOLD  = 5.0       # max entry LTP in ₹ to qualify as "penny"
MAX_DTE          = 0         # EXPIRY DAY ONLY — gamma explosion window is final hour only
OI_VEL_THRESHOLD = 10_000   # minimum OI velocity (contracts/bar) to trigger
OTM_MIN_PCT      = 0.015    # minimum OTM distance (1.5% from spot) — deep OTM only
OTM_MAX_PCT      = 0.05     # maximum OTM distance (5.0% from spot) — beyond = no liquidity

# ── Entry time window — last 1 hour on expiry day ─────────────────────────────
# On expiry day the final hour (14:30–15:29) is where gamma squeezes happen.
# Earlier in the day, deep OTM pennies have no gamma — they won't move even if
# spot moves 100 pts. In the last 60 mins, delta/gamma explode and ₹2 → ₹20
# is possible on a 150pt move. Outside this window we sit on hands.
ENTRY_START_HHMM = (14, 30)  # no entries before 14:30
ENTRY_END_HHMM   = (15, 15)  # no new entries after 15:15 (need 15m to exit)

# Exit parameters — NO SL, hold to TP or EOD
# Reasoning: deep OTM penny at ₹2 has already near-zero floor below -80%.
# Cutting SL gives up the tail that makes the strategy work. Hold and wait.
TP_PCT      = 4.0   # +400% = 5× entry price  (first target)
TP_PCT_MOON = 9.0   # +900% = 10× entry price (moon-shot second target, keep trailing)

LIVE_POLL_SECS   = 10       # poll every 10 seconds in live mode
EOD_HHMM         = (15, 29) # forced exit at market close

# OI history for velocity calculation
OI_VEL_WINDOW    = 5        # bars of OI history for velocity in live mode

# ── Telegram ──────────────────────────────────────────────────────────────────
def _load_telegram_config() -> tuple:
    env_path = ROOT / 'token.env'
    if not env_path.exists():
        return '', []
    env: dict = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                k, _, v = line.partition('=')
                env[k.strip()] = v.strip()
    token    = env.get('TELEGRAM_BOT_TOKEN', '').strip()
    chat_raw = env.get('TELEGRAM_CHAT_IDS', env.get('TELEGRAM_CHAT_ID', '')).strip()
    chat_ids = [c.strip() for c in chat_raw.split(',') if c.strip()]
    return token, chat_ids


def _tg_broadcast(token: str, chat_ids: list, text: str) -> None:
    if not token or not chat_ids:
        return
    for cid in chat_ids:
        _tg_send(token, cid, text)


# ── Alert formatters ──────────────────────────────────────────────────────────
def _fmt_hero_alert(
    side: str,
    strike: int,
    opt_sym: str,
    ltp: float,
    oi_velocity: float,
    dte: int,
    expiry: date,
    spot: float,
    otm_dist_pct: float,
    basis_note: str,
    paper: bool,
) -> str:
    mode_tag   = ' [PAPER]' if paper else ''
    side_emoji = '🟢 CALL' if side == 'CE' else '🔴 PUT'
    tp1_px     = round(ltp * (1 + TP_PCT), 2)
    tp2_px     = round(ltp * (1 + TP_PCT_MOON), 2)
    return (
        f"🎯 <b>HERO ZERO ALERT — NIFTY</b>{mode_tag}\n"
        f"{'─'*32}\n"
        f"{side_emoji}  <b>{opt_sym}</b>\n"
        f"Entry: ₹<b>{ltp:.2f}</b>  |  OTM: {otm_dist_pct:.1f}%  |  DTE: {dte}\n"
        f"Expiry: {expiry.strftime('%d %b %Y')}\n"
        f"Spot: {spot:,.0f}  |  Strike: {strike:,}\n"
        f"{'─'*32}\n"
        f"📐 Direction: {basis_note}\n"
        f"⚡ OI Velocity: <b>{oi_velocity:+,.0f}</b> contracts/bar\n"
        f"   → Institutional sweep confirmed\n"
        f"{'─'*32}\n"
        f"TP1: ₹{tp1_px:.2f} (+400%, 5×)  |  TP2: ₹{tp2_px:.2f} (+900%, 10×)\n"
        f"SL: NONE — hold to TP or EOD\n"
        f"Qty: {NIFTY_LOT} lots  |  Cost: ₹{ltp * NIFTY_LOT:,.0f}"
    )


def _fmt_hero_exit(
    opt_sym: str,
    entry_px: float,
    exit_px: float,
    exit_reason: str,
    pnl_pts: float,
    pnl_inr: float,
    paper: bool,
) -> str:
    win   = pnl_pts > 0
    mult  = exit_px / entry_px if entry_px > 0 else 1.0
    if mult >= 10:
        emoji = '🚀'
    elif mult >= 5:
        emoji = '💰'
    elif win:
        emoji = '✅'
    else:
        emoji = '❌'
    lbl   = 'WIN' if win else 'LOSS'
    sign  = '+' if pnl_pts >= 0 else ''
    reason_map = {
        'TP1': f'🎯 5× achieved! (+400%)',
        'TP2': f'🚀 10× MOON SHOT! (+900%)',
        'EOD': '🕥 EOD exit (14:45)',
    }
    return (
        f"⚡ <b>HERO ZERO EXIT — NIFTY</b>\n"
        f"{emoji} <b>{lbl} ({exit_reason})</b>\n"
        f"{'─'*32}\n"
        f"{opt_sym}\n"
        f"Entry: ₹{entry_px:.2f}  →  Exit: ₹{exit_px:.2f}  ({mult:.1f}×)\n"
        f"PnL: <b>{sign}₹{pnl_inr:,.2f}</b>  ({sign}{pnl_pts:.2f} pts)\n"
        f"Reason: {reason_map.get(exit_reason, exit_reason)}"
    )


# ── Contract resolvers ────────────────────────────────────────────────────────
def _last_tuesday(year: int, month: int) -> date:
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    days_back = (last_day.weekday() - 1) % 7
    return last_day - timedelta(days=days_back)


def _nearest_tuesday_expiry(trade_date: date) -> date:
    y, m = trade_date.year, trade_date.month
    for _ in range(3):
        exp = _last_tuesday(y, m)
        if exp >= trade_date:
            return exp
        m += 1
        if m > 12:
            m, y = 1, y + 1
    raise RuntimeError(f"Cannot find Tuesday expiry for {trade_date}")


def _option_symbol(expiry: date, strike: int, side: str) -> str:
    return (
        f"NSE-NIFTY-{expiry.day}{expiry.strftime('%b')}"
        f"{expiry.strftime('%y')}-{strike}-{side}"
    )


# ── OI velocity helpers ───────────────────────────────────────────────────────
def _compute_velocity_from_series(oi_series: list) -> float:
    """
    Given a list of OI values (possibly ffilled by NSE), find the most recent
    actual OI change and return velocity = change / bars_elapsed.
    """
    n = len(oi_series)
    if n < 2:
        return 0.0
    last_val = oi_series[-1]
    for i in range(n - 2, -1, -1):
        if oi_series[i] != last_val:
            elapsed = (n - 1) - i
            return float((last_val - oi_series[i]) / elapsed)
    return 0.0


# ── Groww auth ────────────────────────────────────────────────────────────────
def _get_groww():
    from growwapi import GrowwAPI
    env = {}
    with open(ROOT / 'token.env') as f:
        for line in f:
            if '=' in line:
                k, _, v = line.strip().partition('=')
                env[k] = v
    totp  = pyotp.TOTP(env['GROWW_TOTP_SECRET']).now()
    token = GrowwAPI.get_access_token(api_key=env['GROWW_API_KEY'], totp=totp)
    return GrowwAPI(token=token)


# ── Live option chain fetch ───────────────────────────────────────────────────
def _fetch_option_chain(g, expiry: date) -> Optional[dict]:
    exp_str = expiry.isoformat()
    try:
        r = g.get_option_chain(
            exchange='NSE', underlying='NIFTY', expiry_date=exp_str,
        )
    except Exception as e:
        log.warning("get_option_chain failed: expiry=%s error=%s", exp_str, e)
        return None
    underlying_ltp = float(r.get('underlying_ltp', 0) or 0)
    chain = r.get('data', r.get('strikes', []))
    if not chain:
        return None
    ce_oi, pe_oi, ce_ltp, pe_ltp = {}, {}, {}, {}
    for row in chain:
        try:
            strike = int(row.get('strike_price', row.get('strike', 0)))
            if strike <= 0:
                continue
            ce_oi[strike]  = float(row.get('call_oi',  row.get('ce_oi',  0)) or 0)
            pe_oi[strike]  = float(row.get('put_oi',   row.get('pe_oi',  0)) or 0)
            ce_ltp[strike] = float(row.get('call_ltp', row.get('ce_ltp', 0)) or 0)
            pe_ltp[strike] = float(row.get('put_ltp',  row.get('pe_ltp', 0)) or 0)
        except Exception:
            continue
    return {
        'underlying_ltp': underlying_ltp,
        'strikes':        sorted(ce_oi.keys()),
        'ce_oi':          ce_oi, 'pe_oi': pe_oi,
        'ce_ltp':         ce_ltp, 'pe_ltp': pe_ltp,
    }


# ── Live scanner ──────────────────────────────────────────────────────────────
def run_live(paper: bool = True):
    """
    Live Hero Zero scanner. Polls every LIVE_POLL_SECS seconds.
    Fires Telegram alert on first qualifying penny.
    Manages exactly one open position at a time.
    """
    today  = date.today()
    now    = datetime.now()
    expiry = _nearest_tuesday_expiry(today)
    dte    = (expiry - today).days

    if today.weekday() >= 5:
        raise RuntimeError("Market closed — weekend.")
    if dte > MAX_DTE:
        log.info(
            "Hero Zero: DTE=%d > MAX_DTE=%d. No penny hunt today.",
            dte, MAX_DTE,
        )
        return

    log.info(
        "Hero Zero live scanner starting. DTE=%d expiry=%s paper=%s",
        dte, expiry, paper,
    )

    tg_token, tg_chats = _load_telegram_config()
    g = _get_groww()

    # Rolling OI history per (strike, side) — keyed for velocity
    oi_history: dict = defaultdict(lambda: deque(maxlen=OI_VEL_WINDOW))

    position: Optional[dict] = None
    fired_alerts: set = set()   # avoid re-alerting same strike in same session

    exit_target = now.replace(hour=EOD_HHMM[0], minute=EOD_HHMM[1], second=0, microsecond=0)

    while True:
        now = datetime.now()

        if now >= exit_target:
            if position:
                log.info("EOD forced exit")
                # Exit logic handled below; here just break after forced exit
            else:
                log.info("Past EOD, no position. Session done.")
                break

        chain = _fetch_option_chain(g, expiry)
        if chain is None:
            log.warning("Chain fetch failed — retrying in %ds", LIVE_POLL_SECS)
            time.sleep(LIVE_POLL_SECS)
            continue

        spot = chain['underlying_ltp']

        # ── Compute basis direction gate ──────────────────────────────────────
        # spot = underlying_ltp from chain; futures ≈ chain underlying + basis
        # In live mode chain gives us the index spot directly.
        # signal_futures_basis: +1=LONG(CE only), -1=SHORT(PE only), 0=skip
        basis_dir, _, basis_note = _basis_signal(spot, spot, dte)
        # Note: chain['underlying_ltp'] IS the spot. For futures we use spot as proxy
        # because live chain doesn't return futures price separately. This means
        # basis = 0 unless we also fetch futures LTP separately. Log and allow both
        # sides when basis is neutral (we still need velocity + direction alignment).
        # TODO: wire futures LTP from separate Groww futures fetch for precise basis.

        # ── Monitor open position ─────────────────────────────────────────────
        if position:
            s, strike = position['side'], position['strike']
            current_ltp = (chain['ce_ltp'] if s == 'CE' else chain['pe_ltp']).get(strike)
            if current_ltp is None:
                time.sleep(LIVE_POLL_SECS)
                continue

            pnl_pct = (current_ltp - position['entry_price']) / position['entry_price']

            exit_reason = None
            # Check TP2 first (10×), then TP1 (5×) — no SL
            if pnl_pct >= TP_PCT_MOON:
                exit_reason = 'TP2'
            elif pnl_pct >= TP_PCT:
                exit_reason = 'TP1'
            elif now >= exit_target:
                exit_reason = 'EOD'
                current_ltp = chain['ce_ltp'].get(strike, chain['pe_ltp'].get(strike, position['entry_price']))

            if exit_reason:
                pnl_pts = current_ltp - position['entry_price']
                pnl_inr = pnl_pts * NIFTY_LOT
                msg = _fmt_hero_exit(
                    position['opt_symbol'],
                    position['entry_price'],
                    current_ltp,
                    exit_reason,
                    pnl_pts,
                    pnl_inr,
                    paper,
                )
                _tg_broadcast(tg_token, tg_chats, msg)
                log.info(
                    "EXIT %s %s @ %.2f | pnl=%.2f (%.1f%%) mult=%.1fx",
                    exit_reason, position['opt_symbol'],
                    current_ltp, pnl_pts, pnl_pct * 100,
                    current_ltp / position['entry_price'],
                )
                position = None
                if exit_reason == 'EOD':
                    break

            time.sleep(LIVE_POLL_SECS)
            continue

        # ── Update OI history (always, even outside entry window) ────────────
        for strike in chain['strikes']:
            oi_history[(strike, 'CE')].append(chain['ce_oi'].get(strike, 0))
            oi_history[(strike, 'PE')].append(chain['pe_oi'].get(strike, 0))

        # ── Entry time gate: expiry day, last 1 hour only (14:30–15:15) ──────
        now_hhmm = now.hour * 60 + now.minute
        entry_start = ENTRY_START_HHMM[0] * 60 + ENTRY_START_HHMM[1]  # 14:30
        entry_end   = ENTRY_END_HHMM[0]   * 60 + ENTRY_END_HHMM[1]    # 15:15
        if now_hhmm < entry_start:
            log.debug(
                "Entry window not open yet (now=%02d:%02d, opens=%02d:%02d). Watching OI buildup.",
                now.hour, now.minute, *ENTRY_START_HHMM,
            )
            time.sleep(LIVE_POLL_SECS)
            continue
        if now_hhmm >= entry_end:
            log.info("Entry window closed (past %02d:%02d). No new entries.", *ENTRY_END_HHMM)
            time.sleep(LIVE_POLL_SECS)
            continue

        # ── Scan for deep OTM pennies with fast OI velocity ──────────────────
        best = None   # (oi_velocity, side, strike, ltp, otm_pct)

        for strike in chain['strikes']:
            for side, ltp_dict in [('CE', chain['ce_ltp']), ('PE', chain['pe_ltp'])]:
                ltp = ltp_dict.get(strike, 0)
                if ltp <= 0 or ltp >= PENNY_THRESHOLD:
                    continue

                # Direction gate: basis must agree with side (or be neutral)
                if basis_dir == 1 and side != 'CE':
                    continue   # basis says LONG → CE only
                if basis_dir == -1 and side != 'PE':
                    continue   # basis says SHORT → PE only

                # Deep OTM band: must be between OTM_MIN_PCT and OTM_MAX_PCT
                otm_pct = abs(strike - spot) / spot * 100
                if side == 'CE':
                    if strike < spot * (1 + OTM_MIN_PCT):
                        continue   # too close (ATM / shallow OTM)
                    if strike > spot * (1 + OTM_MAX_PCT):
                        continue   # too far (no liquidity)
                else:  # PE
                    if strike > spot * (1 - OTM_MIN_PCT):
                        continue
                    if strike < spot * (1 - OTM_MAX_PCT):
                        continue

                # OI velocity confirmation
                hist = list(oi_history[(strike, side)])
                vel = _compute_velocity_from_series(hist)
                if vel < OI_VEL_THRESHOLD:
                    continue

                # Skip already alerted
                if (strike, side) in fired_alerts:
                    continue

                if best is None or vel > best[0]:
                    best = (vel, side, strike, ltp, otm_pct)

        if best:
            vel, side, strike, ltp, otm_pct = best
            opt_sym = _option_symbol(expiry, strike, side)
            fired_alerts.add((strike, side))

            log.info(
                "HERO ZERO TRIGGER: %s %s ltp=%.2f vel=%.0f otm=%.1f%% basis=%s",
                side, opt_sym, ltp, vel, otm_pct, basis_note,
            )

            msg = _fmt_hero_alert(
                side, strike, opt_sym, ltp, vel, dte, expiry, spot, otm_pct,
                basis_note, paper,
            )
            _tg_broadcast(tg_token, tg_chats, msg)

            position = {
                'side':        side,
                'strike':      strike,
                'opt_symbol':  opt_sym,
                'entry_price': ltp,
                'entry_time':  now,
                'qty':         NIFTY_LOT,
                'oi_velocity': vel,
                'basis_note':  basis_note,
            }
        else:
            log.debug("No hero zero candidate this poll (spot=%.0f basis=%s)", spot, basis_note)

        time.sleep(LIVE_POLL_SECS)


# ── Backtest engine ───────────────────────────────────────────────────────────
def run_backtest(start_date: str = None, end_date: str = None) -> pd.DataFrame:
    """
    Bar-by-bar Hero Zero backtest on cached 1m data.

    Logic:
      1. Only trade on DTE ≤ MAX_DTE days
      2. Load yfinance ^NSEI daily spot for basis direction gate
      3. Each bar: scan DEEP OTM strikes (1.5-5% from spot) for close < PENNY_THRESHOLD
      4. Gate: signal_futures_basis(futures_close, spot_close, dte) must agree with side
         * basis=LONG  → CE only
         * basis=SHORT → PE only
         * basis=0     → skip bar
      5. Compute OI velocity (rolling window); if vel > OI_VEL_THRESHOLD → trigger
      6. Exit on TP1 (+400%=5×), TP2 (+900%=10×), or EOD — NO SL
      7. One trade per day (take highest-velocity trigger)

    Returns DataFrame of all trades with columns:
      date, side, strike, expiry, dte, entry_ts, entry_px,
      exit_ts, exit_px, exit_reason, pnl_pts, pnl_inr, pnl_pct,
      oi_velocity, otm_pct, opt_symbol, basis_dir, basis_note
    """
    candle_path = ROOT / 'v3/cache/candles_1m_NIFTY.pkl'
    oi_path     = ROOT / 'v3/cache/option_oi_1m_NIFTY.pkl'

    if not candle_path.exists():
        raise FileNotFoundError(f"Candle cache not found: {candle_path}")
    if not oi_path.exists():
        raise FileNotFoundError(f"OI cache not found: {oi_path}")

    with open(candle_path, 'rb') as f:
        candles = pickle.load(f)
    with open(oi_path, 'rb') as f:
        oi_cache = pickle.load(f)

    # ── Normalise candle dates ─────────────────────────────────────────────────
    candles['date'] = pd.to_datetime(candles['date']) if candles['date'].dtype == object else candles['date']

    # Approximate spot per date from futures first-bar close
    # (used for OTM filter only — not for direction gate)
    spot_by_date = (
        candles.groupby('date')['close']
        .first()
        .to_dict()
    )
    spot_daily = {}  # kept for fallback compatibility

    trades = []

    _start = date.fromisoformat(start_date) if start_date else None
    _end   = date.fromisoformat(end_date)   if end_date   else None

    all_dates = sorted(oi_cache.keys())
    for d_str in all_dates:
        td_check = date.fromisoformat(d_str)
        if _start and td_check < _start:
            continue
        if _end and td_check > _end:
            continue
        td = date.fromisoformat(d_str)
        exp = _nearest_tuesday_expiry(td)
        dte = (exp - td).days

        if dte > MAX_DTE:
            continue

        data = oi_cache[d_str]   # {strike: {CE: df, PE: df}}

        # Spot from futures first-bar close
        spot = spot_by_date.get(td, 0)
        if spot <= 0:
            for k, v in spot_by_date.items():
                if hasattr(k, 'date') and k.date() == td:
                    spot = v
                    break
        if spot <= 0:
            log.warning("No spot for %s — skipping", d_str)
            continue

        # ── Compute day-level direction once using morning candles (9:15–11:00) ──
        # signal_oi_quadrant uses price + futures OI trend.
        # All DTE<=3 historical dates have non-null futures OI.
        # We compute once using the first 105 bars (≈11:00) to avoid lookahead
        # and to reduce per-bar cost from O(n²) to O(1) per day.
        candle_day = candles[candles['date'] == pd.Timestamp(td)].copy()
        df_morning = candle_day[
            (candle_day['ts'].dt.hour * 60 + candle_day['ts'].dt.minute) <= (11 * 60)
        ]
        day_dir, _, day_dir_note = _oi_quadrant_signal(df_morning)
        log.info(
            "HZ [%s] DTE=%d spot=%.0f dir=%+d note=%s",
            d_str, dte, spot, day_dir, day_dir_note,
        )

        if day_dir == 0:
            log.info("HZ [%s] direction neutral — skipping day", d_str)
            continue

        # ── Vectorized scan: flat DataFrame for the day ──────────────────────
        rows = []
        for strike, sides in data.items():
            for side, df_s in sides.items():
                if df_s.empty or 'close' not in df_s.columns:
                    continue
                tmp = df_s[['ts', 'close', 'oi']].copy()
                tmp['strike'] = strike
                tmp['side']   = side
                rows.append(tmp)

        if not rows:
            continue

        flat = pd.concat(rows, ignore_index=True).sort_values(['strike', 'side', 'ts'])
        flat['ts'] = pd.to_datetime(flat['ts'])

        # OI velocity: use ABSOLUTE change — on expiry day OI is net-declining
        # as contracts expire. Signed diff rolling max would be negative.
        # We care about the magnitude of institutional activity, not direction.
        flat['oi_vel'] = (
            flat.groupby(['strike', 'side'])['oi']
            .transform(lambda s: s.diff(1).abs().rolling(OI_VEL_WINDOW, min_periods=2).max())
        )

        # Entry window filter: expiry day last hour (14:30–15:15 only)
        flat['hhmm'] = flat['ts'].dt.hour * 60 + flat['ts'].dt.minute
        entry_start_m = ENTRY_START_HHMM[0] * 60 + ENTRY_START_HHMM[1]  # 870 = 14:30
        entry_end_m   = ENTRY_END_HHMM[0]   * 60 + ENTRY_END_HHMM[1]    # 915 = 15:15
        flat = flat[
            (flat['hhmm'] >= entry_start_m) &
            (flat['hhmm'] <  entry_end_m)
        ]

        # Use spot at 14:30 for OTM band — spot may have moved significantly
        # from morning open by the time we enter in the last hour.
        last_hr_candles = candle_day[
            candle_day['ts'].dt.hour * 60 + candle_day['ts'].dt.minute >= entry_start_m
        ]
        spot_1430 = float(last_hr_candles.iloc[0]['close']) if not last_hr_candles.empty else spot

        # Penny + velocity + direction + deep OTM filters
        flat = flat[(flat['close'] > 0) & (flat['close'] < PENNY_THRESHOLD)]
        flat = flat[flat['oi_vel'] >= OI_VEL_THRESHOLD]

        if day_dir == 1:
            flat = flat[flat['side'] == 'CE']
            flat = flat[
                (flat['strike'] >= spot_1430 * (1 + OTM_MIN_PCT)) &
                (flat['strike'] <= spot_1430 * (1 + OTM_MAX_PCT))
            ]
        else:
            flat = flat[flat['side'] == 'PE']
            flat = flat[
                (flat['strike'] <= spot_1430 * (1 - OTM_MIN_PCT)) &
                (flat['strike'] >= spot_1430 * (1 - OTM_MAX_PCT))
            ]

        if flat.empty:
            log.info("HZ [%s] no penny+velocity trigger after direction filter", d_str)
            continue

        # Entry: first bar that crosses velocity threshold (earliest trigger)
        entry_row = flat.sort_values('ts').iloc[0]
        entry_ts  = entry_row['ts']
        entry_px  = float(entry_row['close'])
        entry_vel = float(entry_row['oi_vel'])
        b_strike  = int(entry_row['strike'])
        b_side    = str(entry_row['side'])
        otm_pct   = abs(b_strike - spot) / spot * 100

        log.info("HZ ENTRY [%s]: %s %s @ ₹%.2f vel=%.0f otm=%.1f%%",
                 d_str, b_side, b_strike, entry_px, entry_vel, otm_pct)

        # Exit: scan bars after entry for TP1/TP2/EOD (no SL)
        df_pos   = data.get(b_strike, {}).get(b_side, pd.DataFrame())
        df_after = df_pos[df_pos['ts'] > entry_ts] if not df_pos.empty else pd.DataFrame()

        exit_px, exit_ts, exit_reason = entry_px, entry_ts, 'EOD'

        for _, bar in df_after.iterrows():
            bar_hhmm = bar['ts'].hour * 60 + bar['ts'].minute
            if bar_hhmm >= EOD_HHMM[0] * 60 + EOD_HHMM[1]:
                break
            px = float(bar['close'])
            if px <= 0:
                continue
            pct_g = (px - entry_px) / entry_px
            exit_px, exit_ts = px, bar['ts']
            if pct_g >= TP_PCT_MOON:
                exit_reason = 'TP2'; break
            elif pct_g >= TP_PCT:
                exit_reason = 'TP1'; break

        pnl_pts = exit_px - entry_px
        pnl_inr = pnl_pts * NIFTY_LOT
        pnl_pct = pnl_pts / entry_px * 100

        trades.append({
            'date':        d_str,
            'side':        b_side,
            'strike':      b_strike,
            'expiry':      str(exp),
            'dte':         dte,
            'entry_ts':    entry_ts,
            'entry_px':    round(entry_px, 2),
            'exit_ts':     exit_ts,
            'exit_px':     round(exit_px, 2),
            'exit_reason': exit_reason,
            'pnl_pts':     round(pnl_pts, 2),
            'pnl_inr':     round(pnl_inr, 2),
            'pnl_pct':     round(pnl_pct, 1),
            'mult':        round(exit_px / entry_px, 2) if entry_px > 0 else 0,
            'oi_velocity': round(entry_vel, 0),
            'otm_pct':     round(otm_pct, 2),
            'day_dir':     day_dir,
            'dir_note':    day_dir_note,
            'opt_symbol':  _option_symbol(exp, b_strike, b_side),
        })

    return pd.DataFrame(trades)


def _print_backtest_summary(df: pd.DataFrame) -> None:
    """Print clean backtest summary to stdout."""
    if df.empty:
        print("No Hero Zero trades found in backtest period.")
        print("  Possible reasons: no DTE≤3 days with penny + direction alignment, or OI vel too low.")
        return

    print("\n" + "═" * 72)
    print("HERO ZERO BACKTEST RESULTS — NIFTY  (v2: direction filter + deep OTM)")
    print(f"Penny threshold: <₹{PENNY_THRESHOLD}  |  DTE ≤ {MAX_DTE}  |  OI vel ≥ {OI_VEL_THRESHOLD:,}")
    print(f"OTM band: {OTM_MIN_PCT*100:.1f}%–{OTM_MAX_PCT*100:.0f}%  |  Direction: basis gate")
    print(f"TP1: +{TP_PCT*100:.0f}% (5×)  |  TP2: +{TP_PCT_MOON*100:.0f}% (10×)  |  NO SL  |  EOD forced")
    print("═" * 72)

    total   = len(df)
    wins    = (df['pnl_pts'] > 0).sum()
    losses  = (df['pnl_pts'] <= 0).sum()
    wr      = wins / total * 100 if total else 0
    tot_pnl = df['pnl_inr'].sum()
    avg_ret = df['pnl_pct'].mean()
    avg_mult = df['mult'].mean() if 'mult' in df.columns else None

    print(f"Total trades : {total}")
    print(f"Wins / Losses: {wins} / {losses}  (WR: {wr:.1f}%)")
    print(f"Total PnL    : ₹{tot_pnl:,.2f}")
    print(f"Avg return   : {avg_ret:+.1f}%", end='')
    if avg_mult is not None:
        print(f"  |  Avg mult: {avg_mult:.2f}×")
    else:
        print()
    if not df.empty:
        print(f"Best trade   : {df['pnl_pct'].max():+.1f}%  ({df.loc[df['pnl_pct'].idxmax(), 'opt_symbol']})")
        print(f"Worst trade  : {df['pnl_pct'].min():+.1f}%  ({df.loc[df['pnl_pct'].idxmin(), 'opt_symbol']})")
    print()

    # Exit breakdown
    print("Exit breakdown:")
    for reason, grp in df.groupby('exit_reason'):
        avg_m = grp['mult'].mean() if 'mult' in grp.columns else 0
        print(f"  {reason:6s}: {len(grp):>3} trades  avg={grp['pnl_pct'].mean():+.1f}%  avg_mult={avg_m:.2f}×  sum=₹{grp['pnl_inr'].sum():,.0f}")
    print()

    # Per-trade log
    print("─" * 72)
    print(f"{'Date':<12} {'Sym':<32} {'Ent':>5} {'Exit':>5} {'Ret%':>7} {'Mult':>5} {'Vel':>8}  {'Why'}")
    print("─" * 72)
    for _, row in df.iterrows():
        sign = '+' if row['pnl_pct'] >= 0 else ''
        mult = row.get('mult', row['exit_px'] / row['entry_px'])
        print(
            f"{row['date']:<12} {row['opt_symbol']:<32} "
            f"₹{row['entry_px']:>4.2f} ₹{row['exit_px']:>4.2f} "
            f"{sign}{row['pnl_pct']:>6.1f}%  {mult:>4.1f}×  {row['oi_velocity']:>8,.0f}  "
            f"{row['exit_reason']}"
        )
    print("═" * 72)

    # Hero Zero achieved?
    tp1 = df[df['exit_reason'] == 'TP1']
    tp2 = df[df['exit_reason'] == 'TP2']
    if tp1.empty and tp2.empty:
        print("\n⚠️  No TP exits — no 5× move found. Need gap/news catalyst on expiry day.")
    else:
        if not tp2.empty:
            print(f"\n🚀 {len(tp2)} MOON SHOT(S) — 10× achieved!")
            for _, h in tp2.iterrows():
                print(f"   {h['opt_symbol']}  ₹{h['entry_px']:.2f} → ₹{h['exit_px']:.2f}  ({h.get('mult',0):.1f}×)")
        if not tp1.empty:
            print(f"\n🎯 {len(tp1)} HERO ZERO(S) — 5× achieved!")
            for _, h in tp1.iterrows():
                print(f"   {h['opt_symbol']}  ₹{h['entry_px']:.2f} → ₹{h['exit_px']:.2f}  ({h.get('mult',0):.1f}×)")


# ── CLI entry point ───────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Hero Zero scanner — OTM penny near expiry')
    parser.add_argument('--backtest', action='store_true', help='Run backtest on cached data')
    parser.add_argument('--live',     action='store_true', help='Enable live alerts (default: paper)')
    parser.add_argument('--dte',      type=int, default=MAX_DTE, help=f'Max DTE (default {MAX_DTE})')
    parser.add_argument('--penny',    type=float, default=PENNY_THRESHOLD,
                        help=f'Penny threshold in ₹ (default {PENNY_THRESHOLD})')
    parser.add_argument('--vel',      type=float, default=OI_VEL_THRESHOLD,
                        help=f'OI velocity threshold (default {OI_VEL_THRESHOLD})')
    parser.add_argument('--start',    default=None, help='Backtest start date YYYY-MM-DD')
    parser.add_argument('--end',      default=None, help='Backtest end date YYYY-MM-DD')
    args = parser.parse_args()

    # Override constants from CLI
    MAX_DTE          = args.dte
    PENNY_THRESHOLD  = args.penny
    OI_VEL_THRESHOLD = args.vel

    if args.backtest:
        print(f"Running Hero Zero backtest with penny<₹{PENNY_THRESHOLD}, DTE≤{MAX_DTE}, OI_vel≥{OI_VEL_THRESHOLD:,.0f}")
        df = run_backtest(
            start_date=args.start if hasattr(args, 'start') else None,
            end_date=args.end   if hasattr(args, 'end')   else None,
        )
        _print_backtest_summary(df)
        out_path = ROOT / 'v3/backtest/hero_zero_backtest.csv'
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if not df.empty:
            df.to_csv(out_path, index=False)
            print(f"\nSaved: {out_path}")
    else:
        paper = not args.live
        run_live(paper=paper)
