"""
v3/live/scanner.py
===================
Live intraday signal scanner for v3.
Runs during market hours. Polls every 1 minute.

Pipeline every tick:
  1. Fetch latest 1m candles (rolling 30-candle window)
  2. Snapshot options chain (every 5 min)
  3. Compute all 6 signals
  4. Output SignalState + Telegram alert on direction change

Usage:
  python v3/live/scanner.py

Logs to: v3/cache/scanner_YYYYMMDD.log + v3/cache/signals_YYYYMMDD.csv
"""
import os, sys, time, pickle, pyotp, logging, csv
from datetime import datetime, date, time as dtime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from v3.signals.engine import compute_signal_state, state_to_dict
from v3.data.options_chain import snapshot_once, load_snapshots, save_snapshots, \
    compute_oi_velocity, detect_strike_walls

log_dir = ROOT / 'v3' / 'cache'
today_str = date.today().strftime('%Y%m%d')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f'scanner_{today_str}.log'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger('v3.scanner')

# ── Config ────────────────────────────────────────────────────────────────────
POLL_SECS         = 60      # candle poll: every 1 minute
OPTIONS_POLL_SECS = 300     # options chain: every 5 minutes
MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
CANDLE_WINDOW = 30          # rolling 1m candle window for signals
SIGNAL_LOG = log_dir / f'signals_{today_str}.csv'


# ── Auth ──────────────────────────────────────────────────────────────────────
def _get_groww():
    from growwapi import GrowwAPI
    env = {}
    with open(ROOT / 'token.env') as f:
        for line in f:
            if '=' in line:
                k, _, v = line.strip().partition('=')
                env[k] = v
    totp = pyotp.TOTP(env['GROWW_TOTP_SECRET']).now()
    token = GrowwAPI.get_access_token(api_key=env['GROWW_API_KEY'], totp=totp)
    return GrowwAPI(token=token), env


def _get_spot_ltp(g) -> float:
    """Get BankNifty spot via LTP."""
    try:
        r = g.get_ltp(exchange='NSE', segment='CASH',
                      groww_symbol='NSE-NIFTY BANK')
        return float(r.get('ltp', 0))
    except:
        return 0.0


def _get_fut_ltp(g, symbol: str) -> float:
    try:
        r = g.get_ltp(exchange='NSE', segment='FNO', groww_symbol=symbol)
        return float(r.get('ltp', 0))
    except:
        return 0.0


def _get_expiry_and_symbol(g) -> tuple[str, str, int]:
    """Returns (expiry_str, groww_symbol, days_to_expiry)"""
    today = date.today()
    for offset in range(3):
        m = today.month + offset
        y = today.year
        if m > 12:
            m -= 12
            y += 1
        try:
            result = g.get_expiries(exchange='NSE', underlying_symbol='BANKNIFTY', year=y, month=m)
            for exp in sorted(result.get('expiries', [])):
                exp_d = date.fromisoformat(exp)
                if exp_d >= today:
                    dte = (exp_d - today).days
                    d_obj = exp_d
                    sym = f"NSE-BANKNIFTY-{d_obj.day}{d_obj.strftime('%b')}{d_obj.strftime('%y')}-FUT"
                    return exp, sym, dte
        except:
            pass
        time.sleep(0.3)
    raise RuntimeError("No expiry found")


def _load_fii_signals() -> tuple[int, int]:
    """Load today's FII participant signals from yesterday's EOD data."""
    try:
        cache_file = ROOT / 'trade_logs' / '_fii_fo_cache.pkl'
        if not cache_file.exists():
            return 0, 0
        with open(cache_file, 'rb') as f:
            fo = pickle.load(f)

        # Use most recent available date
        dates = sorted(fo.keys())
        if not dates:
            return 0, 0
        latest = fo[dates[-1]]
        fl = latest.get('fut_long', 0)
        fs = latest.get('fut_short', 0)
        fut_level = 1 if fl > fs * 1.15 else (-1 if fs > fl * 1.15 else 0)

        # FII cash: from fii_data.csv
        fii_csv = ROOT / 'fii_data.csv'
        if fii_csv.exists():
            import pandas as pd
            fii = pd.read_csv(fii_csv)
            fii['date'] = pd.to_datetime(fii['date']).dt.date
            fii = fii.sort_values('date')
            yesterday = date.today() - timedelta(days=1)
            row = fii[fii['date'] <= yesterday].tail(1)
            if not row.empty:
                net = float(row.iloc[0]['fpi_net'])
                cash = 1 if net > 500 else (-1 if net < -500 else 0)
                return fut_level, cash

        return fut_level, 0
    except Exception as e:
        log.warning(f"FII signals load error: {e}")
        return 0, 0


def _load_pcr() -> tuple[float, float]:
    """Load today's PCR from bhavcopy cache."""
    try:
        cache_files = list((ROOT / 'v3' / 'cache').glob('bhavcopy_BN_*.pkl'))
        if not cache_files:
            return 1.0, 1.0
        with open(sorted(cache_files)[-1], 'rb') as f:
            bhav = pickle.load(f)
        from v3.data.nse_bhavcopy import compute_daily_pcr
        pcr_df = compute_daily_pcr(bhav)
        if pcr_df.empty:
            return 1.0, 1.0
        last = pcr_df.iloc[-1]
        return float(last['pcr']), float(last.get('pcr_5d_ma', last['pcr']))
    except Exception as e:
        log.warning(f"PCR load error: {e}")
        return 1.0, 1.0


def _send_telegram(msg: str, env: dict):
    try:
        import requests as req
        token = env.get('TELEGRAM_BOT_TOKEN', '')
        chat_ids = env.get('TELEGRAM_CHAT_IDS', '').split(',')
        if not token:
            return
        for cid in chat_ids:
            cid = cid.strip()
            if cid:
                req.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json={'chat_id': cid, 'text': msg, 'parse_mode': 'HTML'},
                    timeout=5
                )
    except Exception as e:
        log.warning(f"Telegram error: {e}")


# ── Main scanner loop ─────────────────────────────────────────────────────────
def run():
    import pandas as pd

    g, env = _get_groww()
    log.info("V3 Scanner started")

    expiry, fut_symbol, dte = _get_expiry_and_symbol(g)
    log.info(f"Contract: {fut_symbol}, expiry={expiry}, DTE={dte}")

    # Init options chain snapshots
    snapshots = load_snapshots()
    log.info(f"Loaded {len(snapshots)} existing chain snapshots")

    # Rolling 1m candle buffer
    candle_buffer = pd.DataFrame()

    # State tracking
    last_direction   = 0
    last_options_ts  = 0
    last_candle_ts   = None
    velocity_data    = {}
    walls            = {}
    fii_fut_level, fii_cash_lag1 = _load_fii_signals()
    pcr, pcr_5d_ma   = _load_pcr()

    log.info(f"FII signals: fut_level={fii_fut_level} cash={fii_cash_lag1}")
    log.info(f"PCR: {pcr:.2f} (5d MA: {pcr_5d_ma:.2f})")

    # Init CSV log
    csv_file = open(SIGNAL_LOG, 'w', newline='')
    writer   = None

    _send_telegram(
        f"🚀 <b>V3 Scanner Live</b>\n"
        f"Contract: {fut_symbol}\n"
        f"DTE: {dte} | PCR: {pcr:.2f}\n"
        f"FII: fut={'LONG' if fii_fut_level>0 else 'SHORT' if fii_fut_level<0 else 'NEUTRAL'} "
        f"cash={'BUY' if fii_cash_lag1>0 else 'SELL' if fii_cash_lag1<0 else 'NEUTRAL'}",
        env
    )

    try:
        while True:
            now = datetime.now()
            now_time = now.time()

            if not (MARKET_OPEN <= now_time <= MARKET_CLOSE):
                log.info("Outside market hours. Waiting...")
                save_snapshots(snapshots)
                time.sleep(60)
                continue

            # ── Fetch 1m candles ──────────────────────────────────────────
            try:
                today_str_iso = date.today().isoformat()
                r = g.get_historical_candles(
                    exchange='NSE', segment='FNO', groww_symbol=fut_symbol,
                    start_time=f"{today_str_iso}T09:15:00",
                    end_time=now.strftime('%Y-%m-%dT%H:%M:%S'),
                    candle_interval=g.CANDLE_INTERVAL_MIN_1
                )
                candles = r.get('candles', [])
                if candles:
                    new_df = pd.DataFrame(candles,
                        columns=['ts','open','high','low','close','volume','oi'])
                    new_df['ts'] = pd.to_datetime(new_df['ts'])
                    new_df[['open','high','low','close','volume']] = \
                        new_df[['open','high','low','close','volume']].apply(
                            pd.to_numeric, errors='coerce')
                    new_df['oi'] = pd.to_numeric(new_df['oi'], errors='coerce').ffill()
                    candle_buffer = new_df.tail(CANDLE_WINDOW)
            except Exception as e:
                log.error(f"Candle fetch error: {e}")

            # ── Options chain snapshot (every 5 min) ─────────────────────
            if time.time() - last_options_ts >= OPTIONS_POLL_SECS:
                try:
                    snap = snapshot_once(g, expiry)
                    snapshots.append(snap)
                    last_options_ts = time.time()

                    if len(snapshots) >= 4:
                        velocity_data = compute_oi_velocity(snapshots, window=3)
                    walls = detect_strike_walls(snapshots)
                    log.info(f"Options snapshot: ltp={snap['underlying_ltp']:.0f} "
                             f"call_wall={walls.get('call_wall')} "
                             f"put_wall={walls.get('put_wall')} "
                             f"pcr_live={walls.get('pcr_live',0):.2f}")
                except Exception as e:
                    log.error(f"Options chain error: {e}")

            # ── Compute signals ───────────────────────────────────────────
            fut_ltp  = float(candle_buffer['close'].iloc[-1]) if not candle_buffer.empty else 0
            spot_ltp = _get_spot_ltp(g)
            if spot_ltp == 0:
                spot_ltp = fut_ltp * 0.998  # rough fallback

            state = compute_signal_state(
                df_1m          = candle_buffer if not candle_buffer.empty else None,
                futures_ltp    = fut_ltp,
                spot_ltp       = spot_ltp,
                days_to_expiry = dte,
                pcr            = pcr,
                pcr_5d_ma      = pcr_5d_ma,
                velocity_data  = velocity_data,
                walls          = walls,
                fii_fut_level  = fii_fut_level,
                fii_cash_lag1  = fii_cash_lag1,
                timestamp      = now,
            )

            row = state_to_dict(state)
            log.info(
                f"SIGNAL | ltp={fut_ltp:.0f} dir={'LONG' if state.direction==1 else 'SHORT' if state.direction==-1 else 'NEUTRAL':7s} "
                f"score={state.score:+.3f} fired={state.signal_count}/6 "
                f"pcr_live={state.pcr_live:.2f} "
                f"walls={state.call_wall}/{state.put_wall}"
            )

            # CSV logging
            if writer is None:
                writer = csv.DictWriter(csv_file, fieldnames=row.keys())
                writer.writeheader()
            writer.writerow(row)
            csv_file.flush()

            # ── Telegram alert on direction change ───────────────────────
            if state.direction != 0 and state.direction != last_direction:
                dir_str = "🟢 LONG" if state.direction == 1 else "🔴 SHORT"
                signals_str = (
                    f"OI Quad: {'↑' if state.oi_quadrant>0 else '↓' if state.oi_quadrant<0 else '→'} "
                    f"Basis: {'↑' if state.futures_basis>0 else '↓' if state.futures_basis<0 else '→'} "
                    f"PCR: {'↑' if state.pcr>0 else '↓' if state.pcr<0 else '→'} "
                    f"Vel: {'↑' if state.oi_velocity>0 else '↓' if state.oi_velocity<0 else '→'} "
                    f"Wall: {'↑' if state.strike_defense>0 else '↓' if state.strike_defense<0 else '→'} "
                    f"FII: {'↑' if state.fii_signature>0 else '↓' if state.fii_signature<0 else '→'}"
                )
                msg = (
                    f"<b>V3 Signal: {dir_str}</b>\n"
                    f"LTP: {fut_ltp:.0f} | Score: {state.score:+.3f}\n"
                    f"Signals ({state.signal_count}/6): {signals_str}\n"
                    f"Call wall: {state.call_wall} | Put wall: {state.put_wall}\n"
                    f"PCR live: {state.pcr_live:.2f}\n"
                    f"Time: {now.strftime('%H:%M:%S')}"
                )
                _send_telegram(msg, env)
                log.info(f"ALERT SENT: {dir_str} score={state.score:+.3f}")
                last_direction = state.direction

            time.sleep(POLL_SECS)

    except KeyboardInterrupt:
        log.info("Scanner stopped by user.")
    finally:
        csv_file.close()
        save_snapshots(snapshots)
        log.info(f"Saved {len(snapshots)} snapshots. Signal log: {SIGNAL_LOG}")


if __name__ == '__main__':
    run()
