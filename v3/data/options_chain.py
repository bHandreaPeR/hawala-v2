"""
v3/data/options_chain.py
=========================
Live options chain poller.
Snapshots per-strike OI every N minutes during market hours.
Builds historical OI database for velocity + strike defense detection.

Cache: v3/cache/options_chain_snapshots.pkl
Format: list of {ts, underlying_ltp, strikes: {strike: {CE: {oi, ltp, iv}, PE: {...}}}}
"""
import os, sys, pickle, time, pyotp, logging
from datetime import datetime, date, time as dtime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
CACHE_FILE = ROOT / 'v3' / 'cache' / 'options_chain_snapshots.pkl'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('options_chain')

MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 30)
POLL_INTERVAL_SECS = 300    # every 5 minutes


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
    return GrowwAPI(token=token)


def _get_active_expiry(g) -> str:
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
                if date.fromisoformat(exp) >= today:
                    return exp
        except:
            pass
        time.sleep(0.3)
    raise RuntimeError("No active BankNifty expiry found")


def snapshot_once(g, expiry: str) -> dict:
    """Take a single options chain snapshot."""
    ts = datetime.now().replace(microsecond=0)
    oc = g.get_option_chain(exchange='NSE', underlying='BANKNIFTY', expiry_date=expiry)
    raw_strikes = oc.get('strikes', {})
    underlying_ltp = oc.get('underlying_ltp', 0)

    strikes = {}
    for strike_str, data in raw_strikes.items():
        strike = int(strike_str)
        entry = {}
        for side in ['CE', 'PE']:
            s = data.get(side, {})
            entry[side] = {
                'oi':  s.get('open_interest', 0),
                'ltp': s.get('ltp', 0),
                'iv':  s.get('greeks', {}).get('iv', 0) if s.get('greeks') else 0,
                'delta': s.get('greeks', {}).get('delta', 0) if s.get('greeks') else 0,
            }
        strikes[strike] = entry

    return {'ts': ts, 'underlying_ltp': underlying_ltp, 'strikes': strikes}


def load_snapshots() -> list:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, 'rb') as f:
            return pickle.load(f)
    return []


def save_snapshots(snapshots: list):
    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(snapshots, f)


def run_poller(duration_mins: int = None):
    """
    Poll options chain every POLL_INTERVAL_SECS during market hours.
    duration_mins: max run time. None = run until market close.
    """
    g = _get_groww()
    snapshots = load_snapshots()
    log.info(f"Existing snapshots: {len(snapshots)}")

    expiry = _get_active_expiry(g)
    log.info(f"Active expiry: {expiry}")

    start_time = datetime.now()
    count = 0

    while True:
        now = datetime.now()
        now_time = now.time()

        # Check market hours
        if not (MARKET_OPEN <= now_time <= MARKET_CLOSE):
            log.info(f"Outside market hours ({now_time}). Stopping.")
            break

        # Check duration
        if duration_mins and (now - start_time).total_seconds() > duration_mins * 60:
            log.info("Duration limit reached.")
            break

        try:
            snap = snapshot_once(g, expiry)
            snapshots.append(snap)
            count += 1
            n_strikes = len(snap['strikes'])
            log.info(f"Snapshot #{count} at {snap['ts']}: ltp={snap['underlying_ltp']}, strikes={n_strikes}")

            # Save every 5 snapshots
            if count % 5 == 0:
                save_snapshots(snapshots)
                log.info(f"Saved {len(snapshots)} total snapshots")
        except Exception as e:
            log.error(f"Snapshot failed: {e}")

        time.sleep(POLL_INTERVAL_SECS)

    save_snapshots(snapshots)
    log.info(f"Poller done. Total snapshots: {len(snapshots)}")


# ── Analysis helpers ──────────────────────────────────────────────────────────

def compute_oi_velocity(snapshots: list, window: int = 3) -> dict:
    """
    OI velocity = rate of OI change over last `window` snapshots per strike.
    Returns {strike: {CE_velocity, PE_velocity, net_velocity}}
    High positive CE velocity → call writing (bearish)
    High positive PE velocity → put writing (bullish)
    """
    if len(snapshots) < window + 1:
        return {}

    recent = snapshots[-(window+1):]
    first  = recent[0]
    last   = recent[-1]
    time_delta_mins = max(1, (last['ts'] - first['ts']).total_seconds() / 60)

    all_strikes = set(last['strikes'].keys())
    result = {}

    for strike in all_strikes:
        try:
            ce_now  = last['strikes'][strike]['CE']['oi']
            pe_now  = last['strikes'][strike]['PE']['oi']
            ce_then = first['strikes'].get(strike, {}).get('CE', {}).get('oi', ce_now)
            pe_then = first['strikes'].get(strike, {}).get('PE', {}).get('oi', pe_now)

            ce_vel = (ce_now - ce_then) / time_delta_mins   # contracts/min
            pe_vel = (pe_now - pe_then) / time_delta_mins
            net_vel = pe_vel - ce_vel   # positive = put writing > call writing = bullish

            result[strike] = {
                'ce_oi': ce_now, 'pe_oi': pe_now,
                'ce_velocity': round(ce_vel, 2),
                'pe_velocity': round(pe_vel, 2),
                'net_velocity': round(net_vel, 2),
            }
        except:
            continue

    return result


def detect_strike_walls(snapshots: list, oi_threshold_pct: float = 0.15) -> dict:
    """
    Strike wall = strike where OI >= `oi_threshold_pct` of total OI.
    These are levels FII/institutions are defending.
    Returns {side: [strike, ...], 'call_wall': int, 'put_wall': int}
    """
    if not snapshots:
        return {}

    snap = snapshots[-1]
    strikes_data = snap['strikes']
    ltp = snap['underlying_ltp']

    total_ce_oi = sum(v['CE']['oi'] for v in strikes_data.values())
    total_pe_oi = sum(v['PE']['oi'] for v in strikes_data.values())

    call_walls = []
    put_walls  = []
    call_wall_strike = None
    put_wall_strike  = None
    max_call_oi = 0
    max_put_oi  = 0

    for strike, data in sorted(strikes_data.items()):
        ce_oi = data['CE']['oi']
        pe_oi = data['PE']['oi']

        if total_ce_oi > 0 and ce_oi / total_ce_oi >= oi_threshold_pct:
            call_walls.append(strike)
        if total_pe_oi > 0 and pe_oi / total_pe_oi >= oi_threshold_pct:
            put_walls.append(strike)

        if ce_oi > max_call_oi and strike > ltp:
            max_call_oi = ce_oi
            call_wall_strike = strike
        if pe_oi > max_put_oi and strike < ltp:
            max_put_oi = pe_oi
            put_wall_strike = strike

    return {
        'call_walls': call_walls,
        'put_walls': put_walls,
        'call_wall': call_wall_strike,    # strongest call OI above LTP (resistance)
        'put_wall': put_wall_strike,       # strongest put OI below LTP (support)
        'ltp': ltp,
        'pcr_live': total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0,
    }


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'poll':
        run_poller()
    else:
        # Show current snapshot analysis
        snaps = load_snapshots()
        print(f"Total snapshots: {len(snaps)}")
        if snaps:
            print(f"First: {snaps[0]['ts']}, Last: {snaps[-1]['ts']}")
            walls = detect_strike_walls(snaps)
            print(f"\nStrike walls: {walls}")
            if len(snaps) >= 4:
                vel = compute_oi_velocity(snaps, window=3)
                top_ce = sorted(vel.items(), key=lambda x: -abs(x[1]['ce_velocity']))[:5]
                print(f"\nTop OI velocity strikes:")
                for s, v in top_ce:
                    print(f"  {s}: CE_vel={v['ce_velocity']:.1f} PE_vel={v['pe_velocity']:.1f} net={v['net_velocity']:.1f}")
