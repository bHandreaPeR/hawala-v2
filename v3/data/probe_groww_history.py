"""
v3/data/probe_groww_history.py
================================
Probe Groww API to determine how far back 1m historical data is available
for BankNifty futures AND options.

Tests one trading day per month going backward from Dec 2025 to Jan 2024.
Reports exact availability so we know how much we can expand the backtest.

Usage:
    python v3/data/probe_groww_history.py

Output:
    Console table + probe_results.json in v3/cache/
"""
import sys, json, time, pyotp, logging
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

logging.basicConfig(level=logging.WARNING, format='%(levelname)s %(message)s')
log = logging.getLogger('probe')


# ── Auth ──────────────────────────────────────────────────────────────────────
def _load_env() -> dict:
    env = {}
    with open(ROOT / 'token.env') as f:
        for line in f:
            if '=' in line:
                k, _, v = line.strip().partition('=')
                env[k] = v
    return env

def _get_groww():
    from growwapi import GrowwAPI
    env   = _load_env()
    totp  = pyotp.TOTP(env['GROWW_TOTP_SECRET']).now()
    token = GrowwAPI.get_access_token(api_key=env['GROWW_API_KEY'], totp=totp)
    return GrowwAPI(token=token)


# ── Contract helpers ──────────────────────────────────────────────────────────
def _last_tuesday(year: int, month: int) -> date:
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    days_back = (last_day.weekday() - 1) % 7
    return last_day - timedelta(days=days_back)

EXPIRY_OVERRIDES = {
    date(2026, 3, 31): date(2026, 3, 30),
}

def _get_expiry(trade_date: date) -> date:
    y, m = trade_date.year, trade_date.month
    for _ in range(3):
        exp = _last_tuesday(y, m)
        exp = EXPIRY_OVERRIDES.get(exp, exp)
        if exp >= trade_date:
            return exp
        m = m + 1 if m < 12 else 1
        y = y if m > 1 else y + 1
    raise RuntimeError(f"No expiry found for {trade_date}")

def _fut_symbol(expiry: date) -> str:
    return f"NSE-BANKNIFTY-{expiry.day}{expiry.strftime('%b')}{expiry.strftime('%y')}-FUT"

def _opt_symbol(expiry: date, strike: int, side: str) -> str:
    return f"NSE-BANKNIFTY-{expiry.day}{expiry.strftime('%b')}{expiry.strftime('%y')}-{strike}-{side}"

# Approximate BankNifty ATM by period (round 500 to nearest 100)
# Used only to pick a test strike — doesn't need to be exact.
# If this strike has no data we also try ±1000.
APPROX_ATM = {
    2024: {1:48500, 2:46000, 3:47000, 4:48500, 5:48500, 6:49000,
           7:51000, 8:50000, 9:53500, 10:52000, 11:52500, 12:53500},
    2025: {1:49000, 2:48000, 3:49000, 4:51000, 5:54500, 6:54500,
           7:53000, 8:51500, 9:53500, 10:52500, 11:51500, 12:53000},
}

def _test_month(g, year: int, month: int) -> dict:
    """
    Test data availability for one month.
    Uses the 10th calendar day (skip weekends) as test date.
    Returns dict with keys: date, fut_candles, opt_candles, fut_ok, opt_ok
    """
    # Pick test date: 10th of month, skip to next weekday if needed
    td = date(year, month, 10)
    while td.weekday() >= 5:
        td += timedelta(days=1)

    expiry = _get_expiry(td)
    approx_atm = APPROX_ATM.get(year, {}).get(month, 50000)
    strike = round(approx_atm / 100) * 100  # nearest 100

    fut_sym = _fut_symbol(expiry)
    start   = f"{td}T09:15:00"
    end     = f"{td}T09:25:00"  # just 10 min — enough to confirm data exists

    result = {
        'date': str(td), 'expiry': str(expiry),
        'fut_symbol': fut_sym,
        'opt_symbol': _opt_symbol(expiry, strike, 'CE'),
        'fut_candles': 0, 'opt_candles': 0,
        'fut_ok': False, 'opt_ok': False,
    }

    # Test futures
    try:
        r = g.get_historical_candles(
            exchange='NSE', segment='FNO', groww_symbol=fut_sym,
            start_time=start, end_time=end,
            candle_interval=g.CANDLE_INTERVAL_MIN_1
        )
        n = len(r.get('candles', []))
        result['fut_candles'] = n
        result['fut_ok'] = n > 0
    except Exception as e:
        result['fut_error'] = str(e)
    time.sleep(0.3)

    # Test options — try ATM ± offsets until we get data
    opt_ok = False
    for offset in [0, 1000, -1000, 2000, -2000, 500, -500]:
        test_strike = strike + offset
        sym = _opt_symbol(expiry, test_strike, 'CE')
        try:
            r = g.get_historical_candles(
                exchange='NSE', segment='FNO', groww_symbol=sym,
                start_time=start, end_time=end,
                candle_interval=g.CANDLE_INTERVAL_MIN_1
            )
            n = len(r.get('candles', []))
            if n > 0:
                result['opt_candles'] = n
                result['opt_symbol']  = sym
                result['opt_strike']  = test_strike
                result['opt_ok']      = True
                opt_ok = True
                break
        except Exception as e:
            result['opt_error'] = str(e)
        time.sleep(0.2)

    if not opt_ok:
        result['opt_ok'] = False

    return result


def main():
    print("Probing Groww 1m history for BankNifty futures + options...")
    print("(Testing one day per month, 10 minute window — ~0.5s per test)\n")

    g = _get_groww()

    # Validate auth first
    try:
        r = g.get_historical_candles(
            exchange='NSE', segment='FNO',
            groww_symbol='NSE-BANKNIFTY-28Apr26-FUT',
            start_time='2026-04-28T09:15:00',
            end_time='2026-04-28T09:20:00',
            candle_interval=g.CANDLE_INTERVAL_MIN_1
        )
        if not r.get('candles'):
            print("ERROR: Auth check failed — Apr 2026 futures returned 0 candles. Refresh token.")
            sys.exit(1)
        print(f"Auth OK — Apr 2026 futures: {len(r['candles'])} candles\n")
    except Exception as e:
        print(f"ERROR: Auth failed: {e}")
        sys.exit(1)

    # Probe months: Dec 2025 → Jan 2024 (backward)
    months_to_probe = []
    for year in [2025, 2024]:
        for month in range(12, 0, -1):
            months_to_probe.append((year, month))

    print(f"{'Month':<12} {'TestDate':<12} {'FutSym':<38} {'FUT':>5} {'OPT':>5} {'FutBars':>8} {'OptBars':>8}")
    print("─" * 100)

    all_results = []
    earliest_fut = None
    earliest_opt = None

    for year, month in months_to_probe:
        r = _test_month(g, year, month)
        all_results.append(r)

        fut_str = f"✓ {r['fut_candles']:2d}bars" if r['fut_ok'] else "✗"
        opt_str = f"✓ {r['opt_candles']:2d}bars" if r['opt_ok'] else "✗"

        print(
            f"{year}-{month:02d}      "
            f"{r['date']:<12} "
            f"{r['fut_symbol']:<38} "
            f"{'YES' if r['fut_ok'] else 'NO':>5} "
            f"{'YES' if r['opt_ok'] else 'NO':>5} "
            f"{r['fut_candles']:>8} "
            f"{r['opt_candles']:>8}"
        )

        if r['fut_ok']:
            earliest_fut = r['date']
        if r['opt_ok']:
            earliest_opt = r['date']

        time.sleep(0.4)  # stay under rate limit

    print("\n" + "═" * 100)
    print(f"Earliest FUTURES 1m data : {earliest_fut or 'NOT FOUND in tested range'}")
    print(f"Earliest OPTIONS 1m data : {earliest_opt or 'NOT FOUND in tested range'}")

    # Backtest data expansion estimate
    if earliest_opt:
        from datetime import datetime
        start = datetime.strptime(earliest_opt, '%Y-%m-%d').date()
        end   = date(2026, 4, 30)
        cal_days   = (end - start).days
        est_trading = int(cal_days * 5 / 7 * 0.96)  # ~96% of weekdays are trading days
        est_api_min = est_trading * 40 * 2 * 0.4 / 60  # 40 strikes × 2 sides × 0.4s
        print(f"\nData expansion estimate:")
        print(f"  From {earliest_opt} to 2026-04-30 = ~{est_trading} trading days")
        print(f"  Option OI fetch time estimate: ~{est_api_min:.0f} minutes ({est_api_min/60:.1f} hours)")
        print(f"  Futures fetch time estimate: ~{est_trading * 0.4 / 60:.0f} minutes")

    # Save results
    out = ROOT / 'v3' / 'cache' / 'probe_results.json'
    with open(out, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\nFull results saved → {out}")

    return all_results


if __name__ == '__main__':
    main()
