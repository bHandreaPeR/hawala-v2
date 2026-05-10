"""
test_groww_expired.py
=====================
Probe Groww API for expired option contracts.
All expiry dates are discovered via get_expiries() — never hardcoded.

Run: python test_groww_expired.py
"""
import pyotp, time, json
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parent


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


def pp(label, data):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print('='*60)
    print(json.dumps(data, indent=2, default=str))


g = _get_groww()
print("Auth OK\n")


# ── 1. Discover actual expiries from the API ───────────────────────────────────
# Do NOT assume weekday — let the API tell us what dates exist

print(">>> TEST 1: get_expiries — discover actual dates (no assumptions)")

discovered = {}   # (exchange, symbol) → [exp_date, ...]

for exchange, symbol, year, month in [
    ('NSE', 'BANKNIFTY', 2026, 4),
    ('NSE', 'NIFTY',     2026, 4),
    ('BSE', 'SENSEX',    2026, 4),
    ('NSE', 'BANKNIFTY', 2026, 3),
    ('NSE', 'NIFTY',     2026, 3),
]:
    try:
        r = g.get_expiries(exchange=exchange, underlying_symbol=symbol,
                           year=year, month=month)
        exps = sorted(r.get('expiries', []))
        discovered[(exchange, symbol, year, month)] = exps
        # Show weekday for each expiry date
        annotated = []
        for e in exps:
            d = date.fromisoformat(e)
            annotated.append(f"{e}({d.strftime('%a')})")
        print(f"  {exchange}-{symbol} {year}-{month:02d}: {annotated}")
    except Exception as e:
        print(f"  {exchange}-{symbol} {year}-{month:02d}: ERROR {e}")
    time.sleep(0.3)


# ── 2. get_contracts for each discovered expiry ────────────────────────────────

print("\n\n>>> TEST 2: get_contracts for discovered expiries")

for (exchange, symbol, year, month), exps in discovered.items():
    for expiry in exps:
        try:
            r = g.get_contracts(exchange=exchange, underlying_symbol=symbol,
                                expiry_date=expiry)
            contracts = r.get('contracts', r)
            count  = len(contracts) if isinstance(contracts, list) else '?'
            sample = contracts[:2] if isinstance(contracts, list) else contracts
            print(f"  {exchange}-{symbol} exp={expiry}: count={count}  sample={sample}")
        except Exception as e:
            print(f"  {exchange}-{symbol} exp={expiry}: ERROR {e}")
        time.sleep(0.2)


# ── 3. Candle data for specific expired option strikes ────────────────────────
# Uses discovered expiries — no hardcoded dates

print("\n\n>>> TEST 3: get_historical_candles for expired options")

# Build test cases dynamically from discovered expiries
test_cases = []

# Sensex Apr 30 expiry (Thursday) — the 76500 CE trade
sensex_apr_exps = discovered.get(('BSE', 'SENSEX', 2026, 4), [])
for exp in sensex_apr_exps:
    d = date.fromisoformat(exp)
    test_cases.append({
        'label':      f"SENSEX 76500 CE exp={exp}({d.strftime('%a')}) trade=Apr30",
        'exchange':   'BSE',
        'symbol':     f"BSE-SENSEX-{d.day}{d.strftime('%b')}{d.strftime('%y')}-76500-CE",
        'trade_date': '2026-04-30',
    })

# BankNifty Apr 30 expiry — two backtest trades
bn_apr_exps = discovered.get(('NSE', 'BANKNIFTY', 2026, 4), [])
for exp in bn_apr_exps:
    d = date.fromisoformat(exp)
    for strike, trade_date in [(54900, '2026-04-08'), (56700, '2026-04-16')]:
        test_cases.append({
            'label':      f"BN {strike} CE exp={exp}({d.strftime('%a')}) trade={trade_date}",
            'exchange':   'NSE',
            'symbol':     f"NSE-BANKNIFTY-{d.day}{d.strftime('%b')}{d.strftime('%y')}-{strike}-CE",
            'trade_date': trade_date,
        })

# Nifty — use last discovered April expiry
nifty_apr_exps = discovered.get(('NSE', 'NIFTY', 2026, 4), [])
if nifty_apr_exps:
    exp = nifty_apr_exps[-1]
    d   = date.fromisoformat(exp)
    atm = 24000  # approximate Nifty level late April
    test_cases.append({
        'label':      f"NIFTY {atm} CE exp={exp}({d.strftime('%a')}) trade=Apr27",
        'exchange':   'NSE',
        'symbol':     f"NSE-NIFTY-{d.day}{d.strftime('%b')}{d.strftime('%y')}-{atm}-CE",
        'trade_date': '2026-04-27',
    })

for tc in test_cases:
    start = f"{tc['trade_date']}T09:15:00"
    end   = f"{tc['trade_date']}T15:30:00"
    try:
        r = g.get_historical_candles(
            exchange=tc['exchange'], segment='FNO', groww_symbol=tc['symbol'],
            start_time=start, end_time=end,
            candle_interval=g.CANDLE_INTERVAL_MIN_1,
        )
        candles = r.get('candles', [])
        if candles:
            first, last = candles[0], candles[-1]
            print(f"  OK    {tc['label']}")
            print(f"        count={len(candles)}  open={first[1]}  "
                  f"first_close={first[4]}  last_close={last[4]}  "
                  f"last_oi={last[6] if len(last) > 6 else 'N/A'}")
        else:
            print(f"  EMPTY {tc['label']}  raw_keys={list(r.keys())}")
    except Exception as e:
        print(f"  ERR   {tc['label']}: {e}")
    time.sleep(0.4)


# ── 4. Probe Sensex symbol format ─────────────────────────────────────────────
# Only runs if we have a discovered Sensex expiry to test against

print("\n\n>>> TEST 4: Sensex symbol format probe (using discovered expiry)")

if sensex_apr_exps:
    exp = sensex_apr_exps[-1]   # last April expiry
    d   = date.fromisoformat(exp)
    print(f"  Using expiry: {exp} ({d.strftime('%A')})")

    formats = [
        f"BSE-SENSEX-{d.day}{d.strftime('%b')}{d.strftime('%y')}-76500-CE",       # 30Apr26
        f"BSE-SENSEX-{d.strftime('%d')}{d.strftime('%b')}{d.strftime('%y')}-76500-CE",  # 30Apr26 (zero-padded)
        f"BSE-SENSEX-{d.day}{d.strftime('%b').upper()}{d.strftime('%y')}-76500-CE",  # 30APR26
        f"BSE-SENSEX-{d.day}{d.strftime('%b')}{d.year}-76500-CE",                  # 30Apr2026
        f"BSE-SENSEX-{d.day}{d.strftime('%b').upper()}{d.year}-76500-CE",           # 30APR2026
    ]

    for sym in formats:
        try:
            r = g.get_historical_candles(
                exchange='BSE', segment='FNO', groww_symbol=sym,
                start_time=f"{exp}T09:15:00", end_time=f"{exp}T09:30:00",
                candle_interval=g.CANDLE_INTERVAL_MIN_1,
            )
            candles = r.get('candles', [])
            status = f"OK  count={len(candles)}  first_close={candles[0][4] if candles else None}"
        except Exception as e:
            status = f"ERR {e}"
        print(f"  {sym:48s}  {status}")
        time.sleep(0.3)
else:
    print("  No Sensex April expiries discovered — skipping")
