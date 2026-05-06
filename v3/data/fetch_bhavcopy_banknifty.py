"""
v3/data/fetch_bhavcopy_banknifty.py
=====================================
Fetch NSE F&O bhavcopy filtered for BANKNIFTY options.
Same source files as fetch_bhavcopy_nifty.py but filters 'BANKNIFTY'.

Cache: v3/cache/bhavcopy_BN_all.pkl
Format: {date_str: DataFrame[strike, ce_oi, pe_oi, ce_vol, pe_vol, ce_ltp, pe_ltp]}

Usage:
    cd "Hawala v2/Hawala v2"
    python v3/data/fetch_bhavcopy_banknifty.py
    python v3/data/fetch_bhavcopy_banknifty.py --force   # rebuild from scratch
"""
import io, sys, pickle, time, requests, zipfile, logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
CACHE_DIR  = ROOT / 'v3' / 'cache'
CACHE_FILE = CACHE_DIR / 'bhavcopy_BN_all.pkl'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('fetch_bhavcopy_banknifty')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept-Encoding': 'gzip, deflate',
    'Accept': '*/*',
    'Referer': 'https://www.nseindia.com',
}

SYMBOL = 'BANKNIFTY'


def _url_new(d: date) -> str:
    return (f"https://archives.nseindia.com/content/fo/"
            f"BhavCopy_NSE_FO_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip")


def _url_old(d: date) -> str:
    month_abbr = d.strftime('%b').upper()
    fname = f"fo{d.strftime('%d%b%Y').upper()}bhav.csv.zip"
    return f"https://archives.nseindia.com/content/historical/DERIVATIVES/{d.year}/{month_abbr}/{fname}"


def _fetch_bn_day(d: date, session: requests.Session) -> pd.DataFrame:
    """
    Fetch bhavcopy for one day, parse BANKNIFTY options only.
    Returns DataFrame[strike, ce_oi, pe_oi, ce_vol, pe_vol, ce_ltp, pe_ltp]

    Raises ValueError if the downloaded file cannot be parsed.
    """
    for url_fn in [_url_new, _url_old]:
        url = url_fn(d)
        try:
            r = session.get(url, timeout=15, headers=HEADERS)
            if r.status_code != 200:
                log.debug("fetch_bn_day: status=%d url=%s", r.status_code, url)
                continue

            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                fname = z.namelist()[0]
                with z.open(fname) as f:
                    raw = pd.read_csv(f, low_memory=False)

            cols = list(raw.columns)

            # ── New format (post-2023): TckrSymb ───────────────────────────────
            if 'TckrSymb' in cols:
                bn = raw[raw['TckrSymb'].str.strip() == SYMBOL].copy()
                if bn.empty:
                    log.debug("fetch_bn_day: %s not found in new-format file for %s", SYMBOL, d)
                    return pd.DataFrame()
                bn = bn[bn['OptnTp'].isin(['CE', 'PE'])].copy()
                if bn.empty:
                    return pd.DataFrame()
                bn['strike'] = pd.to_numeric(bn['StrkPric'], errors='coerce')
                bn['oi']     = pd.to_numeric(bn['OpnIntrst'], errors='coerce').fillna(0)
                bn['vol']    = pd.to_numeric(
                    bn.get('TtlTradgVol', bn.get('TtlNbOfTxsExctd', pd.Series(0, index=bn.index))),
                    errors='coerce').fillna(0)
                bn['ltp']    = pd.to_numeric(
                    bn.get('SttlmPric', bn.get('ClsPric', pd.Series(0, index=bn.index))),
                    errors='coerce').fillna(0)
                opt_col = 'OptnTp'

            # ── Old format: SYMBOL ──────────────────────────────────────────────
            elif 'SYMBOL' in cols or 'Symbol' in cols:
                sym_col = 'SYMBOL' if 'SYMBOL' in cols else 'Symbol'
                raw.columns = [c.strip() for c in raw.columns]
                bn = raw[raw[sym_col].str.strip() == SYMBOL].copy()
                if bn.empty:
                    return pd.DataFrame()
                opt_col = next(
                    (c for c in ['OPTION_TYP', 'OptionType', 'OPTIONTYPE'] if c in bn.columns),
                    None)
                if opt_col is None:
                    raise ValueError(
                        f"fetch_bn_day: option type column not found in old-format file for {d}")
                bn = bn[bn[opt_col].isin(['CE', 'PE'])].copy()
                stk_col = next(
                    (c for c in ['STRIKE_PR', 'StrikePrice', 'STRIKEPRICE'] if c in bn.columns),
                    'STRIKE_PR')
                oi_col  = next(
                    (c for c in ['OPEN_INT', 'OpenInterest', 'OPENINT'] if c in bn.columns),
                    'OPEN_INT')
                vol_col = next(
                    (c for c in ['CONTRACTS', 'CONTRACT', 'VOLUME'] if c in bn.columns),
                    None)
                ltp_col = next(
                    (c for c in ['SETTLE_PR', 'SettlePrice', 'CLOSE', 'CLOSE_PR'] if c in bn.columns),
                    None)
                bn['strike'] = pd.to_numeric(bn[stk_col], errors='coerce')
                bn['oi']     = pd.to_numeric(bn[oi_col],  errors='coerce').fillna(0)
                bn['vol']    = pd.to_numeric(bn[vol_col], errors='coerce').fillna(0) if vol_col else 0
                bn['ltp']    = pd.to_numeric(bn[ltp_col], errors='coerce').fillna(0) if ltp_col else 0
            else:
                raise ValueError(
                    f"fetch_bn_day: unrecognised bhavcopy column format for {d}: cols={cols[:8]}")

            ce = bn[bn[opt_col] == 'CE'].groupby('strike').agg(
                ce_oi=('oi', 'sum'), ce_vol=('vol', 'sum'), ce_ltp=('ltp', 'first')
            ).reset_index()
            pe = bn[bn[opt_col] == 'PE'].groupby('strike').agg(
                pe_oi=('oi', 'sum'), pe_vol=('vol', 'sum'), pe_ltp=('ltp', 'first')
            ).reset_index()

            merged = pd.merge(ce, pe, on='strike', how='outer').fillna(0)
            merged['strike'] = merged['strike'].astype(int)
            merged.sort_values('strike', inplace=True)
            merged.reset_index(drop=True, inplace=True)
            return merged

        except (ValueError, KeyError) as e:
            raise
        except Exception as e:
            log.debug("fetch_bn_day: %s url=%s", e, url)
            continue

    return pd.DataFrame()


def fetch_all(start: date, end: date) -> dict:
    """
    Fetch BANKNIFTY bhavcopy for date range, cache to bhavcopy_BN_all.pkl.
    Returns {date_str: DataFrame}.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if CACHE_FILE.exists():
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        log.info("Loaded existing BANKNIFTY bhavcopy cache: days=%d", len(cache))
    else:
        cache = {}

    session = requests.Session()
    try:
        session.get('https://www.nseindia.com', timeout=10, headers=HEADERS)
        time.sleep(1)
    except Exception as e:
        log.warning("NSE session warm-up failed: %s", e)

    d = start
    added = 0
    while d <= end:
        if d.weekday() < 5 and str(d) not in cache:
            try:
                df = _fetch_bn_day(d, session)
                if not df.empty:
                    cache[str(d)] = df
                    added += 1
                    # Save incrementally so progress survives timeout/interruption
                    with open(CACHE_FILE, 'wb') as _f:
                        pickle.dump(cache, _f)
                    if added % 10 == 0 or added <= 3:
                        log.info(
                            "Fetched date=%s strikes=%d total_days=%d",
                            d, len(df), len(cache),
                        )
                else:
                    log.debug("No BANKNIFTY data for date=%s (holiday or future)", d)
            except ValueError as e:
                raise RuntimeError(
                    f"Unrecoverable parse error on {d}: {e}"
                ) from e
            time.sleep(0.3)
        d += timedelta(days=1)

    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(cache, f)

    log.info(
        "BANKNIFTY bhavcopy cache saved: total_days=%d new_days=%d path=%s",
        len(cache), added, CACHE_FILE,
    )
    return cache


if __name__ == '__main__':
    from datetime import date as dt
    import sys

    force = '--force' in sys.argv
    if force and CACHE_FILE.exists():
        CACHE_FILE.unlink()
        log.info("Cache cleared (--force)")

    # Fetch last 3 months
    end_d   = dt.today()
    start_d = dt(end_d.year - (1 if end_d.month <= 3 else 0),
                 (end_d.month - 3) % 12 or 12, 1)

    log.info("Fetching BANKNIFTY bhavcopy from %s to %s ...", start_d, end_d)
    cache = fetch_all(start_d, end_d)

    print(f"\nBANKNIFTY bhavcopy cache: {len(cache)} days")
    if cache:
        last = max(cache.keys())
        df   = cache[last]
        print(f"Last day: {last}, strikes: {len(df)}, "
              f"strike range: {df['strike'].min()}–{df['strike'].max()}")
        total_ce = df['ce_oi'].sum()
        total_pe = df['pe_oi'].sum()
        print(f"PCR on {last}: {total_pe/total_ce:.3f}  (CE_OI={total_ce:.0f}  PE_OI={total_pe:.0f})")
