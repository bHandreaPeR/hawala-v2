"""
v3/data/fetch_bhavcopy_nifty.py
================================
Fetch NSE F&O bhavcopy filtered for NIFTY options.
Same source files as nse_bhavcopy.py (NSE derivatives) but filters 'NIFTY'
instead of 'BANKNIFTY'.

Cache: v3/cache/bhavcopy_NIFTY_all.pkl
Format: {date_str: DataFrame[strike, ce_oi, pe_oi, ce_vol, pe_vol, ce_ltp, pe_ltp]}

Usage: python v3/data/fetch_bhavcopy_nifty.py
"""
import io, sys, pickle, time, requests, zipfile, logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
CACHE_DIR  = ROOT / 'v3' / 'cache'
CACHE_FILE = CACHE_DIR / 'bhavcopy_NIFTY_all.pkl'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('fetch_bhavcopy_nifty')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept-Encoding': 'gzip, deflate',
    'Accept': '*/*',
    'Referer': 'https://www.nseindia.com',
}

# NSE bhavcopy URLs — same files used for BankNifty (contain all F&O symbols)
def _url_new(d: date) -> str:
    return (f"https://archives.nseindia.com/content/fo/"
            f"BhavCopy_NSE_FO_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip")

def _url_old(d: date) -> str:
    month_abbr = d.strftime('%b').upper()
    fname = f"fo{d.strftime('%d%b%Y').upper()}bhav.csv.zip"
    return f"https://archives.nseindia.com/content/historical/DERIVATIVES/{d.year}/{month_abbr}/{fname}"


def _fetch_nifty_day(d: date, session: requests.Session) -> pd.DataFrame:
    """
    Fetch bhavcopy for one day, parse NIFTY options only.
    Returns DataFrame[strike, ce_oi, pe_oi, ce_vol, pe_vol, ce_ltp, pe_ltp]

    Raises ValueError if the downloaded file cannot be parsed.
    """
    for url_fn in [_url_new, _url_old]:
        url = url_fn(d)
        try:
            r = session.get(url, timeout=15, headers=HEADERS)
            if r.status_code != 200:
                log.debug("fetch_nifty_day: status=%d url=%s", r.status_code, url)
                continue

            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                fname = z.namelist()[0]
                with z.open(fname) as f:
                    raw = pd.read_csv(f, low_memory=False)

            cols = list(raw.columns)

            # ── New format (post-2023): TckrSymb ───────────────────────────────
            if 'TckrSymb' in cols:
                nf = raw[raw['TckrSymb'].str.strip() == 'NIFTY'].copy()
                if nf.empty:
                    log.debug("fetch_nifty_day: NIFTY not found in new-format file for %s", d)
                    return pd.DataFrame()
                nf = nf[nf['OptnTp'].isin(['CE', 'PE'])].copy()
                if nf.empty:
                    return pd.DataFrame()
                nf['strike'] = pd.to_numeric(nf['StrkPric'], errors='coerce')
                nf['oi']     = pd.to_numeric(nf['OpnIntrst'], errors='coerce').fillna(0)
                nf['vol']    = pd.to_numeric(
                    nf.get('TtlTradgVol', nf.get('TtlNbOfTxsExctd', pd.Series(0, index=nf.index))),
                    errors='coerce').fillna(0)
                nf['ltp']    = pd.to_numeric(
                    nf.get('SttlmPric', nf.get('ClsPric', pd.Series(0, index=nf.index))),
                    errors='coerce').fillna(0)
                opt_col = 'OptnTp'

            # ── Old format: SYMBOL ──────────────────────────────────────────────
            elif 'SYMBOL' in cols or 'Symbol' in cols:
                sym_col = 'SYMBOL' if 'SYMBOL' in cols else 'Symbol'
                raw.columns = [c.strip() for c in raw.columns]
                nf = raw[raw[sym_col].str.strip() == 'NIFTY'].copy()
                if nf.empty:
                    return pd.DataFrame()
                opt_col = next(
                    (c for c in ['OPTION_TYP', 'OptionType', 'OPTIONTYPE'] if c in nf.columns),
                    None)
                if opt_col is None:
                    raise ValueError(f"fetch_nifty_day: option type column not found in old-format file for {d}")
                nf = nf[nf[opt_col].isin(['CE', 'PE'])].copy()
                stk_col = next(
                    (c for c in ['STRIKE_PR', 'StrikePrice', 'STRIKEPRICE'] if c in nf.columns),
                    'STRIKE_PR')
                oi_col  = next(
                    (c for c in ['OPEN_INT', 'OpenInterest', 'OPENINT'] if c in nf.columns),
                    'OPEN_INT')
                vol_col = next(
                    (c for c in ['CONTRACTS', 'CONTRACT', 'VOLUME'] if c in nf.columns),
                    None)
                ltp_col = next(
                    (c for c in ['SETTLE_PR', 'SettlePrice', 'CLOSE', 'CLOSE_PR'] if c in nf.columns),
                    None)
                nf['strike'] = pd.to_numeric(nf[stk_col], errors='coerce')
                nf['oi']     = pd.to_numeric(nf[oi_col],  errors='coerce').fillna(0)
                nf['vol']    = pd.to_numeric(nf[vol_col], errors='coerce').fillna(0) if vol_col else 0
                nf['ltp']    = pd.to_numeric(nf[ltp_col], errors='coerce').fillna(0) if ltp_col else 0
            else:
                raise ValueError(
                    f"fetch_nifty_day: unrecognised bhavcopy column format for {d}: cols={cols[:8]}")

            ce = nf[nf[opt_col] == 'CE'].groupby('strike').agg(
                ce_oi=('oi', 'sum'), ce_vol=('vol', 'sum'), ce_ltp=('ltp', 'first')
            ).reset_index()
            pe = nf[nf[opt_col] == 'PE'].groupby('strike').agg(
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
            log.debug("fetch_nifty_day: %s url=%s", e, url)
            continue

    return pd.DataFrame()


def fetch_all(start: date, end: date) -> dict:
    """
    Fetch NIFTY bhavcopy for date range, cache to bhavcopy_NIFTY_all.pkl.
    Returns {date_str: DataFrame}.
    """
    if CACHE_FILE.exists():
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
        log.info("Loaded existing NIFTY bhavcopy cache: days=%d", len(cache))
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
                df = _fetch_nifty_day(d, session)
                if not df.empty:
                    cache[str(d)] = df
                    added += 1
                    # Save incrementally so progress survives timeout/interruption
                    with open(CACHE_FILE, 'wb') as _f:
                        pickle.dump(cache, _f)
                    if added % 10 == 0 or added <= 3:
                        log.info("Fetched date=%s strikes=%d total_days=%d", d, len(df), len(cache))
                else:
                    log.debug("No NIFTY data for date=%s (holiday or future)", d)
            except ValueError as e:
                raise RuntimeError(
                    f"Unrecoverable parse error on {d}: {e}"
                ) from e
            time.sleep(0.3)
        d += timedelta(days=1)

    with open(CACHE_FILE, 'wb') as f:
        pickle.dump(cache, f)

    log.info("NIFTY bhavcopy cache saved: total_days=%d new_days=%d path=%s",
             len(cache), added, CACHE_FILE)
    return cache


if __name__ == '__main__':
    from datetime import date as dt
    import sys

    force = '--force' in sys.argv
    if force and CACHE_FILE.exists():
        CACHE_FILE.unlink()
        log.info("Cache cleared (--force)")

    # Fetch last 3 months — enough to cover current backtest window
    end_d   = dt.today()
    start_d = dt(end_d.year - (1 if end_d.month <= 3 else 0),
                 (end_d.month - 3) % 12 or 12, 1)

    log.info("Fetching NIFTY bhavcopy from %s to %s ...", start_d, end_d)
    cache = fetch_all(start_d, end_d)

    print(f"\nNIFTY bhavcopy cache: {len(cache)} days")
    if cache:
        last = max(cache.keys())
        df   = cache[last]
        print(f"Last day: {last}, strikes: {len(df)}, "
              f"strike range: {df['strike'].min()}–{df['strike'].max()}")
        total_ce = df['ce_oi'].sum()
        total_pe = df['pe_oi'].sum()
        print(f"PCR on {last}: {total_pe/total_ce:.3f}  (CE_OI={total_ce:.0f}  PE_OI={total_pe:.0f})")
