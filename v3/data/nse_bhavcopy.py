"""
v3/data/nse_bhavcopy.py
========================
Fetch NSE F&O bhavcopy (daily) for historical per-strike options OI.
Used to compute PCR, max pain, OI walls across 2024-2025.

Source: https://archives.nseindia.com/content/historical/DERIVATIVES/YYYY/MMM/cm<ddMMMyyyy>bhav.csv.zip
Alt:    https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_<YYYYMMDD>_F_0000.csv.zip

Cache: v3/cache/bhavcopy_BN_<YEAR>.pkl
Format: {date_str: DataFrame[strike, ce_oi, pe_oi, ce_vol, pe_vol, ce_ltp, pe_ltp]}
"""
import os, io, sys, pickle, time, requests, zipfile, logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CACHE_DIR = ROOT / 'v3' / 'cache'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('nse_bhavcopy')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Accept-Encoding': 'gzip, deflate',
    'Accept': '*/*',
    'Referer': 'https://www.nseindia.com',
}

# NSE new bhavcopy URL (post-2023)
# https://archives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip
def _bhav_url_new(d: date) -> str:
    return (f"https://archives.nseindia.com/content/fo/"
            f"BhavCopy_NSE_FO_0_0_0_{d.strftime('%Y%m%d')}_F_0000.csv.zip")

# NSE old bhavcopy URL (pre-2023)
def _bhav_url_old(d: date) -> str:
    month_abbr = d.strftime('%b').upper()
    fname = f"fo{d.strftime('%d%b%Y').upper()}bhav.csv.zip"
    return f"https://archives.nseindia.com/content/historical/DERIVATIVES/{d.year}/{month_abbr}/{fname}"


def _fetch_bhav_day(d: date, session: requests.Session) -> pd.DataFrame:
    """
    Fetch bhavcopy for one day, parse BANKNIFTY options.
    Returns DataFrame with columns: strike, ce_oi, pe_oi, ce_vol, pe_vol, ce_ltp, pe_ltp

    New NSE format (post-2023) columns:
      TckrSymb, OptnTp, StrkPric, OpnIntrst, TtlTradgVol, ClsPric, SttlmPric
    Old NSE format columns:
      SYMBOL, OPTION_TYP, STRIKE_PR, OPEN_INT, CONTRACTS, SETTLE_PR
    """
    for url_fn in [_bhav_url_new, _bhav_url_old]:
        url = url_fn(d)
        try:
            r = session.get(url, timeout=15, headers=HEADERS)
            if r.status_code != 200:
                continue

            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                fname = z.namelist()[0]
                with z.open(fname) as f:
                    raw = pd.read_csv(f, low_memory=False)

            cols = list(raw.columns)

            # ── New format (post-2023): TckrSymb, OptnTp, StrkPric ──────
            if 'TckrSymb' in cols:
                bn = raw[raw['TckrSymb'].str.strip() == 'BANKNIFTY'].copy()
                if bn.empty:
                    continue
                # Filter options only (OptnTp = CE or PE; futures have XX or blank)
                bn = bn[bn['OptnTp'].isin(['CE', 'PE'])].copy()
                if bn.empty:
                    continue
                bn['strike'] = pd.to_numeric(bn['StrkPric'], errors='coerce')
                bn['oi']     = pd.to_numeric(bn['OpnIntrst'], errors='coerce').fillna(0)
                bn['vol']    = pd.to_numeric(bn.get('TtlTradgVol', bn.get('TtlNbOfTxsExctd', 0)),
                                              errors='coerce').fillna(0)
                bn['ltp']    = pd.to_numeric(bn.get('SttlmPric', bn.get('ClsPric', 0)),
                                              errors='coerce').fillna(0)
                opt_col = 'OptnTp'

            # ── Old format: SYMBOL, OPTION_TYP, STRIKE_PR ───────────────
            elif 'SYMBOL' in cols or 'Symbol' in cols:
                sym_col = 'SYMBOL' if 'SYMBOL' in cols else 'Symbol'
                raw.columns = [c.strip() for c in raw.columns]
                bn = raw[raw[sym_col].str.strip() == 'BANKNIFTY'].copy()
                if bn.empty:
                    continue
                opt_col = next((c for c in ['OPTION_TYP','OptionType','OPTIONTYPE'] if c in bn.columns), None)
                if opt_col is None:
                    continue
                bn = bn[bn[opt_col].isin(['CE','PE'])].copy()
                stk_col = next((c for c in ['STRIKE_PR','StrikePrice','STRIKEPRICE'] if c in bn.columns), 'STRIKE_PR')
                oi_col  = next((c for c in ['OPEN_INT','OpenInterest','OPENINT'] if c in bn.columns), 'OPEN_INT')
                vol_col = next((c for c in ['CONTRACTS','CONTRACT','VOLUME'] if c in bn.columns), None)
                ltp_col = next((c for c in ['SETTLE_PR','SettlePrice','CLOSE','CLOSE_PR'] if c in bn.columns), None)
                bn['strike'] = pd.to_numeric(bn[stk_col], errors='coerce')
                bn['oi']     = pd.to_numeric(bn[oi_col],  errors='coerce').fillna(0)
                bn['vol']    = pd.to_numeric(bn[vol_col], errors='coerce').fillna(0) if vol_col else 0
                bn['ltp']    = pd.to_numeric(bn[ltp_col], errors='coerce').fillna(0) if ltp_col else 0
            else:
                continue

            ce = bn[bn[opt_col]=='CE'].groupby('strike').agg(
                ce_oi=('oi','sum'), ce_vol=('vol','sum'), ce_ltp=('ltp','first')).reset_index()
            pe = bn[bn[opt_col]=='PE'].groupby('strike').agg(
                pe_oi=('oi','sum'), pe_vol=('vol','sum'), pe_ltp=('ltp','first')).reset_index()

            merged = pd.merge(ce, pe, on='strike', how='outer').fillna(0)
            merged['strike'] = merged['strike'].astype(int)
            merged.sort_values('strike', inplace=True)
            merged.reset_index(drop=True, inplace=True)
            return merged

        except Exception as e:
            log.debug(f"  {url}: {e}")
            continue

    return pd.DataFrame()


def fetch_and_cache_bhavcopy(start: date, end: date) -> dict:
    """
    Fetch NSE F&O bhavcopy for date range, cache per-year.
    Returns dict: {date_str → DataFrame}
    """
    cache_file = CACHE_DIR / f'bhavcopy_BN_{start.year}_{end.year}.pkl'
    if cache_file.exists():
        with open(cache_file, 'rb') as f:
            cache = pickle.load(f)
    else:
        cache = {}

    log.info(f"Existing bhavcopy cache: {len(cache)} days")

    session = requests.Session()
    # Warm up NSE session
    try:
        session.get('https://www.nseindia.com', timeout=10, headers=HEADERS)
        time.sleep(1)
    except:
        pass

    d = start
    added = 0
    while d <= end:
        if d.weekday() < 5 and str(d) not in cache:
            df = _fetch_bhav_day(d, session)
            if not df.empty:
                cache[str(d)] = df
                added += 1
                if added % 10 == 0:
                    log.info(f"  {d}: {len(df)} strikes | total={len(cache)}")
            time.sleep(0.25)
        d += timedelta(days=1)

    with open(cache_file, 'wb') as f:
        pickle.dump(cache, f)

    log.info(f"Bhavcopy cache saved: {len(cache)} days (+{added} new)")
    return cache


def load_bhavcopy(start: date, end: date) -> dict:
    cache_file = CACHE_DIR / f'bhavcopy_BN_{start.year}_{end.year}.pkl'
    if not cache_file.exists():
        raise FileNotFoundError(f"No bhavcopy cache at {cache_file}. Run fetch_and_cache_bhavcopy() first.")
    with open(cache_file, 'rb') as f:
        return pickle.load(f)


def compute_daily_pcr(bhav_cache: dict) -> pd.DataFrame:
    """
    Compute daily PCR (total PE OI / total CE OI) across all BankNifty strikes.
    Returns DataFrame[date, ce_oi_total, pe_oi_total, pcr, pcr_5d_ma]
    """
    rows = []
    for date_str, df in sorted(bhav_cache.items()):
        if df.empty:
            continue
        ce_total = df['ce_oi'].sum()
        pe_total = df['pe_oi'].sum()
        pcr = pe_total / ce_total if ce_total > 0 else float('nan')
        rows.append({'date': date_str, 'ce_oi_total': ce_total,
                     'pe_oi_total': pe_total, 'pcr': pcr})
    res = pd.DataFrame(rows)
    if not res.empty:
        res['date'] = pd.to_datetime(res['date'])
        res.sort_values('date', inplace=True)
        res['pcr_5d_ma'] = res['pcr'].rolling(5).mean()
        res['pcr_signal'] = res.apply(
            lambda r: 1 if r['pcr'] < 0.8 else (-1 if r['pcr'] > 1.3 else 0), axis=1)
    return res


def compute_max_pain(df_strikes: pd.DataFrame) -> int:
    """
    Max pain = strike where total option writers lose the least.
    Input: DataFrame with strike, ce_oi, pe_oi columns.
    """
    strikes = sorted(df_strikes['strike'].unique())
    min_pain = float('inf')
    max_pain_strike = strikes[len(strikes)//2]

    for test_strike in strikes:
        # CE writers lose if test_strike > strike (calls expire in money)
        ce_loss = df_strikes.apply(
            lambda r: max(0, test_strike - r['strike']) * r['ce_oi'], axis=1).sum()
        # PE writers lose if test_strike < strike
        pe_loss = df_strikes.apply(
            lambda r: max(0, r['strike'] - test_strike) * r['pe_oi'], axis=1).sum()
        total = ce_loss + pe_loss
        if total < min_pain:
            min_pain = total
            max_pain_strike = test_strike

    return max_pain_strike


if __name__ == '__main__':
    from datetime import date
    log.info("Fetching NSE bhavcopy 2024-2025 ...")
    cache = fetch_and_cache_bhavcopy(date(2024, 1, 1), date(2025, 12, 31))
    pcr = compute_daily_pcr(cache)
    print(f"\nPCR table: {len(pcr)} rows")
    print(pcr.tail(10).to_string(index=False))
    pcr.to_csv(CACHE_DIR / 'pcr_daily.csv', index=False)
    print("\nSample max pain (last available day):")
    last_date = max(cache.keys())
    mp = compute_max_pain(cache[last_date])
    print(f"  {last_date}: max pain = {mp}")
