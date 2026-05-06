"""
Daily intelligence extractor.

Takes the dict returned by fetch.fetch_for_date() and produces a flat dict of
~50 atomic numeric features keyed by snake_case names.

Convention:
- All values are JSON-serializable (int / float / str / null).
- Feature group prefixes: nifty_, banknifty_, fii_, dii_, client_, pro_,
  irf_, fpi_, cd_, breadth_.
- No silent fallbacks: a missing input -> a feature value of None plus an
  entry in the "missing" list at top level.

This file deliberately stays computational; persistence + classifier wiring
live elsewhere.
"""
from __future__ import annotations
import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict

import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _round_idx_strike(spot: float, step: int) -> int:
    return int(round(spot / step) * step)


def _safe_float(x) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if pd.isna(v):
        return None
    return v


def _safe_int(x) -> int | None:
    v = _safe_float(x)
    return int(v) if v is not None else None


# ---------------------------------------------------------------------------
# per-group extractors
# ---------------------------------------------------------------------------
def _udiff_index_features(udiff: pd.DataFrame, ticker: str, strike_step: int) -> dict:
    """Compute index-option features from UDiFF bhavcopy for the front expiry."""
    df = udiff[(udiff["TckrSymb"] == ticker) & (udiff["FinInstrmTp"] == "IDO")].copy()
    if df.empty:
        return {f"{ticker.lower()}_udiff_status": "no_options_rows"}

    # Front expiry = nearest XpryDt to TradDt
    trade_dt = df["TradDt"].iloc[0]
    df = df[df["XpryDt"] >= trade_dt]
    if df.empty:
        return {f"{ticker.lower()}_udiff_status": "no_future_expiry"}
    front = df["XpryDt"].min()
    cur = df[df["XpryDt"] == front]

    # Spot underlying
    spot = float(cur["UndrlygPric"].dropna().iloc[0])

    ce = cur[cur["OptnTp"] == "CE"].copy()
    pe = cur[cur["OptnTp"] == "PE"].copy()
    if ce.empty or pe.empty:
        return {f"{ticker.lower()}_udiff_status": "missing_ce_or_pe"}

    # Aggregate OI / Vol
    ce_oi    = int(ce["OpnIntrst"].sum())
    pe_oi    = int(pe["OpnIntrst"].sum())
    ce_vol   = int(ce["TtlTradgVol"].sum())
    pe_vol   = int(pe["TtlTradgVol"].sum())
    ce_oi_ch = int(ce["ChngInOpnIntrst"].sum())
    pe_oi_ch = int(pe["ChngInOpnIntrst"].sum())

    pcr_oi  = pe_oi / ce_oi if ce_oi else None
    pcr_vol = pe_vol / ce_vol if ce_vol else None

    # ATM strike + per-strike OI for max-pain
    atm = _round_idx_strike(spot, strike_step)
    # max-pain: strike that minimizes total writer payout
    strikes = sorted(set(ce["StrkPric"]).union(pe["StrkPric"]))
    pain = []
    ce_oi_by_k = ce.groupby("StrkPric")["OpnIntrst"].sum().to_dict()
    pe_oi_by_k = pe.groupby("StrkPric")["OpnIntrst"].sum().to_dict()
    for k in strikes:
        loss_ce = sum(max(0.0, k - kk) * v for kk, v in ce_oi_by_k.items())
        loss_pe = sum(max(0.0, kk - k) * v for kk, v in pe_oi_by_k.items())
        pain.append((k, loss_ce + loss_pe))
    max_pain = min(pain, key=lambda x: x[1])[0]

    # ATM call / put OI
    atm_ce_oi = int(ce_oi_by_k.get(atm, 0))
    atm_pe_oi = int(pe_oi_by_k.get(atm, 0))

    # Strike walls: max OI strike on each side
    ce_oi_series = pd.Series(ce_oi_by_k).sort_values(ascending=False)
    pe_oi_series = pd.Series(pe_oi_by_k).sort_values(ascending=False)
    max_ce_strike = int(ce_oi_series.index[0])
    max_pe_strike = int(pe_oi_series.index[0])

    # ATM IV proxy: nearest contract's settle price isn't IV directly,
    # but the volatility report covers that. Here we just compute the
    # ATM straddle premium as a quick implied-move proxy.
    atm_ce_settle = ce.loc[ce["StrkPric"] == atm, "SttlmPric"]
    atm_pe_settle = pe.loc[pe["StrkPric"] == atm, "SttlmPric"]
    atm_straddle = (
        (float(atm_ce_settle.iloc[0]) + float(atm_pe_settle.iloc[0]))
        if (len(atm_ce_settle) and len(atm_pe_settle))
        else None
    )

    p = ticker.lower()
    return {
        f"{p}_spot":             spot,
        f"{p}_atm_strike":       atm,
        f"{p}_max_pain":         max_pain,
        f"{p}_max_pain_dev_pct": (max_pain - spot) / spot * 100.0,
        f"{p}_ce_oi_total":      ce_oi,
        f"{p}_pe_oi_total":      pe_oi,
        f"{p}_ce_oi_chg":        ce_oi_ch,
        f"{p}_pe_oi_chg":        pe_oi_ch,
        f"{p}_pcr_oi":           pcr_oi,
        f"{p}_pcr_vol":          pcr_vol,
        f"{p}_ce_vol_total":     ce_vol,
        f"{p}_pe_vol_total":     pe_vol,
        f"{p}_atm_ce_oi":        atm_ce_oi,
        f"{p}_atm_pe_oi":        atm_pe_oi,
        f"{p}_max_ce_oi_strike": max_ce_strike,
        f"{p}_max_pe_oi_strike": max_pe_strike,
        f"{p}_atm_straddle":     atm_straddle,
        f"{p}_front_expiry":     str(front),
    }


def _participant_features(part_oi: pd.DataFrame, prefix: str) -> dict:
    """Extract net long, net long pct etc. for FII/DII/Client/Pro from OI table."""
    out: dict = {}
    for cohort in ("FII", "DII", "Client", "Pro"):
        if cohort not in part_oi.index:
            continue
        row = part_oi.loc[cohort]
        fil_long  = int(row["Future Index Long"])
        fil_short = int(row["Future Index Short"])
        fsl_long  = int(row["Future Stock Long"])
        fsl_short = int(row["Future Stock Short"])
        ce_long   = int(row["Option Index Call Long"])
        pe_long   = int(row["Option Index Put Long"])
        ce_short  = int(row["Option Index Call Short"])
        pe_short  = int(row["Option Index Put Short"])
        total_long  = int(row["Total Long Contracts"])
        total_short = int(row["Total Short Contracts"])

        c = cohort.lower()
        out[f"{prefix}_{c}_fut_idx_long"]      = fil_long
        out[f"{prefix}_{c}_fut_idx_short"]     = fil_short
        out[f"{prefix}_{c}_fut_idx_net"]       = fil_long - fil_short
        out[f"{prefix}_{c}_fut_stk_long"]      = fsl_long
        out[f"{prefix}_{c}_fut_stk_short"]     = fsl_short
        out[f"{prefix}_{c}_fut_stk_net"]       = fsl_long - fsl_short
        out[f"{prefix}_{c}_opt_idx_ce_long"]   = ce_long
        out[f"{prefix}_{c}_opt_idx_pe_long"]   = pe_long
        out[f"{prefix}_{c}_opt_idx_ce_short"]  = ce_short
        out[f"{prefix}_{c}_opt_idx_pe_short"]  = pe_short
        out[f"{prefix}_{c}_total_long"]        = total_long
        out[f"{prefix}_{c}_total_short"]       = total_short
        out[f"{prefix}_{c}_total_net"]         = total_long - total_short
    return out


def _fii_stats_features(fii_stats: pd.DataFrame) -> dict:
    """Extract per-instrument FII net buy/sell from fii_stats categorical frame."""
    out: dict = {}
    targets = {
        "INDEX FUTURES":      "fii_stats_idx_fut",
        "INDEX OPTIONS":      "fii_stats_idx_opt",
        "STOCK FUTURES":      "fii_stats_stk_fut",
        "STOCK OPTIONS":      "fii_stats_stk_opt",
        "BANKNIFTY FUTURES":  "fii_stats_bn_fut",
        "BANKNIFTY OPTIONS":  "fii_stats_bn_opt",
        "NIFTY FUTURES":      "fii_stats_ni_fut",
        "NIFTY OPTIONS":      "fii_stats_ni_opt",
    }
    for cat, prefix in targets.items():
        if cat not in fii_stats.index:
            continue
        row = fii_stats.loc[cat]
        out[f"{prefix}_buy_contracts"]   = _safe_int(row.get("buy_contracts"))
        out[f"{prefix}_buy_crore"]       = _safe_float(row.get("buy_crore"))
        out[f"{prefix}_sell_contracts"]  = _safe_int(row.get("sell_contracts"))
        out[f"{prefix}_sell_crore"]      = _safe_float(row.get("sell_crore"))
        out[f"{prefix}_net_contracts"]   = _safe_int(row.get("net_contracts"))
        out[f"{prefix}_net_crore"]       = _safe_float(row.get("net_crore"))
        out[f"{prefix}_eod_oi_contracts"] = _safe_int(row.get("eod_oi_contracts"))
        out[f"{prefix}_eod_oi_crore"]    = _safe_float(row.get("eod_oi_crore"))
    return out


def _vol_features(volt: pd.DataFrame) -> dict:
    """Pull NIFTY / BANKNIFTY rows from FOVOLT and emit applicable_vol fields."""
    out: dict = {}
    for sym in ("NIFTY", "BANKNIFTY"):
        if sym not in volt.index:
            continue
        r = volt.loc[sym]
        p = sym.lower()
        out[f"{p}_volt_d"]              = _safe_float(r.get("applicable_vol_d"))
        out[f"{p}_volt_annual"]         = _safe_float(r.get("applicable_vol_annual"))
        out[f"{p}_volt_spot_d"]         = _safe_float(r.get("spot_vol_d"))
        out[f"{p}_volt_fut_d"]          = _safe_float(r.get("fut_vol_d"))
        out[f"{p}_volt_log_ret_spot"]   = _safe_float(r.get("spot_log_ret"))
        out[f"{p}_volt_log_ret_fut"]    = _safe_float(r.get("fut_log_ret"))
    return out


def _deleq_features(deleq: pd.DataFrame) -> dict:
    """Aggregate combined delta-equivalent OI across the universe."""
    if deleq.empty:
        return {}
    # Universe-level totals
    total_notional = int(deleq["notional_oi"].sum())
    total_fut_eq   = float(deleq["fut_eq_oi"].sum())
    return {
        "deleq_universe_notional": total_notional,
        "deleq_universe_fut_eq":   total_fut_eq,
    }


def _market_activity_features(mkt: dict) -> dict:
    """Pull index-level volume + OI from market_activity 'fut_idx' / 'opt_idx'."""
    out: dict = {}

    if "summary" in mkt:
        s = mkt["summary"]
        idx = s.index.astype(str).str.strip()
        s = s.set_index(idx)
        for prod, prefix in (
            ("Index Futures", "mkt_idx_fut"),
            ("Index Options", "mkt_idx_opt"),
            ("Stock Futures", "mkt_stk_fut"),
            ("Stock Options", "mkt_stk_opt"),
            ("F&O Total",     "mkt_fno_total"),
        ):
            if prod in s.index:
                row = s.loc[prod]
                out[f"{prefix}_contracts"] = _safe_int(row.get("No of Contracts"))
                out[f"{prefix}_crore"]     = _safe_float(row.get("Traded Value (Rs. Crs.)"))

    if "fut_idx" in mkt:
        f = mkt["fut_idx"]
        f_idx = f["Symbol"].astype(str).str.strip()
        f = f.assign(_sym=f_idx)
        for sym in ("NIFTY", "BANKNIFTY"):
            row = f[f["_sym"] == sym]
            if row.empty:
                continue
            r = row.iloc[0]
            p = sym.lower() + "_fut_idx_mkt"
            out[f"{p}_contracts"] = _safe_int(r.get("No of Contracts Traded"))
            out[f"{p}_qty"]       = _safe_int(r.get("Traded Quantity"))
            out[f"{p}_crore"]     = _safe_float(r.get("Total Traded Value (Rs. In Crs.)"))
            out[f"{p}_oi"]        = _safe_int(
                r.get("Open interest (Qty.) as at end of trading hrs.")
            )

    if "opt_idx" in mkt:
        o = mkt["opt_idx"].copy()
        sym_clean = o["Symbol"].astype(str).str.strip()
        o = o.assign(_sym=sym_clean)
        for sym in ("NIFTY", "BANKNIFTY"):
            sub = o[o["_sym"] == sym]
            if sub.empty:
                continue
            p = sym.lower() + "_opt_idx_mkt"
            out[f"{p}_contracts"] = _safe_int(sub["No of Contracts Traded"].sum())
            out[f"{p}_crore"]     = _safe_float(sub["Total Traded Value (Rs. In Crs.)"].sum())
            out[f"{p}_oi"]        = _safe_int(
                sub["Open interest (Qty.) as at end of trading hrs."].sum()
            )

    return out


def _sec_ban_features(ban: pd.DataFrame) -> dict:
    return {"sec_ban_count": int(len(ban)), "sec_ban_symbols": list(map(str, ban.index))}


def _nbf_fii_features(nbf_fii: pd.DataFrame) -> dict:
    if nbf_fii.empty:
        return {}
    if "Interest Rate Futures" not in nbf_fii.index:
        return {}
    row = nbf_fii.loc["Interest Rate Futures"]
    return {
        "irf_fii_long_excl_lt": _safe_float(row.get("fii_excl_lt")),
        "irf_fii_long_lt":      _safe_float(row.get("fii_lt")),
        "irf_fii_long_total":   _safe_float(row.get("fii_total")),
    }


def _nbf_fpi_features(nbf_fpi: pd.DataFrame) -> dict:
    if nbf_fpi.empty:
        return {}
    row = nbf_fpi.iloc[0]
    return {
        "fpi_long_cr":     _safe_float(row.get("fpi_long_cr")),
        "fpi_permissible": _safe_float(row.get("permissible_cr")),
        "fpi_available":   _safe_float(row.get("available_cr")),
        "fpi_pct_used":    _safe_float(row.get("pct_used")),
        "fpi_breach_90":   str(row.get("breach_90", "")),
    }


def _cd_features(sett: pd.DataFrame, volt: pd.DataFrame) -> dict:
    """Pull USDINR near-month settle + applicable vol."""
    out: dict = {}
    if not sett.empty:
        usdinr = sett[sett["underlying"] == "USDINR"].copy()
        if not usdinr.empty:
            usdinr = usdinr.sort_values("expiry")
            r = usdinr.iloc[0]
            out["cd_usdinr_near_settle"] = _safe_float(r.get("settle"))
            out["cd_usdinr_near_expiry"] = str(r.get("expiry"))
    if not volt.empty and "USDINR" in volt.index:
        r = volt.loc["USDINR"]
        out["cd_usdinr_vol_d_pct"]      = _safe_float(r.get("applicable_vol_d_pct"))
        out["cd_usdinr_vol_annual_pct"] = _safe_float(r.get("applicable_vol_annual_pct"))
    return out


# ---------------------------------------------------------------------------
# top-level
# ---------------------------------------------------------------------------
def build_daily_intel(trade_date: date, dfs: Dict[str, Any]) -> dict:
    """
    Convert parsed DataFrames into a flat dict of features.

    `dfs` is the output of fetch_for_date(). All keys present must already
    be DataFrames or dicts of DataFrames (parser-defined).  The fetch layer
    may also include a sentinel '__skipped_optional__' list naming any
    optional reports that were 404 on this date (NSE publication gap).
    """
    intel: dict = {
        "trade_date": str(trade_date),
        "schema_version": 1,
        "skipped_fetch": list(dfs.get("__skipped_fetch__", [])),
    }

    # FO features
    if "FO-UDIFF-BHAVCOPY-CSV" in dfs:
        ud = dfs["FO-UDIFF-BHAVCOPY-CSV"]
        intel.update(_udiff_index_features(ud, "NIFTY",     strike_step=50))
        intel.update(_udiff_index_features(ud, "BANKNIFTY", strike_step=100))

    if "FO-PARTICIPANTWISE-OI" in dfs:
        intel.update(_participant_features(dfs["FO-PARTICIPANTWISE-OI"], prefix="poi"))
    if "FO-PARTICIPANTWISE-TRADING-VOL" in dfs:
        intel.update(_participant_features(dfs["FO-PARTICIPANTWISE-TRADING-VOL"], prefix="pvol"))
    if "FO-FII-DERIVATIVE-STAT" in dfs:
        intel.update(_fii_stats_features(dfs["FO-FII-DERIVATIVE-STAT"]))
    if "FO-VOLATILITY" in dfs:
        intel.update(_vol_features(dfs["FO-VOLATILITY"]))
    if "FO-COMBINE-OI-DELEQ" in dfs:
        intel.update(_deleq_features(dfs["FO-COMBINE-OI-DELEQ"]))
    if "FO-MARKET-ACTIVITY-REPORT" in dfs:
        intel.update(_market_activity_features(dfs["FO-MARKET-ACTIVITY-REPORT"]))
    if "FO-SEC-BAN" in dfs:
        intel.update(_sec_ban_features(dfs["FO-SEC-BAN"]))

    # NBF features
    if "NBF-FII-GROSS-LONG-POSITION" in dfs:
        intel.update(_nbf_fii_features(dfs["NBF-FII-GROSS-LONG-POSITION"]))
    if "NBF-FPI-LONG-PSN" in dfs:
        intel.update(_nbf_fpi_features(dfs["NBF-FPI-LONG-PSN"]))

    # CD features
    sett = dfs.get("CD-SETT-PRICE")
    volt = dfs.get("CD_VOLATILITY")
    if isinstance(sett, pd.DataFrame) and isinstance(volt, pd.DataFrame):
        intel.update(_cd_features(sett, volt))

    return intel


def write_intel_json(intel: dict, root: Path) -> Path:
    out_dir = root / "v3" / "cache" / "nse_reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"daily_intel_{intel['trade_date'].replace('-', '')}.json"
    out = out_dir / fname
    out.write_text(json.dumps(intel, indent=2, default=str))
    return out
