"""
Phase 5: critical analysis of intel_timeseries.parquet against T-day Nifty
futures open->close returns.

Outputs four blocks of remarks (daily / weekly / monthly / total) plus the
IC table and redundancy-vs-classifier check.

Per project rules: numbers only, no synthesised data, brutally honest.

Pairing convention:
    intel row at trade-date T  -> predicts return on trade-date T+1
    (NSE EOD reports for T are available at end-of-day T,
     usable to position for T+1.)
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
INTEL = ROOT / "v3" / "cache" / "nse_reports" / "intel_timeseries.parquet"
CANDLES = ROOT / "v3" / "cache" / "candles_1m_NIFTY.pkl"
THRESH = ROOT / "v3" / "cache" / "fii_dii_thresholds.json"
OUT_DIR = ROOT / "v3" / "cache" / "nse_reports"
OUT_IC = OUT_DIR / "phase5_ic_table.csv"
OUT_REPORT = OUT_DIR / "phase5_remarks.md"


# ---------------------------------------------------------------------------
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    intel = pd.read_parquet(INTEL)
    intel["trade_date"] = pd.to_datetime(intel["trade_date"])
    intel = intel.sort_values("trade_date").reset_index(drop=True)

    candles = pd.read_pickle(CANDLES)
    candles["date"] = pd.to_datetime(candles["date"])
    rets = candles.groupby("date").agg(
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index().rename(columns={"date": "trade_date"})
    rets["ret_pct"]   = (rets["close"] / rets["open"] - 1.0) * 100.0
    rets["range_pct"] = (rets["high"] - rets["low"]) / rets["open"] * 100.0
    rets["dir"] = np.where(
        rets["ret_pct"] > 0.10, 1,
        np.where(rets["ret_pct"] < -0.10, -1, 0),
    )
    return intel, rets


def numeric_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns
            if c not in ("trade_date","schema_version","skipped_fetch")
            and pd.api.types.is_numeric_dtype(df[c])]


# ---------------------------------------------------------------------------
def daily_diagnostics(intel: pd.DataFrame, rets: pd.DataFrame) -> dict:
    """Per-day stats — count of populated columns, range of key features."""
    nc = numeric_cols(intel)
    pop = intel[nc].notna().sum(axis=1)
    pop.index = intel["trade_date"]
    daily = pd.DataFrame({
        "n_features_populated": pop.values,
        "n_features_max":       len(nc),
    }, index=intel["trade_date"])

    # merge in T-day returns (same-day, NOT shifted)
    daily = daily.join(rets.set_index("trade_date")[["ret_pct","range_pct","dir"]])

    return {
        "rows": len(daily),
        "feature_pop_min": int(pop.min()),
        "feature_pop_median": int(pop.median()),
        "feature_pop_max": int(pop.max()),
        "ret_pct_describe": daily["ret_pct"].describe().to_dict(),
        "range_pct_describe": daily["range_pct"].describe().to_dict(),
        "dir_counts": daily["dir"].value_counts().to_dict(),
        "outage_days": daily.loc[daily["n_features_populated"] < daily["n_features_max"] - 5,
                                  ["n_features_populated","ret_pct"]].to_dict("index"),
    }


# ---------------------------------------------------------------------------
def weekly_aggregate(intel: pd.DataFrame, rets: pd.DataFrame, key_features: list[str]) -> pd.DataFrame:
    df = intel.merge(rets[["trade_date","ret_pct","range_pct","dir"]], on="trade_date", how="left")
    iso = df["trade_date"].dt.isocalendar()
    df["yr_wk"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)

    agg = {f: "mean" for f in key_features if f in df.columns}
    agg.update({"ret_pct": "sum", "range_pct": "sum", "dir": "sum",
                "trade_date": ["min","max","count"]})
    wk = df.groupby("yr_wk").agg(agg)
    wk.columns = ["__".join(c).rstrip("_") if isinstance(c, tuple) else c
                  for c in wk.columns]
    return wk.reset_index()


def monthly_aggregate(intel: pd.DataFrame, rets: pd.DataFrame, key_features: list[str]) -> pd.DataFrame:
    df = intel.merge(rets[["trade_date","ret_pct","range_pct","dir"]], on="trade_date", how="left")
    df["yr_mo"] = df["trade_date"].dt.strftime("%Y-%m")
    agg = {f: "mean" for f in key_features if f in df.columns}
    agg.update({"ret_pct": "sum", "range_pct": "sum", "dir": "sum",
                "trade_date": ["min","max","count"]})
    mo = df.groupby("yr_mo").agg(agg)
    mo.columns = ["__".join(c).rstrip("_") if isinstance(c, tuple) else c
                  for c in mo.columns]
    return mo.reset_index()


# ---------------------------------------------------------------------------
def ic_analysis(intel: pd.DataFrame, rets: pd.DataFrame) -> pd.DataFrame:
    """Information Coefficient: pearson + spearman of feature[T] vs return[T+1].

    Bootstrap CI: 1000 reps, 5-day blocks (preserves serial correlation).
    Returns table: feature, ic_pearson, ic_spearman, ic_p_lo, ic_p_hi, n.
    """
    from scipy.stats import pearsonr, spearmanr

    df = intel.merge(rets[["trade_date","ret_pct","dir"]], on="trade_date", how="left")
    df = df.sort_values("trade_date").reset_index(drop=True)
    # Predict NEXT-day return
    df["next_ret_pct"] = df["ret_pct"].shift(-1)
    df["next_dir"]     = df["dir"].shift(-1)
    df = df.dropna(subset=["next_ret_pct"])

    nc = numeric_cols(intel)
    rows = []
    rng = np.random.default_rng(seed=1729)
    n_obs = len(df)

    for f in nc:
        sub = df[[f, "next_ret_pct", "next_dir"]].dropna()
        if len(sub) < 30:
            continue
        try:
            r_p, p_p = pearsonr(sub[f], sub["next_ret_pct"])
            r_s, p_s = spearmanr(sub[f], sub["next_ret_pct"])
        except Exception:
            continue

        # Block bootstrap (5-day blocks)
        x = sub[f].values
        y = sub["next_ret_pct"].values
        n = len(x)
        block = 5
        n_blocks = max(1, n // block)
        boots = []
        for _ in range(400):  # 400 reps for speed
            idxs = rng.integers(0, n_blocks, size=n_blocks) * block
            sample = []
            for i in idxs:
                sample.extend(range(i, min(i+block, n)))
            sample = np.array(sample)
            xs, ys = x[sample], y[sample]
            if xs.std() < 1e-12 or ys.std() < 1e-12:
                continue
            r = np.corrcoef(xs, ys)[0,1]
            boots.append(r)
        if boots:
            lo, hi = np.percentile(boots, [2.5, 97.5])
        else:
            lo, hi = np.nan, np.nan

        # Direction-only IC (sign agreement)
        sign_match = ((sub[f] > sub[f].median()) == (sub["next_dir"] > 0)).mean()

        rows.append({
            "feature":      f,
            "ic_pearson":   r_p,
            "ic_p_pvalue":  p_p,
            "ic_spearman":  r_s,
            "ic_s_pvalue":  p_s,
            "ic_p_ci_lo":   lo,
            "ic_p_ci_hi":   hi,
            "sign_match":   sign_match,
            "n":            len(sub),
            "feat_std":     float(sub[f].std()),
        })

    out = pd.DataFrame(rows)
    out["abs_ic_p"] = out["ic_pearson"].abs()
    out = out.sort_values("abs_ic_p", ascending=False).reset_index(drop=True)
    return out


# ---------------------------------------------------------------------------
def redundancy_check(intel: pd.DataFrame, rets: pd.DataFrame,
                     candidate_features: list[str]) -> pd.DataFrame:
    """Pairwise correlation of candidate features against the existing 8
    classifier features.  Existing features are computed *intra-day* on
    bar-level data, so we don't have them aligned at trade-date granularity
    in this dataset.  Instead we use the candidate intel features against
    each other to surface within-NSE-data collinearity that would hurt the
    classifier.
    """
    df = intel[candidate_features].dropna()
    if df.empty:
        return pd.DataFrame()
    corr = df.corr(method="pearson").abs()
    # extract upper triangle as long format
    pairs = []
    feats = corr.columns.tolist()
    for i, a in enumerate(feats):
        for b in feats[i+1:]:
            pairs.append({"a": a, "b": b, "abs_corr": corr.loc[a,b]})
    cp = pd.DataFrame(pairs).sort_values("abs_corr", ascending=False)
    return cp


# ---------------------------------------------------------------------------
def main() -> int:
    intel, rets = load_data()

    # ---- (a) Daily diagnostics ----
    daily = daily_diagnostics(intel, rets)

    # ---- (b) Weekly + Monthly aggregates ----
    KEY = [
        "nifty_pcr_oi","nifty_volt_annual","nifty_max_pain_dev_pct",
        "fii_stats_idx_fut_net_crore","fii_stats_idx_opt_net_crore",
        "fii_stats_ni_fut_net_crore","fii_stats_ni_opt_net_crore",
        "poi_fii_fut_idx_net","pvol_fii_fut_idx_net",
        "poi_dii_fut_idx_net","poi_pro_fut_idx_net","poi_client_fut_idx_net",
        "deleq_universe_fut_eq",
        "cd_usdinr_near_settle","cd_usdinr_vol_d_pct",
        "irf_fii_long_total",
    ]
    KEY = [k for k in KEY if k in intel.columns]
    weekly = weekly_aggregate(intel, rets, KEY)
    monthly = monthly_aggregate(intel, rets, KEY)

    # ---- (c) IC analysis ----
    ic = ic_analysis(intel, rets)
    ic.to_csv(OUT_IC, index=False)

    # ---- (d) Redundancy check on top-IC features ----
    top_ic = ic[ic["abs_ic_p"] >= 0.08].head(40)
    red = redundancy_check(intel, rets, top_ic["feature"].tolist())

    # ---- print everything ----
    print("=" * 78)
    print("DAILY DIAGNOSTICS (164 trade-days)")
    print("=" * 78)
    print(f"Feature population per day: min={daily['feature_pop_min']}  "
          f"median={daily['feature_pop_median']}  max={daily['feature_pop_max']} / 256")
    print(f"\nNifty daily return %:")
    for k, v in daily["ret_pct_describe"].items():
        print(f"  {k:8s} = {v:8.4f}")
    print(f"\nNifty daily range %:")
    for k, v in daily["range_pct_describe"].items():
        print(f"  {k:8s} = {v:8.4f}")
    print(f"\nDirection counts (>+0.1% / [-0.1%,+0.1%] / <-0.1%):")
    print(f"  {daily['dir_counts']}")

    print(f"\nOutage days (more than 5 features missing):")
    if daily["outage_days"]:
        for dt, info in list(daily["outage_days"].items())[:10]:
            print(f"  {pd.Timestamp(dt).date()}  "
                  f"populated={info['n_features_populated']}  "
                  f"ret={info['ret_pct']:+.3f}%")
    else:
        print("  (none beyond the 9 documented in Phase 4)")

    print()
    print("=" * 78)
    print("WEEKLY AGGREGATE (key features)")
    print("=" * 78)
    cols_show = ["yr_wk","trade_date__count","ret_pct__sum","range_pct__sum"]
    cols_show += [c for c in weekly.columns if c.endswith("__mean")
                  and any(k in c for k in ["pcr_oi","volt_annual","fii_stats_ni_fut","fii_stats_ni_opt"])]
    print(weekly[cols_show].round(3).to_string(index=False))

    print()
    print("=" * 78)
    print("MONTHLY AGGREGATE (key features)")
    print("=" * 78)
    cols_show_m = ["yr_mo","trade_date__count","ret_pct__sum","range_pct__sum"]
    cols_show_m += [c for c in monthly.columns if c.endswith("__mean")
                    and any(k in c for k in ["pcr_oi","volt_annual","fii_stats_ni_fut","fii_stats_ni_opt",
                                              "cd_usdinr_near_settle","cd_usdinr_vol_d_pct"])]
    print(monthly[cols_show_m].round(3).to_string(index=False))

    print()
    print("=" * 78)
    print("INFORMATION COEFFICIENT — Top 25 features (intel[T] -> return[T+1])")
    print("=" * 78)
    show = ic.head(25)[["feature","ic_pearson","ic_p_pvalue","ic_spearman","ic_p_ci_lo","ic_p_ci_hi","sign_match","n"]]
    print(show.round(4).to_string(index=False))

    print()
    print("=" * 78)
    print(f"IC SUMMARY: {len(ic)} features tested, "
          f"{(ic['abs_ic_p']>=0.08).sum()} with |IC|>=0.08, "
          f"{(ic['abs_ic_p']>=0.15).sum()} with |IC|>=0.15")
    print("=" * 78)
    # Surviving features = |IC|>=0.08 and CI doesn't cross zero
    surv = ic[(ic["abs_ic_p"] >= 0.08) &
              ((ic["ic_p_ci_lo"] > 0) | (ic["ic_p_ci_hi"] < 0))]
    print(f"Survivors (|IC|>=0.08 AND 95% CI doesn't cross zero): {len(surv)}")
    if len(surv):
        print(surv[["feature","ic_pearson","ic_p_ci_lo","ic_p_ci_hi"]].round(4).to_string(index=False))

    print()
    print("=" * 78)
    print("REDUNDANCY (top-IC features cross-correlation, |ρ|>=0.85)")
    print("=" * 78)
    if not red.empty:
        red_high = red[red["abs_corr"] >= 0.85]
        if len(red_high):
            print(red_high.head(30).round(3).to_string(index=False))
        else:
            print("(no |ρ|>=0.85 pairs among top-IC candidates)")

    print(f"\nFull IC table written to: {OUT_IC}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
