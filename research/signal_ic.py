# ============================================================
# research/signal_ic.py — ORB Signal IC & Feature Analysis
# ============================================================
# Reads existing ORB trade logs and computes Information
# Coefficient (IC = Pearson correlation) for each candidate
# feature against trade outcome.
#
# Uses logistic regression to find the true predictive
# features, then reports AUC on 2025 OOS holdout.
#
# Run: python research/signal_ic.py
# ============================================================

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.linear_model import LogisticRegressionCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, accuracy_score
import warnings
warnings.filterwarnings('ignore')


# ── Load trade logs ────────────────────────────────────────────────────────────

def load_orb_trades() -> pd.DataFrame:
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'trade_logs')
    frames = []
    # Load all available logs and combine (prefer combined, but stack IS+OOS too)
    for fname in ['baseline_combined_all.csv', 'baseline_IS_2024.csv',
                  'baseline_OOS_2025.csv']:
        fpath = os.path.join(log_dir, fname)
        if os.path.exists(fpath):
            frames.append(pd.read_csv(fpath))

    if not frames:
        sys.exit("❌  No trade log CSVs found in trade_logs/. Run run_baseline.py first.")

    df = pd.concat(frames, ignore_index=True).drop_duplicates()
    orb = df[df['strategy'] == 'ORB'].copy()
    print(f"✅  Loaded {len(orb)} ORB trades  ({orb['year'].min()}–{orb['year'].max()})")
    return orb


# ── Feature engineering ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize entry_ts column name
    ts_col = 'entry_ts' if 'entry_ts' in df.columns else 'entry_time'
    df['entry_dt'] = pd.to_datetime(df[ts_col])
    df['entry_hour'] = df['entry_dt'].dt.hour
    df['day_of_week'] = df['entry_dt'].dt.dayofweek   # 0=Mon, 4=Fri

    # Gap-based features (already in ORB log)
    df['gap_pct_abs']   = df['gap_pts'].abs() / df['entry']          # gap as % of price
    df['gap_vs_atr']    = df['gap_pts'].abs() / df['atr14'].clip(1)   # gap / ATR14

    # ORB range feature (orb_size already in ORB log)
    if 'orb_size' in df.columns:
        df['orb_vs_atr'] = df['orb_size'] / df['atr14'].clip(1)
    else:
        df['orb_vs_atr'] = np.nan

    # Stop/target tightness
    df['stop_vs_atr']   = df['stop_pts'].abs() / df['atr14'].clip(1)
    df['target_vs_atr'] = df['target_pts'].abs() / df['atr14'].clip(1)

    # Win target = 1 only if TARGET HIT (not square-off or stop)
    df['target_win'] = (df['exit_reason'] == 'TARGET HIT').astype(int)
    # General win (positive P&L)
    df['gen_win']    = df['win'].astype(int)

    return df


# ── IC computation ────────────────────────────────────────────────────────────

def compute_ic(df: pd.DataFrame, features: list, target: str = 'gen_win') -> pd.DataFrame:
    results = []
    for feat in features:
        sub = df[[feat, target]].dropna()
        if len(sub) < 20:
            continue
        x = sub[feat].values
        y_ = sub[target].values
        ic, pval = stats.pearsonr(x, y_)
        results.append({
            'feature': feat,
            'IC':      round(ic, 4),
            'p_value': round(pval, 4),
            'significant': pval < 0.10,
            'abs_IC':  abs(ic),
        })
    return pd.DataFrame(results).sort_values('abs_IC', ascending=False)


# ── Logistic regression ────────────────────────────────────────────────────────

def run_logistic(df: pd.DataFrame, features: list, train_year: int, oos_year: int):
    df = df.dropna(subset=features + ['gen_win'])

    train = df[df['year'] <= train_year]
    oos   = df[df['year'] == oos_year]

    if len(train) < 20 or len(oos) < 5:
        print(f"  ⚠  Insufficient data: train={len(train)}, OOS={len(oos)}")
        return

    X_tr = train[features].values
    y_tr = train['gen_win'].values
    X_oo = oos[features].values
    y_oo = oos['gen_win'].values

    scaler = StandardScaler()
    X_tr_s = scaler.fit_transform(X_tr)
    X_oo_s = scaler.transform(X_oo)

    lr = LogisticRegressionCV(cv=min(5, len(train)//5), Cs=10,
                               penalty='l2', max_iter=500, random_state=42)
    lr.fit(X_tr_s, y_tr)

    auc_tr = roc_auc_score(y_tr, lr.predict_proba(X_tr_s)[:,1])
    auc_oo = roc_auc_score(y_oo, lr.predict_proba(X_oo_s)[:,1])
    acc_oo = accuracy_score(y_oo, lr.predict(X_oo_s))

    print(f"\n  Logistic Regression  (train ≤ {train_year} | OOS = {oos_year})")
    print(f"  Train AUC : {auc_tr:.3f}   OOS AUC : {auc_oo:.3f}   OOS Acc : {acc_oo:.1%}")
    print(f"\n  Feature coefficients (L2-regularised, standardised):")
    coef_df = pd.DataFrame({
        'feature': features,
        'coef':    lr.coef_[0],
    }).sort_values('coef', key=abs, ascending=False)
    for _, row in coef_df.iterrows():
        direction = '▲' if row['coef'] > 0 else '▼'
        print(f"    {direction}  {row['feature']:<22}  {row['coef']:+.4f}")

    return auc_oo, coef_df


# ── Gap-size win rate breakdown ───────────────────────────────────────────────

def gap_size_breakdown(df: pd.DataFrame):
    print(f"\n  {'─'*60}")
    print(f"  Gap Size → Win Rate Breakdown")
    print(f"  {'Gap Range (pts)':<20} {'N':>5} {'WR%':>7} {'AvgPnL':>10} {'Suggestion'}")
    print(f"  {'─'*60}")

    bins   = [(50, 75), (75, 100), (100, 150), (150, 200), (200, 300), (300, 400)]
    for lo, hi in bins:
        mask = (df['gap_pts'].abs() >= lo) & (df['gap_pts'].abs() < hi)
        sub  = df[mask]
        if len(sub) == 0:
            continue
        wr      = sub['gen_win'].mean() * 100
        avg_pnl = sub['pnl_rs'].mean()
        note    = '✅ Edge' if wr >= 52 and avg_pnl > 0 else ('⚠ Weak' if wr >= 48 else '❌ No edge')
        print(f"  {lo}–{hi} pts{'':<11} {len(sub):>5}  {wr:>6.1f}%  ₹{avg_pnl:>8,.0f}  {note}")

    # Show the 100pt threshold specifically
    below = df[df['gap_pts'].abs() < 100]
    above = df[df['gap_pts'].abs() >= 100]
    print(f"  {'─'*60}")
    print(f"  {'<100 pts (futures):':<20} {len(below):>5}  {below['gen_win'].mean()*100:>6.1f}%  "
          f"₹{below['pnl_rs'].mean():>8,.0f}")
    print(f"  {'≥100 pts (options):':<20} {len(above):>5}  {above['gen_win'].mean()*100:>6.1f}%  "
          f"₹{above['pnl_rs'].mean():>8,.0f}")


# ── Day-of-week breakdown ─────────────────────────────────────────────────────

def dow_breakdown(df: pd.DataFrame):
    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
    print(f"\n  {'─'*50}")
    print(f"  Day-of-Week → Win Rate")
    print(f"  {'Day':<6} {'N':>5} {'WR%':>7} {'AvgPnL':>10}")
    print(f"  {'─'*50}")
    for d in range(5):
        sub = df[df['day_of_week'] == d]
        if sub.empty:
            continue
        print(f"  {days[d]:<6} {len(sub):>5}  {sub['gen_win'].mean()*100:>6.1f}%  "
              f"₹{sub['pnl_rs'].mean():>8,.0f}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "█"*64)
print("  SIGNAL IC ANALYSIS — ORB FEATURE RESEARCH")
print("█"*64)

orb = load_orb_trades()
orb = engineer_features(orb)

FEATURES = ['gap_pct_abs', 'gap_vs_atr', 'orb_vs_atr',
            'stop_vs_atr', 'entry_hour', 'day_of_week']
FEATURES = [f for f in FEATURES if f in orb.columns and orb[f].notna().sum() > 10]

# ── IC table ──────────────────────────────────────────────────────────────────
print(f"\n  {'─'*58}")
print(f"  Feature IC vs Win (Pearson r, p < 0.10 = significant)")
print(f"  {'Feature':<22} {'IC':>8} {'p-value':>10} {'Sig?':>6}")
print(f"  {'─'*58}")
ic_df = compute_ic(orb, FEATURES, target='gen_win')
for _, row in ic_df.iterrows():
    sig = '  ✅' if row['significant'] else ''
    print(f"  {row['feature']:<22} {row['IC']:>+8.4f}  {row['p_value']:>9.4f}{sig}")

# ── Logistic regression ───────────────────────────────────────────────────────
print(f"\n{'='*64}")
print("  LOGISTIC REGRESSION (predict win from features)")
print(f"{'='*64}")
run_logistic(orb, FEATURES, train_year=2024, oos_year=2025)

# ── Gap-size breakdown ────────────────────────────────────────────────────────
print(f"\n{'='*64}")
print("  GAP SIZE SEGMENTATION — validates 50/100 pt thresholds")
print(f"{'='*64}")
gap_size_breakdown(orb)

# ── Day-of-week ───────────────────────────────────────────────────────────────
dow_breakdown(orb)

print(f"\n{'━'*64}")
print("  KEY TAKEAWAY:")
print("  Features with |IC| > 0.05 and p < 0.10 are worth keeping.")
print("  Gap size breakdown tells you where to split futures vs options.")
print(f"{'━'*64}\n")
