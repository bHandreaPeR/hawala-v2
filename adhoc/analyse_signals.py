"""
analyse_signals.py — Feature regression on raw candlestick signals.

Generates ALL potential signals (pattern≥1, EMA trend aligned, basic RSI),
simulates outcomes with fixed rules (stop=1×ATR, target=1.5×ATR),
then runs logistic regression to find which features actually predict wins.

2022-2023 = analysis set. 2024 = holdout to check generalisation.
"""
import pickle, sys, warnings
import numpy as np
import pandas as pd
from datetime import time as dtime
from sklearn.linear_model import LogisticRegressionCV
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')
sys.path.insert(0, '/Users/subhransubaboo/Claude Projects/Hawala v2/Hawala v2')

from strategies.patterns import atr, ema, rsi, detect_all_patterns

# ── Load cached futures data ───────────────────────────────────────────────────
CACHE = 'trade_logs/_data_cache_BANKNIFTY_2022-01-01_2024-12-31.pkl'
with open(CACHE, 'rb') as f:
    raw = pickle.load(f)

df = raw.between_time('09:15', '15:30').copy()
print(f"Loaded {len(df)} candles  ({df.index[0].date()} → {df.index[-1].date()})")

# ── Indicators ─────────────────────────────────────────────────────────────────
bar_atr  = atr(df, 14)
bar_ef   = ema(df['Close'], 20)
bar_es   = ema(df['Close'], 50)
bar_rsi  = rsi(df['Close'], 14)
bar_trend = np.where(bar_ef > bar_es, 1, np.where(bar_ef < bar_es, -1, 0))

df['_atr']   = bar_atr
df['_ef']    = bar_ef
df['_es']    = bar_es
df['_rsi']   = bar_rsi
df['_trend'] = bar_trend

patterns = detect_all_patterns(df, bar_atr, body_atr_min=0.3, wick_ratio=1.5)
df['_bh']  = patterns['bullish_hits']
df['_brh'] = patterns['bearish_hits']
df['_bn']  = patterns['bullish_names']
df['_brn'] = patterns['bearish_names']

SQUAREOFF = dtime(15, 15)
SLIPPAGE  = 10   # pts
LOT_SIZE  = 15
STOP_ATR  = 1.0
TGT_ATR   = 1.5

all_ts = df.index.tolist()
MIN_WARMUP = 60

records = []

for i, sig_ts in enumerate(all_ts):
    if i < MIN_WARMUP or i + 1 >= len(all_ts):
        continue

    sig = df.iloc[i]
    tdate = sig_ts.date()

    # Basic pattern filter (low bar — pattern≥1, EMA trend, broad RSI)
    bull = int(sig['_bh']) >= 1
    bear = int(sig['_brh']) >= 1
    trend = int(sig['_trend'])
    rsi_v = float(sig['_rsi'])
    atr_v = float(sig['_atr'])

    if atr_v <= 0 or np.isnan(atr_v):
        continue

    direction = 0
    if bull and trend == 1 and 35 <= rsi_v <= 72:
        direction = 1
    elif bear and trend == -1 and 28 <= rsi_v <= 65:
        direction = -1
    if direction == 0:
        continue

    entry_ts  = all_ts[i + 1]
    if entry_ts.date() != tdate:
        continue
    if entry_ts.time() >= dtime(14, 30):
        continue

    entry_bar = df.iloc[i + 1]
    raw_open  = float(entry_bar['Open'])
    entry_px  = raw_open + (SLIPPAGE if direction == 1 else -SLIPPAGE)
    stop_px   = entry_px - STOP_ATR * atr_v * direction
    tgt_px    = entry_px + TGT_ATR  * atr_v * direction

    # ── Simulate outcome ───────────────────────────────────────────────────────
    pnl_pts    = None
    exit_reason = None
    sim_bars   = df[(df.index.date == tdate) & (df.index > entry_ts)]

    for eidx, erow in sim_bars.iterrows():
        if eidx.time() >= SQUAREOFF:
            pnl_pts = (float(erow['Open']) - entry_px) * direction
            exit_reason = 'SQUAREOFF'
            break
        lo, hi = float(erow['Low']), float(erow['High'])
        if direction == 1:
            if lo <= stop_px:
                pnl_pts = stop_px - entry_px; exit_reason = 'STOP'; break
            if hi >= tgt_px:
                pnl_pts = tgt_px - entry_px;  exit_reason = 'TARGET'; break
        else:
            if hi >= stop_px:
                pnl_pts = entry_px - stop_px; exit_reason = 'STOP'; break
            if lo <= tgt_px:
                pnl_pts = entry_px - tgt_px;  exit_reason = 'TARGET'; break

    if pnl_pts is None:
        last = sim_bars[sim_bars.index.time < SQUAREOFF]
        if last.empty:
            continue
        pnl_pts = (float(last.iloc[-1]['Close']) - entry_px) * direction
        exit_reason = 'SQUAREOFF'

    win = 1 if exit_reason == 'TARGET' else 0

    # ── Compute candidate features at signal bar ───────────────────────────────
    ef_v = float(sig['_ef']); es_v = float(sig['_es'])
    ema_gap_pct   = abs(ef_v - es_v) / es_v * 100 if es_v > 0 else 0

    # EMA_SLOW slope over last 10 bars (trend velocity)
    es_10ago = float(df.iloc[i - 10]['_es']) if i >= 10 else es_v
    ema_slope_pct = (es_v - es_10ago) / es_10ago * 100 if es_10ago > 0 else 0
    ema_slope_signed = ema_slope_pct * direction   # positive = trend moving in trade dir

    # EMA consistency: how many consecutive bars has trend held?
    consistency = 0
    for j in range(i, max(i - 30, MIN_WARMUP), -1):
        if int(df.iloc[j]['_trend']) == trend:
            consistency += 1
        else:
            break

    # ATR regime: current vs 60-bar rolling median
    atr_median = bar_atr.iloc[max(0, i - 59):i + 1].median()
    atr_ratio  = atr_v / atr_median if atr_median > 0 else 1.0

    # Signal bar body strength
    body = abs(float(sig['Close']) - float(sig['Open']))
    body_atr = body / atr_v if atr_v > 0 else 0

    # RSI position (direction-adjusted: positive = RSI supports trade direction)
    rsi_mid = rsi_v - 50
    rsi_signed = rsi_mid * direction   # positive = RSI on correct side of 50

    # Pattern count
    pat_stack = int(sig['_bh']) if direction == 1 else int(sig['_brh'])

    # Time features
    entry_hour = entry_ts.hour + entry_ts.minute / 60
    dow = tdate.weekday()

    records.append({
        'date':          tdate,
        'year':          tdate.year,
        'direction':     direction,
        'pnl_pts':       pnl_pts,
        'win':           win,
        'exit_reason':   exit_reason,
        # Features
        'ema_gap_pct':      ema_gap_pct,
        'ema_slope_signed': ema_slope_signed,
        'ema_consistency':  consistency,
        'atr_ratio':        atr_ratio,
        'atr_v':            atr_v,
        'body_atr':         body_atr,
        'rsi_signed':       rsi_signed,
        'rsi_v':            rsi_v,
        'pat_stack':        pat_stack,
        'entry_hour':       entry_hour,
        'dow':              dow,
    })

df_sig = pd.DataFrame(records)
print(f"\nTotal raw signals: {len(df_sig)}  |  Win rate: {df_sig['win'].mean()*100:.1f}%")
print(f"By year:\n{df_sig.groupby('year')['win'].agg(['mean','count']).rename(columns={'mean':'WR','count':'N'}).assign(WR=lambda x:(x.WR*100).round(1))}")
print(f"\nExit breakdown:\n{df_sig['exit_reason'].value_counts()}")

# ── Logistic Regression (2022-2023 train / 2024 holdout) ──────────────────────
FEATURES = ['ema_gap_pct', 'ema_slope_signed', 'ema_consistency',
            'atr_ratio', 'body_atr', 'rsi_signed', 'pat_stack',
            'entry_hour', 'dow']

train = df_sig[df_sig['year'] <= 2023].dropna(subset=FEATURES)
test  = df_sig[df_sig['year'] == 2024].dropna(subset=FEATURES)

X_tr = train[FEATURES].values
y_tr = train['win'].values
X_te = test[FEATURES].values
y_te = test['win'].values

scaler = StandardScaler()
X_tr_s = scaler.fit_transform(X_tr)
X_te_s = scaler.transform(X_te)

lr = LogisticRegressionCV(cv=5, Cs=10, penalty='l2', max_iter=1000, random_state=42)
lr.fit(X_tr_s, y_tr)

print(f"\n{'='*60}")
print(f"  LOGISTIC REGRESSION  (train=2022-23, holdout=2024)")
print(f"{'='*60}")
print(f"  Train AUC : {roc_auc_score(y_tr, lr.predict_proba(X_tr_s)[:,1]):.3f}")
if len(y_te) > 0 and y_te.sum() > 0:
    print(f"  Holdout AUC (2024): {roc_auc_score(y_te, lr.predict_proba(X_te_s)[:,1]):.3f}")

coef_df = pd.DataFrame({'feature': FEATURES, 'coef': lr.coef_[0]})
coef_df['abs_coef'] = coef_df['coef'].abs()
coef_df = coef_df.sort_values('abs_coef', ascending=False)
print(f"\n  Feature coefficients (L2 regularised, standardised):")
print(f"  {'Feature':<22} {'Coef':>8}  direction")
for _, row in coef_df.iterrows():
    sign = '↑ win' if row['coef'] > 0 else '↓ win'
    print(f"  {row['feature']:<22} {row['coef']:>8.3f}  {sign}")

# ── Per-feature win rate analysis ─────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  WIN RATE BY FEATURE QUARTILE  (all years)")
print(f"{'='*60}")

for feat in ['ema_gap_pct', 'ema_slope_signed', 'ema_consistency',
             'atr_ratio', 'body_atr', 'pat_stack']:
    try:
        df_sig['_q'] = pd.qcut(df_sig[feat], 4, duplicates='drop')
        tbl = df_sig.groupby('_q')['win'].agg(['mean', 'count'])
        tbl['mean'] = (tbl['mean'] * 100).round(1)
        print(f"\n  {feat}:")
        for idx, row in tbl.iterrows():
            print(f"    {str(idx):30s}  WR={row['mean']:5.1f}%  N={int(row['count'])}")
    except Exception:
        pass

# ── Win rate by day-of-week ───────────────────────────────────────────────────
print(f"\n  Day of week WR:")
days = ['Mon','Tue','Wed','Thu','Fri']
for d, nm in enumerate(days):
    sub = df_sig[df_sig['dow'] == d]
    if len(sub):
        print(f"    {nm}: WR={sub['win'].mean()*100:.1f}%  N={len(sub)}")

# ── Win rate by entry hour ─────────────────────────────────────────────────────
print(f"\n  Entry hour WR:")
df_sig['_hr'] = df_sig['entry_hour'].apply(lambda x: f"{int(x):02d}:{int((x%1)*60):02d}")
tbl = df_sig.groupby(df_sig['entry_hour'].apply(lambda x: int(x)))['win'].agg(['mean','count'])
for hr, row in tbl.iterrows():
    print(f"    {hr:02d}:xx  WR={row['mean']*100:.1f}%  N={int(row['count'])}")

# ── Optimal threshold search for key continuous features ──────────────────────
print(f"\n{'='*60}")
print(f"  THRESHOLD SEARCH  (train set 2022-23 only)")
print(f"{'='*60}")

for feat, lo_is_good in [('ema_gap_pct', False),    # higher gap = ?
                          ('ema_slope_signed', False), # higher slope = ?
                          ('ema_consistency', False),  # higher = ?
                          ('atr_ratio', True)]:        # lower ratio = ?
    sub = train.copy()
    vals = sorted(sub[feat].unique())
    # Test 10 percentile thresholds
    thresholds = np.percentile(vals, np.arange(10, 91, 10))
    best_wr, best_thr, best_n = 0, None, 0
    results = []
    for thr in thresholds:
        if lo_is_good:
            kept = sub[sub[feat] <= thr]
        else:
            kept = sub[sub[feat] >= thr]
        if len(kept) < 10:
            continue
        wr = kept['win'].mean() * 100
        results.append((thr, wr, len(kept)))
        if wr > best_wr:
            best_wr, best_thr, best_n = wr, thr, len(kept)
    print(f"\n  {feat}  (higher/lower = {'lower' if lo_is_good else 'higher'} is better?)")
    for thr, wr, n in results:
        marker = ' ◄ BEST' if thr == best_thr else ''
        op = '≤' if lo_is_good else '≥'
        print(f"    {op}{thr:7.3f}: WR={wr:5.1f}% N={n}{marker}")

print("\nDone.")
df_sig.to_csv('trade_logs/_signal_analysis.csv', index=False)
print("Full signal table saved to trade_logs/_signal_analysis.csv")
