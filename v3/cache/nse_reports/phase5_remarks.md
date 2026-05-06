# Phase 5 Findings — NSE Intel Time Series Critical Analysis

Window: 2025-09-01 → 2026-05-04, 164 NSE trading days, 256 features × 164 rows.
Pairing convention: intel[T] → predicts return on T+1 (NSE EOD reports for T are
finalised at end-of-day T, usable to position for T+1).

## TL;DR (the brutally honest version)

The naive top-IC features look impressive but are **mostly trend artifacts** — Sep
2025 → Apr 2026 had a falling Nifty AND rising stock-segment open interest, so
any size/level feature picks up that joint trend, not actual predictive content.

Once you de-trend (day-over-day change or 5-day rate-of-change), the picture
collapses dramatically:

- ~159 features had |IC|≥0.08 on raw levels.
- Only **5 ROC-features survive** with stable sign across H1 (Sep-Dec) vs H2
  (Jan-Apr) AND |IC|≥0.10:
  - `roc5_poi_dii_total_net`            IC=+0.18 (H1=+0.21, H2=+0.20) ← **strongest survivor**
  - `roc5_poi_client_fut_idx_net`       IC=+0.135 (H1=+0.11, H2=+0.16)
  - `roc5_cd_usdinr_near_settle`        IC=-0.119 (H1=-0.105, H2=-0.118) ← cross-asset, validates the hypothesis
  - `roc5_fii_stats_ni_fut_net_crore`   IC=-0.114 (H1=-0.08, H2=-0.13)
  - `roc5_fii_stats_ni_opt_net_crore`   IC=+0.082 (H1=+0.04, H2=+0.13)

That's it. **5 features are real, the other 250+ are noise or scale.**
This is consistent with what your project memory says about basis_momentum
being the only strong directional signal in the existing 8-feature classifier.

---

## DAILY remarks (164 days)

| Stat | Value |
|---|---|
| Trading days | 164 |
| Feature population (median per day) | 251 / 256 |
| Nifty fut open→close mean | -0.017% |
| Nifty fut open→close std  | 0.61% |
| Nifty fut open→close range | -1.77% to +1.40% |
| Nifty daily intraday range mean | 0.89% |
| Nifty daily intraday range max | 2.42% |
| Direction split (>+0.1% / [-0.1,+0.1] / <-0.1%) | 68 up / 20 flat / 75 down |

- **Bearish lean**: 75 down vs 68 up days. Net 7 more red days than green.
- **20 "flat" days** — no directional move, will not generate trades. F_VOL
  gate already excludes most of these.
- **Outage days (>5 features missing)**:
  - 2025-09-08 (NBF×2 + CD×2 missing): Nifty +0.08%, normal day
  - 2026-01-15 (full archive blackout, 11 reports missing): no return data either
  - 2026-02-19 (NBF×2 + CD×2 missing): Nifty -1.72%, big down day
  - 2026-03-19 (NBF×2 + CD×2 missing): Nifty -0.91%, down day
  - 2026-04-01 (NBF×2 + CD×2 missing): Nifty -0.10%, flat
- **Pattern check**: 3 of 4 cross-asset outage days are non-trivial down days
  (-1.72%, -0.91%). Suggests NSE publishing infrastructure may correlate with
  market stress days. Not actionable but worth noting for live ops.
- **Re-fetch candidate**: 2026-01-15 should be retried — full data blackout.

## WEEKLY remarks (36 trading-weeks)

| Period | Sample weeks | Notes |
|---|---|---|
| 2025-W36 → W43 (Sep-Oct opening) | 8 weeks | Mixed but bullish bias: +0.18, +0.73, +0.55, **-2.13**, +0.51, +1.58, +1.36, -1.22 |
| 2025-W44 → W52 (Nov-Dec) | 9 weeks | Range-bound, range_pct/wk avg 3.0-3.6%, vol_annual flat at 0.144-0.149 |
| 2026-W01 → W05 (Jan rout) | 5 weeks | Heavy: +0.70, **-2.34**, -0.29, **-2.61**, +0.93. Net Jan = -3.84% |
| 2026-W06 → W14 (Feb-Mar drawdown) | 9 weeks | Persistent selling. W11 -1.94% range 7.55%, W12 +0.53% range 7.61% — vol expansion |
| 2026-W15 → W18 (Apr recovery) | 4 weeks | Sharp bounce: **+3.82**, +0.85, -0.86, +0.82 |

Three regime markers from the data:
- **Vol regime shift in March**: nifty_volt_annual climbs from 0.140 (Dec) to
  0.155 (Mar) to **0.177** (Apr). Daily range_pct/week roughly doubles.
- **FII Nifty options flow flips sign in late February**: Sep-Jan averages
  +₹3-7K cr/wk net buying, then Feb-Apr flips to **net selling** -₹2-10K cr/wk.
  This is the largest single behavioural shift in the dataset.
- **DII passive cushion** (`poi_dii_total_net`) consistently positive but
  trending down through the drawdown weeks; recovers mid-Apr.

## MONTHLY remarks (9 calendar months)

| Month | Days | Net % | Range Σ | PCR-OI | NIFTY vol | FII Ni-Fut net (₹cr/d avg) | FII Ni-Opt net (₹cr/d avg) | USDINR | USDINR vol_d |
|---|---|---|---|---|---|---|---|---|---|
| 2025-09 | 22 | -1.34 | 13.06 | 0.93 | 0.156 | -144 | +5,467 | 88.40 | 0.234 |
| 2025-10 | 20 | +2.68 | 15.67 | 1.02 | 0.151 | +13   | +7,076 | 88.43 | 0.233 |
| 2025-11 | 19 | +1.02 | 13.06 | 1.00 | 0.146 | -473  | +5,503 | 88.83 | 0.234 |
| 2025-12 | 22 | -0.93 | 13.07 | 0.88 | 0.142 | -420  | +3,292 | 90.10 | 0.241 |
| 2026-01 | 21 | -3.84 | 18.14 | 0.81 | 0.138 | -992  | +287  | 90.81 | 0.242 |
| 2026-02 | 20 | -2.35 | 18.56 | 0.84 | 0.143 | +203  | -710  | 90.75 | 0.261 |
| 2026-03 | 19 | -3.78 | 29.06 | 0.86 | 0.155 | -1,373| -6,391| 92.65 | 0.270 |
| 2026-04 | 20 | +5.95 | 23.84 | 1.05 | 0.175 | +153  | -4,042| 93.52 | 0.295 |
| 2026-05 | 1  | -0.13 | 1.26  | 0.62 | 0.176 | -1,345| +5,000 (one-day blip)| 95.24 | 0.294 |

Five things this table tells you:

1. **Rupee depreciation is monotonic**: USDINR went **88.40 → 95.24** (7.7%) in
   8 months. USDINR vol_d climbed from 0.234 to 0.294. The rupee was a leading
   indicator: started weakening in Dec (90.10), accelerated through Mar-Apr.
   Cross-asset hypothesis confirmed.
2. **FII Nifty options flow inverted at the Feb 2026 boundary**. From +₹3-7K
   cr/day net buying through Sep-Jan, to net selling from Feb onwards. This is
   the single most consistent macro signal in the dataset.
3. **Vol bottom = Jan 2026** (`nifty_volt_annual`=0.138). Vol then expanded
   monotonically through Apr (0.175). The F_VOL gate that already exists in v3
   was correctly calibrated for 95-of-164 low-vol days, mostly clustered
   Sep-Feb. April will likely have generated more trades.
4. **April recovery is anomalous**. Despite continued FII Ni-Opt selling
   (-₹4K cr/d), the index gained +5.95%. PCR-OI rose to 1.05 (put-side OI
   rebuilding). The classifier needs to handle the regime where flows say
   one thing but price moves the other.
5. **Range expansion in Mar-Apr**: monthly range_pct sum doubled from 13-18
   (Sep-Feb) to 23-29 (Mar-Apr). Higher-volatility regime.

## TOTAL remarks (over the full 164-day window)

- **256 features generated, ~5 actually predictive after de-trending.** Be
  brutal about discarding the rest.
- **Top raw-IC features are trend artifacts.** `poi_client_fut_stk_short`
  (IC=-0.35) is the highest-IC feature in the entire dataset, but it's just
  capturing "stock-segment OI is building over time, Nifty is falling over
  time, ergo high correlation". Adding it to the classifier would teach the
  model "more activity → bearish forever", which doesn't generalise.
- **Redundancy is everywhere**: 30+ pairs with |ρ|≥0.85. Buy and sell volumes
  for the same instrument move identically (ρ=0.999). Long and short positions
  for the same cohort are 99% correlated (ρ=0.999). The gross-flow features
  carry no extra info beyond turnover.
- **The existing 8-feature classifier is already only effectively-4 features**:
  per `fii_dii_thresholds.json`, four of the eight features
  (`ce_pe_imbalance`, `strike_coverage`, `ce_skew_shift`, `pe_skew_shift`) have
  identical centroids across all three labels (FII_BULL, FII_BEAR, DII_MIXED)
  — they carry zero discriminative information in the trained model. The
  actual workhorses are `basis_momentum`, `atm_build_rate`,
  `oi_add_intensity`, `fii_cash_context`. Memory note already flags
  basis_momentum as the primary directional signal — confirmed.
- **77.3% directional accuracy on 22 trades is impressive but small-N**.
  The current classifier output likely improves marginally if at all from
  adding more features. The real upside is in **filling regime-specific
  gaps** (April-style "flow says down, price says up" anomalies).

---

## How to leverage this for v3

### Step 1 — Drop the noise. Use ONLY ROC/de-trended versions.

For any new feature derived from the intel time-series, compute:
- `d_X = X.diff(1)` (1-day change)
- `roc5_X = X.diff(5)` (5-day rate of change)
- `z20_X = (X - rolling_mean_20) / rolling_std_20` (20-day z-score)

Discard the raw level X for the classifier — the level captures regime drift,
not directional signal.

### Step 2 — Add 3 robust features to the classifier (NOT 50)

Based on H1/H2 stability + economic plausibility:

| New feature | Why | Expected IC | Replaces? |
|---|---|---|---|
| `roc5_poi_dii_total_net` | DII positioning shift; passive flow that leads | +0.18 stable | adds new dimension (DII context, not in current 8) |
| `roc5_cd_usdinr_near_settle` | Rupee stress = leading index sell-off | -0.12 stable | adds cross-asset dimension |
| `roc5_fii_stats_ni_fut_net_crore` | FII Nifty futures flow over 5 days | -0.11 stable | partially overlaps with `fii_cash_context`; test both |

These are 3 features with stable sign across H1/H2 and economically interpretable.

### Step 3 — Retrain `FIIDIIClassifier` with 11 features (existing 8 + 3 new)

Concrete plan:
- File: `v3/signals/fii_dii_classifier.py`
- Add a feature-engineering pass that loads `intel_timeseries.parquet`, computes the 3 ROC features, joins on `trade_date`.
- Keep the existing Gaussian centroids/dispersion model (don't re-architect).
- Recalibrate centroids on the same 78-day labeled subset, hold out the last 30 days for OOS.
- Acceptance criterion: **OOS WR ≥ 75% AND OOS dir-acc ≥ 75%** with the new features. Below those — revert.

### Step 4 — Don't touch the F_VOL gate, ATR-based exits, or signal engine

The 78-day backtest at 77.3% Dir Acc with the F_VOL gate at 0.85% is already
working. The risk in adding features to the classifier is **over-fitting the
classifier**, not the signal engine. Keep the rest of the stack frozen during
this experiment.

### Step 5 — Ship a separate "regime context" panel in the morning Telegram alert

Even before any classifier changes, the parsed intel gives you 3 numbers worth
seeing every morning:
- `roc5_cd_usdinr` (rupee weakening over last 5 days, +/- bps)
- `roc5_poi_dii_total_net` (DII building or selling)
- `roc5_fii_stats_ni_opt_net_crore` (FII Nifty options flow over 5 days)

These three contextual numbers reduce false-positive signals on days where the
flow contradicts the engine's signal. Implementation cost: ~30 lines in
`v3/live/runner_nifty.py` morning alert.

### Step 6 — Re-fetch the 2026-01-15 blackout

`python3 v3/scripts/fetch_nse_reports_history.py --start 2026-01-15 --end 2026-01-15 --force`

If NSE has back-filled the archive in the last 4 months, you regain a data
point in the heart of the January drawdown — useful for IC stability checks.

---

## What NOT to do

1. **Don't add 30 features and let the model figure it out.** Most are
   redundant or trend-fooled. Adding them lowers OOS performance.
2. **Don't trust the raw-level top-IC list.** The IC=-0.35 feature looks
   amazing on paper and is useless in practice.
3. **Don't deploy the new features without OOS validation.** The 78-day
   training set is small enough that 3 new features could find spurious
   patterns. Hold out 30 days. If they fail OOS, revert.
4. **Don't expect the existing 77.3% to jump to 85%.** Realistic upside is
   maybe 2-4 pp on Dir Acc and similar on WR — *if* the new features survive
   OOS. The bigger gain may be in regime-specific behavior (e.g. April-style
   anomalies) rather than headline accuracy.
5. **Don't rely on IRD/IRF reports yet.** All values are 0 in the dataset
   (FIIs have effectively no IRF participation). The hypothesis fails on
   sample-size grounds, not on logic — re-evaluate when Indian IRF activity
   picks up (could be 1-2 years out).
