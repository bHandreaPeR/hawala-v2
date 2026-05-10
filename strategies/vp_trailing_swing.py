# ============================================================
# strategies/vp_trailing_swing.py — Volume Profile fade with
# selective multi-day hold for EOD-profitable trades.
# ============================================================
# Same entry/exit machinery as strategies/vp_trailing.py, but at
# the daily 15:15 squareoff we make a smart decision:
#
#   • If the trade is in PROFIT (current pnl > 0) — KEEP it
#     overnight. The trailing stop persists; on day 2+ the stop
#     is also clamped to entry (break-even) so a gap against us
#     never costs more than slippage. Hold up to MAX_HOLD_DAYS.
#   • Otherwise — close at 15:15 as before.
#
# Why: empirically, EOD-profitable trades hit target 37% of the
# time within 3 days (vs 15% for EOD-loss trades). The losers
# almost always fail; the winners often complete.
#
# Cross-day exit triggers:
#   • Trailing stop (chandelier from peak favourable price)
#   • Original target (POC)
#   • MAX_HOLD_DAYS elapsed → force-close at next 15:15
#   • Contract roll (front-month changes) → force-close
#   • Regime shift mid-trade (auction reset) → force-close
# ============================================================

import numpy as np
import pandas as pd
from collections import deque
from datetime import time as dtime

from strategies.volume_profile import (
    _bar_distribute_volume,
    _value_area,
    _build_daily_summary,
    _is_regime_shift,
)


def run_vp_trailing_swing(data: pd.DataFrame,
                          instrument_config: dict,
                          strategy_params: dict,
                          regime_df=None,
                          params=None) -> pd.DataFrame:
    """Volume-profile fade with selective multi-day hold."""
    LOT_SIZE  = instrument_config.get('lot_size',  15)
    BROKERAGE = instrument_config.get('brokerage', 40)

    def _p(key, default):
        if params is not None and key in params:
            return params[key]
        return strategy_params.get(key, default)

    VA_PCT             = float(_p('VP_VA_PCT',            0.70))
    BIN_PTS            = float(_p('VP_BIN_PTS',           20))
    MIN_PROFILE_DAYS   = int  (_p('VP_MIN_PROFILE_DAYS',  3))

    PIERCE_MIN_ATR     = float(_p('VPT_PIERCE_MIN_ATR',   0.30))
    PIERCE_MAX_ATR     = float(_p('VPT_PIERCE_MAX_ATR',   2.50))
    REVERSAL_ATR       = float(_p('VPT_REVERSAL_ATR',     0.30))
    REVERSAL_MAX_BARS  = int  (_p('VPT_REVERSAL_MAX_BARS', 8))
    REQUIRE_CANDLE_REV = bool (_p('VPT_REQUIRE_CANDLE_REV', True))
    INITIAL_STOP_BUFFER_ATR = float(_p('VPT_INITIAL_STOP_BUFFER_ATR', 0.20))
    TRAIL_ATR          = float(_p('VPT_TRAIL_ATR',        0.75))
    TARGET_FRAC        = float(_p('VPT_TARGET_FRAC',      1.00))
    BE_TRIGGER_FRAC    = float(_p('VPT_BE_TRIGGER_FRAC',  0.50))
    MIN_TARGET_PTS     = float(_p('VPT_MIN_TARGET_PTS',   2 * BIN_PTS))

    # ── Multi-day hold params ───────────────────────────────────────────────
    MAX_HOLD_DAYS      = int  (_p('VPT_MAX_HOLD_DAYS',    3))
    OVERNIGHT_TRAIL_ATR= float(_p('VPT_OVERNIGHT_TRAIL_ATR', 0.50))  # tighter
    CARRY_REQUIRES_PROFIT = bool(_p('VPT_CARRY_REQUIRES_PROFIT', True))

    # ── Pierce-state persistence across days ────────────────────────────────
    # When > 0, an unresolved pierce (PIERCED_UP/PIERCED_DN with no trade)
    # is carried into the next trading day. Lets the strategy enter on a
    # delayed reversal (e.g. piece on day 1 morning, reversal on day 2).
    PIERCE_PERSIST_DAYS = int(_p('VPT_PIERCE_PERSIST_DAYS', 0))

    # ── Trend filter ────────────────────────────────────────────────────────
    # Skip fade entries that go AGAINST the recent N-day daily-close trend.
    # Empirically: shorts into uptrends and longs into downtrends have
    # 30% WR with -ve EV, while WITH-trend or flat-regime fades are 55%+
    # WR with strong +EV. Threshold is in PERCENT (e.g. 0.5 = 0.5%).
    #   LONG  signal allowed when trend_Nd_pct >= -threshold
    #   SHORT signal allowed when trend_Nd_pct <= +threshold
    # Set TREND_LOOKBACK_DAYS=0 to disable.
    # NOTE: empirically the binary filter HURT total return on all instruments
    # (the loss-analysis showing +EV against-trend trades was hiding lookahead
    # bias). Default is OFF; expose params for future research.
    TREND_LOOKBACK_DAYS = int(_p('VPT_TREND_LOOKBACK_DAYS', 0))
    TREND_THRESHOLD_PCT = float(_p('VPT_TREND_THRESHOLD_PCT', 99.0))

    # ── Trend-based position sizing (preferred over binary filter) ─────────
    # Reduce position when fade direction goes AGAINST a clear trend.
    #   POS_TREND_LOOKBACK_DAYS = N (0 disables sizing)
    #   POS_TREND_NEUTRAL_PCT   = trend < this (in absolute %) → full size
    #   POS_AGAINST_TREND_FACTOR = multiplier on `per_trade_risk_pct` when
    #                              the trade is against trend (e.g. 0.5)
    POS_TREND_LOOKBACK_DAYS = int(_p('VPT_POS_TREND_LOOKBACK_DAYS', 5))
    POS_TREND_NEUTRAL_PCT   = float(_p('VPT_POS_TREND_NEUTRAL_PCT',  0.5))
    POS_AGAINST_TREND_FACTOR = float(_p('VPT_POS_AGAINST_TREND_FACTOR', 0.5))
    # Base risk pct (compounding engine reads `per_trade_risk_pct` per row).
    POS_BASE_RISK_PCT = float(_p('VPT_POS_BASE_RISK_PCT', 0.90))

    # ── Regime filter: skip in low-vol & strong-trend regimes ────────────────
    # Empirically: months with realized vol < 10% annualized are net losers
    # for this fade strategy (no movement = no pierce). Months with high
    # trendiness (|net move|/total move > 0.50) get crushed (trend grinds
    # over our fades). Computed from rolling 20 daily closes — NO lookahead.
    REGIME_RV_LOOKBACK_DAYS  = int(_p('VPT_REGIME_RV_LOOKBACK_DAYS', 20))
    REGIME_MIN_RV_ANN_PCT    = float(_p('VPT_REGIME_MIN_RV_ANN_PCT', 0.0))   # 0 = off
    REGIME_MAX_TRENDINESS    = float(_p('VPT_REGIME_MAX_TRENDINESS', 1.0))   # 1 = off

    # ── Adaptive Value-Area % ───────────────────────────────────────────────
    # In low-RV regimes the price range is compressed and a fixed 70% VA
    # encompasses too much of the daily action — pierces are lost in noise.
    # When RV < threshold, narrow the VA (e.g. 50%) so pierces remain
    # meaningful. Above the threshold, use the base VA.
    VA_PCT_LOW_RV     = float(_p('VPT_VA_PCT_LOW_RV',  VA_PCT))   # default = same
    VA_PCT_RV_THRESHOLD = float(_p('VPT_VA_PCT_RV_THRESHOLD', 0.0))  # 0 = never

    # ── Post-regime-shift blackout ──────────────────────────────────────────
    # When a regime shift fires (price moves outside prior VA on big gap+vol),
    # the new auction is forming. Trading the next 1-2 days against the
    # stale full-profile or under-formed sub-profile is risky. Skip entries
    # entirely during this cold-start window.
    REGIME_BLACKOUT_DAYS = int(_p('VPT_REGIME_BLACKOUT_DAYS', 0))    # 0 = off

    # ── Cluster-loss cooldown ────────────────────────────────────────────────
    # When N consecutive losses pile up on this instrument, the auction is
    # likely in a slow-grind regime that the gap-based regime detector
    # missed (e.g. BANKNIFTY Jan 2025 lost ₹51k across 7 LONG fades into
    # a grinding downtrend with no single big-gap day). Freeze NEW-DAY
    # entries for COOLDOWN_DAYS while the new regime crystallises.
    # Same-day re-entries (within the per-day MAX_TRADES limit) are
    # unaffected — only the day-start gate fires this barrier.
    CLUSTER_LOSS_COUNT    = int(_p('VPT_CLUSTER_LOSS_COUNT',   0))   # 0 = off
    CLUSTER_COOLDOWN_DAYS = int(_p('VPT_CLUSTER_COOLDOWN_DAYS', 5))

    # ── Realistic slippage ──────────────────────────────────────────────────
    # Real-world fills on stop orders during fast markets can be 30–200 pts
    # worse than the printed price. Backtest defaults to 5 pts which under-
    # states losses. Set per-instrument: BN ~30, NIFTY ~10, SENSEX ~20.
    # Applied symmetrically (entry + exit each pay SLIPPAGE_PTS).
    SLIPPAGE_PTS = float(_p('VPT_SLIPPAGE_PTS', 5))

    # ── Daily loss limit (kill switch) ──────────────────────────────────────
    # If cumulative day-of P&L (in points, before lot multiplier) goes below
    # -DAILY_MAX_LOSS_PTS, halt all NEW entries today. Existing position is
    # still managed normally. Reset at start of each new day.
    DAILY_MAX_LOSS_PTS = float(_p('VPT_DAILY_MAX_LOSS_PTS', 0))    # 0 = off

    # ── Block re-entry after BREAKEVEN exit on same day ─────────────────────
    # Apr-May 2026 forensics: ~6 BREAKEVEN exits per 5 weeks paid pure friction
    # (₹4k aggregate) because the strategy re-fired on the same indecisive
    # day after being tagged out at entry. With this on, ONE BE exit closes
    # the day for that instrument.
    BLOCK_AFTER_BE = bool(_p('VPT_BLOCK_AFTER_BE', False))

    # ── Early-cut: force-close losing trades before normal squareoff ────────
    # If trade is held past EARLY_CUT_TIME and pnl_pts is < -EARLY_CUT_LOSS_PTS,
    # close immediately at next bar's close. Stops the slow-bleed-into-15:15
    # square-offs (Apr 15 BANKNIFTY -98 pts cost ₹16.6k combined). Set
    # EARLY_CUT_LOSS_PTS=0 to disable.
    EARLY_CUT_TIME      = _p('VPT_EARLY_CUT_TIME',      '14:30')
    EARLY_CUT_LOSS_PTS  = float(_p('VPT_EARLY_CUT_LOSS_PTS', 0))   # 0 = off

    ENTRY_WINDOW       = _p('VPT_ENTRY_WINDOW',          ('10:00', '14:00'))
    SQUAREOFF          = _p('VPT_SQUAREOFF',              '15:15')
    MAX_TRADES_DAY     = int  (_p('VPT_MAX_TRADES_PER_DAY', 2))
    DOW_ALLOW          = _p('VPT_DOW_ALLOW',              [0, 1, 2, 3, 4])

    REGIME_GAP_ATR    = float(_p('VP_REGIME_GAP_ATR',    1.5))
    REGIME_VOL_MULT   = float(_p('VP_REGIME_VOL_MULT',   1.5))
    REGIME_ACCEPT_ATR = float(_p('VP_REGIME_ACCEPT_ATR', 1.0))
    SUB_MIN_DAYS      = int  (_p('VP_SUB_MIN_DAYS',      2))

    MIN_T = dtime.fromisoformat(ENTRY_WINDOW[0])
    MAX_T = dtime.fromisoformat(ENTRY_WINDOW[1])
    SQ_T  = dtime.fromisoformat(SQUAREOFF)
    EC_T  = dtime.fromisoformat(EARLY_CUT_TIME)

    if 'Volume' not in data.columns:
        return pd.DataFrame()

    daily = _build_daily_summary(data)

    # ── Pre-compute N-day daily-close trend pct per date ────────────────────
    # For decision-day d we use YESTERDAY's close vs (N+1)-days-ago close,
    # i.e. the trend SEEN before today opens — no lookahead.
    #   trend_pct[d] = (close[d-1] - close[d-1-N]) / close[d-1-N] × 100
    trend_pct_by_date: dict = {}      # for FILTER (TREND_LOOKBACK_DAYS)
    pos_trend_pct_by_date: dict = {}  # for SIZING (POS_TREND_LOOKBACK_DAYS)

    def _trend_map(N):
        out = {}
        if N > 0 and 'close' in daily.columns:
            closes = daily['close']
            for i, d in enumerate(daily.index):
                if i >= N + 1:
                    y = closes.iloc[i - 1]
                    p = closes.iloc[i - 1 - N]
                    if pd.notna(y) and pd.notna(p) and p > 0:
                        out[d] = (y - p) / p * 100
        return out

    trend_pct_by_date     = _trend_map(TREND_LOOKBACK_DAYS)
    pos_trend_pct_by_date = _trend_map(POS_TREND_LOOKBACK_DAYS)

    # Pre-compute rolling 20-day realised vol (annualised %) and trendiness
    # at each date — using closes UP TO YESTERDAY (no lookahead).
    rv_by_date: dict = {}
    trendiness_by_date: dict = {}
    if REGIME_RV_LOOKBACK_DAYS > 0 and 'close' in daily.columns:
        closes = daily['close'].values
        idxs = list(daily.index)
        N = REGIME_RV_LOOKBACK_DAYS
        for i, d in enumerate(idxs):
            if i < N + 1:
                continue
            window_closes = closes[i - N:i]   # last N closes (excl. today)
            if len(window_closes) < N:
                continue
            rets = np.diff(window_closes) / window_closes[:-1]
            rv_ann = float(rets.std() * np.sqrt(252) * 100) \
                if len(rets) > 1 else 0.0
            net = abs(window_closes[-1] - window_closes[0])
            tot = float(np.abs(np.diff(window_closes)).sum())
            trend = net / tot if tot > 0 else 0.0
            rv_by_date[d]         = rv_ann
            trendiness_by_date[d] = trend

    def _size_factor_for(direction_int, tdate):
        """Return per-trade risk multiplier based on trend alignment."""
        if POS_TREND_LOOKBACK_DAYS <= 0:
            return 1.0, None
        tp = pos_trend_pct_by_date.get(tdate, None)
        if tp is None:
            return 1.0, None
        # |trend| inside neutral zone → flat regime → full size
        if abs(tp) <= POS_TREND_NEUTRAL_PCT:
            return 1.0, tp
        # Trend is meaningful — check if WITH or AGAINST our direction
        # direction_int = +1 LONG, -1 SHORT
        with_trend = (direction_int == 1 and tp > 0) or \
                     (direction_int == -1 and tp < 0)
        if with_trend:
            return 1.0, tp
        else:
            return POS_AGAINST_TREND_FACTOR, tp

    has_contract = 'Contract' in data.columns
    contract_for_day: dict = {}
    if has_contract:
        for ts, c in data['Contract'].items():
            d = ts.date()
            if d not in contract_for_day:
                contract_for_day[d] = c

    dates = sorted(set(data.index.date))

    # Profile state
    current_contract: str | None = None
    profile_full: dict = {}
    profile_full_days = 0
    profile_sub:  dict = {}
    profile_sub_days  = 0
    regime_start_date = None
    prev_full_poc: float | None = None

    # ── HOISTED trade state (persists across day boundaries) ────────────────
    in_trade        = False
    entry_px        = None
    entry_ts        = None
    entry_date      = None
    direction       = 0
    target_px       = None
    initial_stop    = None
    trail_stop      = None
    peak_fav_px     = None
    be_active       = False
    days_held       = 0
    vah_at_entry = val_at_entry = poc_at_entry = None
    pierce_at_entry = 0.0
    profile_used_at_entry = None
    regime_start_at_entry = None
    contract_at_entry = None
    size_factor_at_entry = 1.0
    trend_pct_at_entry: float | None = None

    # Persistent pierce snapshot — carried across day boundaries when
    # PIERCE_PERSIST_DAYS > 0 and no trade has been triggered yet.
    pierce_carry: dict | None = None  # {'state', 'pierce_extreme_px', 'days_alive'}

    # Cluster-loss cooldown state
    consecutive_losses = 0
    cooldown_until: 'date | None' = None   # block new-day entries while date < this

    # Daily-loss-limit state (resets each day)
    today_pnl_pts = 0.0
    today_be_count = 0           # # of BREAKEVEN exits on the current day
    last_pnl_date: 'date | None' = None

    records: list = []

    def _emit_exit(ts, exit_px, exit_reason, atr14):  # noqa: C901
        nonlocal today_be_count
        """Record a closed trade and reset trade state."""
        nonlocal in_trade, entry_px, entry_ts, entry_date, direction
        nonlocal target_px, initial_stop, trail_stop, peak_fav_px
        nonlocal be_active, days_held
        nonlocal vah_at_entry, val_at_entry, poc_at_entry
        nonlocal pierce_at_entry, profile_used_at_entry
        nonlocal regime_start_at_entry, contract_at_entry
        nonlocal size_factor_at_entry, trend_pct_at_entry
        nonlocal consecutive_losses, cooldown_until
        nonlocal today_pnl_pts, last_pnl_date

        # Apply slippage: pay SLIPPAGE_PTS at entry (worse fill) AND at exit.
        # Total cost: 2 × SLIPPAGE_PTS subtracted from the gross move.
        gross_pts = (exit_px - entry_px) * direction
        pnl_pts   = gross_pts - 2 * SLIPPAGE_PTS
        pnl_rs    = round(pnl_pts * LOT_SIZE - BROKERAGE, 2)
        bias    = round(min(pierce_at_entry / max(atr14, 1e-6), 1.0), 4)
        per_trade_risk_pct = POS_BASE_RISK_PCT * size_factor_at_entry
        records.append({
            'date':         entry_date,
            'entry_ts':     entry_ts,
            'exit_ts':      ts,
            'year':         entry_date.year,
            'instrument':   instrument_config.get('symbol', ''),
            'strategy':     'VP_TRAIL_SWING',
            'direction':    'LONG' if direction == 1 else 'SHORT',
            'entry':        round(entry_px, 2),
            'exit_price':   round(exit_px, 2),
            'stop':         round(initial_stop, 2),
            'target':       round(target_px, 2),
            'pnl_pts':      round(pnl_pts, 2),
            'pnl_rs':       pnl_rs,
            'win':          1 if pnl_rs > 0 else 0,
            'exit_reason':  exit_reason,
            'bias_score':   bias,
            'lots_used':    LOT_SIZE,
            'capital_used': instrument_config.get('margin_per_lot', 75_000),
            'atr14':        round(atr14, 2),
            'stop_pts':     round(abs(initial_stop - entry_px), 2),
            'target_pts':   round(abs(target_px - entry_px), 2),
            'vah':          round(vah_at_entry, 2) if vah_at_entry is not None else None,
            'val':          round(val_at_entry, 2) if val_at_entry is not None else None,
            'poc':          round(poc_at_entry, 2) if poc_at_entry is not None else None,
            'pierce_pts':   round(pierce_at_entry, 2),
            'profile_used': profile_used_at_entry,
            'regime_start': regime_start_at_entry,
            'days_held':    days_held,
            'regime':       'neutral',
            'macro_ok':     True,
            'size_factor':  size_factor_at_entry,
            'trend_pct':    round(trend_pct_at_entry, 2) if trend_pct_at_entry is not None else None,
            'per_trade_risk_pct': round(per_trade_risk_pct, 4),
        })
        in_trade = False

        # Cluster-loss tracking
        if pnl_rs < 0:
            consecutive_losses += 1
        else:
            consecutive_losses = 0
        if (CLUSTER_LOSS_COUNT > 0
                and consecutive_losses >= CLUSTER_LOSS_COUNT):
            from datetime import timedelta as _td
            cooldown_until = ts.date() + _td(days=CLUSTER_COOLDOWN_DAYS)

        # Daily-loss-limit tracking — accumulate today's P&L in points
        exit_d = ts.date()
        if last_pnl_date != exit_d:
            today_pnl_pts = 0.0
            today_be_count = 0
            last_pnl_date = exit_d
        today_pnl_pts += pnl_pts
        if exit_reason == 'BREAKEVEN':
            today_be_count += 1

    for di, tdate in enumerate(dates):
        day_df = data[data.index.date == tdate]
        if day_df.empty:
            continue

        # ── Contract roll → reset profiles AND force-close any open trade ──
        if has_contract:
            cfront = contract_for_day.get(tdate)
            if cfront != current_contract:
                if in_trade and len(day_df):
                    # Force-close at first bar of new contract (gap fill estimate)
                    first_bar = day_df.iloc[0]
                    _emit_exit(day_df.index[0], float(first_bar['Open']),
                               'CONTRACT ROLL', 300.0)
                current_contract  = cfront
                profile_full      = {}
                profile_full_days = 0
                profile_sub       = {}
                profile_sub_days  = 0
                regime_start_date = None
                prev_full_poc     = None
                pierce_carry      = None  # drop carry across roll

        # Regime shift while holding → force close
        is_shift_today = _is_regime_shift(daily, tdate, prev_full_poc,
                                          REGIME_GAP_ATR, REGIME_VOL_MULT,
                                          REGIME_ACCEPT_ATR)
        if is_shift_today:
            if in_trade and len(day_df):
                first_bar = day_df.iloc[0]
                _emit_exit(day_df.index[0], float(first_bar['Open']),
                           'REGIME SHIFT', 300.0)
            profile_sub       = {}
            profile_sub_days  = 0
            regime_start_date = tdate
            pierce_carry      = None  # drop carry across regime shift

        atr14 = float(daily.at[tdate, 'atr14']) \
            if (tdate in daily.index
                and not pd.isna(daily.at[tdate, 'atr14'])) else 300.0
        if atr14 <= 0:
            atr14 = 300.0

        # Regime filter: low-vol & strong-trend months are systematic losers
        rv_today    = rv_by_date.get(tdate, None)
        trend_today = trendiness_by_date.get(tdate, None)
        regime_blocks_entry = False
        if rv_today is not None and REGIME_MIN_RV_ANN_PCT > 0 \
                and rv_today < REGIME_MIN_RV_ANN_PCT:
            regime_blocks_entry = True
        if trend_today is not None and REGIME_MAX_TRENDINESS < 1.0 \
                and trend_today > REGIME_MAX_TRENDINESS:
            regime_blocks_entry = True

        # Post-regime-shift blackout: skip entries while sub-profile builds
        if (REGIME_BLACKOUT_DAYS > 0
                and regime_start_date is not None
                and profile_sub_days < REGIME_BLACKOUT_DAYS):
            regime_blocks_entry = True

        # Cluster-loss cooldown: freeze new-day entries after consecutive losses
        if (cooldown_until is not None
                and tdate < cooldown_until):
            regime_blocks_entry = True
        elif cooldown_until is not None and tdate >= cooldown_until:
            # cooldown elapsed — clear it. Counter stays; next loss
            # immediately re-arms cooldown if still at threshold.
            cooldown_until = None
            consecutive_losses = 0   # give a fresh start after cooldown

        # Daily loss limit — reset at start of each new day, halt entries if
        # already breached today (open position is still managed)
        if last_pnl_date != tdate:
            today_pnl_pts = 0.0
            today_be_count = 0
            last_pnl_date = tdate
        if (DAILY_MAX_LOSS_PTS > 0
                and today_pnl_pts < -DAILY_MAX_LOSS_PTS):
            regime_blocks_entry = True

        # Block re-entry after BREAKEVEN if param set
        if BLOCK_AFTER_BE and today_be_count >= 1:
            regime_blocks_entry = True

        # Adaptive Value-Area: narrow when RV is below threshold
        effective_va_pct = VA_PCT
        if (VA_PCT_RV_THRESHOLD > 0 and rv_today is not None
                and rv_today < VA_PCT_RV_THRESHOLD):
            effective_va_pct = VA_PCT_LOW_RV

        # Cold start / DOW filter — but if we're holding a trade, still manage it
        do_trade_management_only = (di < 15
                                    or profile_full_days < MIN_PROFILE_DAYS
                                    or regime_blocks_entry
                                    or (DOW_ALLOW is not None
                                        and tdate.weekday() not in DOW_ALLOW))

        bars = day_df.between_time('09:15', '15:30')
        if bars.empty:
            continue

        # If we're holding a trade overnight, increment days_held
        if in_trade:
            days_held += 1

        # Per-day pierce / setup state — DOES reset each day, UNLESS
        # carrying an unresolved pierce from yesterday.
        state = 'WATCHING'
        pierce_extreme_px = None
        pierce_extreme_bar = -1
        pierce_dir = 0
        trades_today = 0
        prev_bar = None

        # Restore carried pierce state if eligible
        if (pierce_carry is not None and PIERCE_PERSIST_DAYS > 0
                and not in_trade
                and pierce_carry['days_alive'] < PIERCE_PERSIST_DAYS):
            state             = pierce_carry['state']
            pierce_extreme_px = pierce_carry['pierce_extreme_px']
            pierce_dir        = pierce_carry['pierce_dir']
            pierce_extreme_bar = -1   # bar index reset; doesn't matter for elapsed
            pierce_carry['days_alive'] += 1
        else:
            pierce_carry = None  # drop stale carry

        for bar_i, ts in enumerate(bars.index):
            br = bars.loc[ts]
            o, h, l, c = (float(br['Open']), float(br['High']),
                          float(br['Low']),  float(br['Close']))
            v = float(br.get('Volume', 0))
            t_now = ts.time()

            if (regime_start_date is not None
                    and profile_sub_days >= SUB_MIN_DAYS
                    and profile_sub):
                active_profile   = profile_sub
                profile_used_now = 'sub'
                regime_start_now = regime_start_date
            else:
                active_profile   = profile_full
                profile_used_now = 'full'
                regime_start_now = None

            vah, val, poc, _ = _value_area(active_profile, BIN_PTS, effective_va_pct)

            # ── Manage open trade ─────────────────────────────────────────────
            if in_trade:
                # Update peak fav + ratchet trailing stop
                # Use tighter trail on subsequent days
                effective_trail_atr = (TRAIL_ATR if days_held == 0
                                       else OVERNIGHT_TRAIL_ATR)
                if direction == 1:
                    if h > peak_fav_px: peak_fav_px = h
                    candidate = peak_fav_px - effective_trail_atr * atr14
                    if candidate > trail_stop:
                        trail_stop = candidate
                else:
                    if l < peak_fav_px: peak_fav_px = l
                    candidate = peak_fav_px + effective_trail_atr * atr14
                    if candidate < trail_stop:
                        trail_stop = candidate

                # On any held-overnight day, clamp trail stop to entry (BE)
                # if not already past it. Floor-protection for gap risk.
                if days_held >= 1:
                    if direction == 1 and trail_stop < entry_px:
                        trail_stop = entry_px
                    elif direction == -1 and trail_stop > entry_px:
                        trail_stop = entry_px

                # BE arming (intraday day-0 protection)
                if not be_active and BE_TRIGGER_FRAC <= 1.0:
                    target_dist = abs(target_px - entry_px)
                    if direction == 1:
                        mfe = peak_fav_px - entry_px
                    else:
                        mfe = entry_px - peak_fav_px
                    if (target_dist > 0
                            and mfe >= BE_TRIGGER_FRAC * target_dist):
                        if direction == 1:
                            trail_stop = max(trail_stop, entry_px)
                        else:
                            trail_stop = min(trail_stop, entry_px)
                        be_active = True

                # Check exits
                exit_px = None
                exit_reason = None

                # Hard target
                if direction == 1 and h >= target_px:
                    exit_px, exit_reason = target_px, 'TARGET HIT'
                elif direction == -1 and l <= target_px:
                    exit_px, exit_reason = target_px, 'TARGET HIT'
                # Trailing stop
                elif direction == 1 and l <= trail_stop:
                    exit_px = trail_stop
                    exit_reason = ('BREAKEVEN' if be_active and trail_stop >= entry_px - 1
                                   else 'TRAIL STOP')
                elif direction == -1 and h >= trail_stop:
                    exit_px = trail_stop
                    exit_reason = ('BREAKEVEN' if be_active and trail_stop <= entry_px + 1
                                   else 'TRAIL STOP')

                # Early-cut: bail on losers before final squareoff window
                if (exit_px is None and EARLY_CUT_LOSS_PTS > 0
                        and t_now >= EC_T and t_now < SQ_T
                        and days_held == 0):
                    pnl_pts_now_ec = (c - entry_px) * direction
                    if pnl_pts_now_ec <= -EARLY_CUT_LOSS_PTS:
                        exit_px, exit_reason = c, 'EARLY CUT'

                # Day-end decision: keep or close?
                if exit_px is None and t_now >= SQ_T:
                    pnl_pts_now = (c - entry_px) * direction
                    held_too_long = days_held >= MAX_HOLD_DAYS
                    in_profit = pnl_pts_now > 0

                    if held_too_long:
                        exit_px, exit_reason = c, 'MAX HOLD'
                    elif CARRY_REQUIRES_PROFIT and not in_profit:
                        exit_px, exit_reason = c, 'SQUARE OFF'
                    # else: carry overnight (do not exit)

                if exit_px is not None:
                    _emit_exit(ts, exit_px, exit_reason, atr14)
                    state = 'WATCHING'
                    pierce_extreme_px = None
                    pierce_dir = 0
                    trades_today += 1

            # ── Hunt for new setup if not in a trade ─────────────────────────
            if (not in_trade and not do_trade_management_only
                    and trades_today < MAX_TRADES_DAY
                    and vah is not None and val is not None
                    and MIN_T <= t_now < MAX_T):

                pierce_min = PIERCE_MIN_ATR * atr14
                pierce_max = PIERCE_MAX_ATR * atr14

                if state == 'WATCHING':
                    if h > vah + pierce_min:
                        state = 'PIERCED_UP'
                        pierce_dir = -1
                        pierce_extreme_px = h
                        pierce_extreme_bar = bar_i
                    elif l < val - pierce_min:
                        state = 'PIERCED_DN'
                        pierce_dir = 1
                        pierce_extreme_px = l
                        pierce_extreme_bar = bar_i

                elif state == 'PIERCED_UP':
                    if h > pierce_extreme_px:
                        pierce_extreme_px = h
                        pierce_extreme_bar = bar_i
                    if pierce_extreme_px - vah > pierce_max:
                        state = 'WATCHING'
                        pierce_extreme_px = None; pierce_dir = 0
                    else:
                        bars_since_extreme = bar_i - pierce_extreme_bar
                        retrace = pierce_extreme_px - c
                        candle_rev_ok = (
                            (not REQUIRE_CANDLE_REV) or
                            (prev_bar is not None
                             and h <= prev_bar['high']
                             and c < o)
                        )
                        # Trend filter: skip SHORT fades when trend is strongly UP
                        trend_pct = trend_pct_by_date.get(tdate, None)
                        trend_ok = (TREND_LOOKBACK_DAYS == 0
                                    or trend_pct is None
                                    or trend_pct <= TREND_THRESHOLD_PCT)
                        if (retrace >= REVERSAL_ATR * atr14
                                and bars_since_extreme <= REVERSAL_MAX_BARS
                                and candle_rev_ok and trend_ok):
                            tgt = c - TARGET_FRAC * (c - poc)
                            if c - tgt >= MIN_TARGET_PTS:
                                direction        = -1
                                entry_px         = c
                                entry_ts         = ts
                                entry_date       = tdate
                                pierce_at_entry  = pierce_extreme_px - vah
                                vah_at_entry, val_at_entry, poc_at_entry = vah, val, poc
                                profile_used_at_entry = profile_used_now
                                regime_start_at_entry = regime_start_now
                                contract_at_entry = current_contract
                                size_factor_at_entry, trend_pct_at_entry = \
                                    _size_factor_for(direction, tdate)
                                target_px        = tgt
                                initial_stop     = pierce_extreme_px + INITIAL_STOP_BUFFER_ATR * atr14
                                trail_stop       = initial_stop
                                peak_fav_px      = c
                                be_active        = False
                                days_held        = 0
                                in_trade         = True
                                state            = 'IN_TRADE'
                        if not in_trade and bars_since_extreme > REVERSAL_MAX_BARS:
                            state = 'WATCHING'
                            pierce_extreme_px = None; pierce_dir = 0

                elif state == 'PIERCED_DN':
                    if l < pierce_extreme_px:
                        pierce_extreme_px = l
                        pierce_extreme_bar = bar_i
                    if val - pierce_extreme_px > pierce_max:
                        state = 'WATCHING'
                        pierce_extreme_px = None; pierce_dir = 0
                    else:
                        bars_since_extreme = bar_i - pierce_extreme_bar
                        retrace = c - pierce_extreme_px
                        candle_rev_ok = (
                            (not REQUIRE_CANDLE_REV) or
                            (prev_bar is not None
                             and l >= prev_bar['low']
                             and c > o)
                        )
                        # Trend filter: skip LONG fades when trend is strongly DOWN
                        trend_pct = trend_pct_by_date.get(tdate, None)
                        trend_ok = (TREND_LOOKBACK_DAYS == 0
                                    or trend_pct is None
                                    or trend_pct >= -TREND_THRESHOLD_PCT)
                        if (retrace >= REVERSAL_ATR * atr14
                                and bars_since_extreme <= REVERSAL_MAX_BARS
                                and candle_rev_ok and trend_ok):
                            tgt = c + TARGET_FRAC * (poc - c)
                            if tgt - c >= MIN_TARGET_PTS:
                                direction        = 1
                                entry_px         = c
                                entry_ts         = ts
                                entry_date       = tdate
                                pierce_at_entry  = val - pierce_extreme_px
                                vah_at_entry, val_at_entry, poc_at_entry = vah, val, poc
                                profile_used_at_entry = profile_used_now
                                regime_start_at_entry = regime_start_now
                                contract_at_entry = current_contract
                                size_factor_at_entry, trend_pct_at_entry = \
                                    _size_factor_for(direction, tdate)
                                target_px        = tgt
                                initial_stop     = pierce_extreme_px - INITIAL_STOP_BUFFER_ATR * atr14
                                trail_stop       = initial_stop
                                peak_fav_px      = c
                                be_active        = False
                                days_held        = 0
                                in_trade         = True
                                state            = 'IN_TRADE'
                        if not in_trade and bars_since_extreme > REVERSAL_MAX_BARS:
                            state = 'WATCHING'
                            pierce_extreme_px = None; pierce_dir = 0

            # Always grow profiles after decisions
            _bar_distribute_volume(profile_full, l, h, v, BIN_PTS)
            if regime_start_date is not None:
                _bar_distribute_volume(profile_sub, l, h, v, BIN_PTS)

            prev_bar = {'open': o, 'high': h, 'low': l, 'close': c}

        # End-of-day: snapshot pierce state for tomorrow if eligible
        if (PIERCE_PERSIST_DAYS > 0 and not in_trade
                and state in ('PIERCED_UP', 'PIERCED_DN')
                and pierce_extreme_px is not None):
            if pierce_carry is None:
                pierce_carry = {
                    'state': state,
                    'pierce_extreme_px': pierce_extreme_px,
                    'pierce_dir': pierce_dir,
                    'days_alive': 0,
                }
            else:
                pierce_carry['state'] = state
                pierce_carry['pierce_extreme_px'] = pierce_extreme_px
                pierce_carry['pierce_dir'] = pierce_dir
        else:
            # Pierce resolved (trade taken) or no active pierce — clear carry
            pierce_carry = None

        # Day finished
        profile_full_days += 1
        if regime_start_date is not None:
            profile_sub_days += 1
        _, _, prev_full_poc, _ = _value_area(profile_full, BIN_PTS, VA_PCT)

    # If we end the entire dataset with an open trade, close at last bar
    if in_trade and dates:
        last_date = dates[-1]
        last_bars = data[data.index.date == last_date]
        if len(last_bars):
            _emit_exit(last_bars.index[-1], float(last_bars.iloc[-1]['Close']),
                       'END OF DATA', 300.0)

    return pd.DataFrame(records)
