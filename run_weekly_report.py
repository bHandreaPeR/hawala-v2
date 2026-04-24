"""
run_weekly_report.py — Hawala v2 Weekly Performance Summary

Reads live_trades.csv (written by alert_runner.py) and generates a
weekly summary covering Mon–Fri of the most recently completed trading week.

Sends formatted summary + table via Telegram.

Schedule: Saturday 18:00 IST via cron
    0 12 * * 6 cd /path/to/project && python run_weekly_report.py
"""

import os, sys, pathlib, datetime, json
import pandas as pd
from dotenv import load_dotenv

load_dotenv('token.env')
TG_TOKEN    = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT_IDS = [c.strip() for c in os.getenv('TELEGRAM_CHAT_IDS', '').split(',') if c.strip()]

TRADE_LOG   = pathlib.Path('trade_logs/live_trades.csv')
SIGNAL_DIR  = pathlib.Path('trade_logs')
DOW_NAMES   = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
DOW_ALLOW   = [1, 2, 4]   # Tue, Wed, Fri (matches config ORB_DOW_ALLOW)

# ── Capital context from latest backtest ─────────────────────────────────────
BACKTEST_END_EQUITY  = 1_010_426   # ₹ from full_backtest_2026-YTD.csv last row
LIVE_LOT_SIZE        = 30          # BANKNIFTY post-Nov 2024
LIVE_LOTS_PER_TRADE  = 1           # alert_runner uses 1 lot fixed
MARGIN_PER_LOT       = 75_000      # approx SPAN + exposure margin


# ── Helpers ───────────────────────────────────────────────────────────────────

def _week_range() -> tuple[datetime.date, datetime.date]:
    """Return Mon–Fri of the most recently completed trading week."""
    today = datetime.date.today()
    # Saturday=5 → last week Mon; Sunday=6 → last week Mon; else → this week Mon
    if today.weekday() == 5:
        monday = today - datetime.timedelta(days=5)
    elif today.weekday() == 6:
        monday = today - datetime.timedelta(days=6)
    else:
        # We're mid-week: report last week
        monday = today - datetime.timedelta(days=today.weekday() + 7)
    friday = monday + datetime.timedelta(days=4)
    return monday, friday


def _signal_for_date(d: datetime.date) -> dict:
    """Pull hawala_signal from the daily JSON if available."""
    f = SIGNAL_DIR / f'market_signal_{d.isoformat()}.json'
    if not f.exists():
        return {}
    try:
        return json.loads(f.read_text()).get('hawala_signal', {})
    except Exception:
        return {}


def _dow_label(wd: int) -> str:
    if wd not in DOW_ALLOW:
        return '🚫 DOW block'
    return '✅ trade day'


def _pnl_sign(v: float) -> str:
    return f'+₹{v:,.0f}' if v >= 0 else f'-₹{abs(v):,.0f}'


# ── Main ──────────────────────────────────────────────────────────────────────

def build_weekly_report() -> str:
    monday, friday = _week_range()
    week_label = f"{monday.strftime('%d %b')} – {friday.strftime('%d %b %Y')}"

    # ── Load live trades for the week ────────────────────────────────────────
    if TRADE_LOG.exists():
        df = pd.read_csv(TRADE_LOG, parse_dates=['date'])
        df['date'] = pd.to_datetime(df['date']).dt.date
        week_df = df[(df['date'] >= monday) & (df['date'] <= friday)].copy()
    else:
        week_df = pd.DataFrame()

    # ── Build day-by-day table ───────────────────────────────────────────────
    rows = []
    total_pnl    = 0.0
    trade_count  = 0
    win_count    = 0

    for i in range(5):
        d   = monday + datetime.timedelta(days=i)
        wd  = d.weekday()
        sig = _signal_for_date(d)

        day_trades = week_df[week_df['date'] == d] if not week_df.empty else pd.DataFrame()

        if wd not in DOW_ALLOW:
            status = '🚫 DOW'
            detail = f"({DOW_NAMES[wd]} excluded)"
            pnl_str = '—'
        elif not sig:
            status = '📭 No data'
            detail = ''
            pnl_str = '—'
        elif sig.get('macro_blocked'):
            status = '🚧 Macro block'
            detail = sig.get('reason', '')
            pnl_str = '—'
        elif day_trades.empty:
            # Has signal but no trade logged (runner may not have run)
            overall = sig.get('overall', '—')
            status  = f'📡 {overall}'
            gap_pts = sig.get('gap_pts', '—')
            detail  = f"gap {gap_pts:+.0f} pts" if isinstance(gap_pts, (int, float)) else ''
            pnl_str = '(no live data)'
        else:
            trade_rows = day_trades[day_trades['strategy'] != 'NO_TRADE']
            if trade_rows.empty:
                status  = '⏭ No entry'
                detail  = ''
                pnl_str = '—'
            else:
                day_pnl = float(trade_rows['pnl_rs'].sum())
                total_pnl   += day_pnl
                trade_count += len(trade_rows)
                win_count   += int((trade_rows['pnl_rs'] > 0).sum())
                strats = ', '.join(trade_rows['strategy'].unique())
                status  = '🎯' if day_pnl > 0 else '🛑'
                detail  = strats
                pnl_str = _pnl_sign(day_pnl)

        rows.append({
            'day':    DOW_NAMES[wd],
            'date':   d.strftime('%d %b'),
            'status': status,
            'detail': detail,
            'pnl':    pnl_str,
        })

    # ── Trade detail table ───────────────────────────────────────────────────
    trade_lines = []
    if not week_df.empty:
        actual = week_df[week_df['strategy'] != 'NO_TRADE'].sort_values('date')
        for _, r in actual.iterrows():
            pnl_icon = '🎯' if float(r['pnl_rs']) > 0 else '🛑'
            entry_t  = str(r.get('entry_time', '')).strip()
            exit_t   = str(r.get('exit_time', '')).strip()
            times    = f"{entry_t}→{exit_t}" if entry_t and exit_t else '—'
            trade_lines.append(
                f"  {r['date'].strftime('%d %b')} {DOW_NAMES[r['date'].weekday()]}  "
                f"{r['strategy']:<12} {str(r.get('direction','')):<6} "
                f"E:{float(r['entry']):.0f} X:{float(r['exit']):.0f}  "
                f"{times}  {pnl_icon} {_pnl_sign(float(r['pnl_rs']))}"
            )

    win_rate = round(win_count / trade_count * 100, 1) if trade_count else 0

    # ── Format Telegram message ──────────────────────────────────────────────
    lines = [
        f"📊 <b>HAWALA v2 — Weekly Report</b>",
        f"<b>{week_label}</b>",
        "",
        "<b>Day-by-Day:</b>",
    ]
    for r in rows:
        lines.append(f"  <b>{r['day']} {r['date']}</b>  {r['status']}  {r['detail']}  {r['pnl']}")

    lines += [
        "",
        "<b>Trades Executed:</b>",
    ]
    if trade_lines:
        lines += trade_lines
    else:
        lines.append("  (no live trades logged this week — check if alert_runner ran)")

    lines += [
        "",
        f"<b>Week P&amp;L:  {_pnl_sign(total_pnl)}</b>   |  "
        f"Trades: {trade_count}  |  WR: {win_rate:.0f}%",
        "",
        "<b>Capital note:</b>",
        f"  Live runner: <b>1 lot × {LIVE_LOT_SIZE} units</b> (fixed, ~₹{MARGIN_PER_LOT:,} margin)",
        f"  Backtest equity (compounded): ₹{BACKTEST_END_EQUITY:,}",
        f"  At backtest scale, current lot size would be ~{max(1, BACKTEST_END_EQUITY//MARGIN_PER_LOT)} lots",
        "",
        "<b>DOW Filter:</b>  Mon ❌  Tue ✅  Wed ✅  Thu ❌  Fri ✅",
        "(Mon + Thu excluded — weekly expiry + Monday volatility)",
    ]

    return '\n'.join(lines)


def main():
    print("📋 Generating weekly report...")
    report = build_weekly_report()
    print(report)

    if not (TG_TOKEN and TG_CHAT_IDS):
        print("\n⚠  Telegram not configured — printing only.")
        return

    from alerts.telegram import send
    for chat_id in TG_CHAT_IDS:
        send(TG_TOKEN, chat_id, report)
    print("\n✅ Weekly report sent via Telegram.")


if __name__ == '__main__':
    main()
