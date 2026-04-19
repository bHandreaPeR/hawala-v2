"""
run_daily_report.py — Hawala v2 Automated Daily Report

Fetches all data live → saves JSON → builds dark-theme PDF → sends via Telegram.

Cron: 02:00 UTC Mon-Fri (= 07:30 IST)
    cd /path/to/project && python run_daily_report.py

Manual:
    python run_daily_report.py
"""

import json
import os
import pathlib
import sys
import datetime

# ── Load credentials from token.env ─────────────────────────────────────────
ENV_FILE = pathlib.Path(__file__).parent / "token.env"

def _load_env():
    if not ENV_FILE.exists():
        print(f"❌  {ENV_FILE} not found — run set_tokens.py first")
        sys.exit(1)
    env = {}
    for line in ENV_FILE.read_text().splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    env = _load_env()
    tg_token   = env.get("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = env.get("TELEGRAM_CHAT_ID", "")

    if not tg_token or not tg_chat_id:
        print("⚠  Telegram credentials not set in token.env — PDF will be generated but NOT sent")

    # ── Step 1: Fetch all data ────────────────────────────────────────────
    from data.fetch_report_data import fetch_all
    data = fetch_all()

    # ── Step 2: Save JSON signal file ────────────────────────────────────
    logs_dir = pathlib.Path(__file__).parent / "trade_logs"
    logs_dir.mkdir(exist_ok=True)
    date_iso  = data.get("date_iso", datetime.date.today().isoformat())
    json_path = logs_dir / f"market_signal_{date_iso}.json"
    json_path.write_text(json.dumps(data, indent=2, default=str))
    print(f"  ✅ JSON saved → {json_path}")

    # ── Step 3: Build HTML (source of truth) ─────────────────────────────
    from gen_html_report import build_html
    html_path = pathlib.Path(__file__).parent / f"market_report_{date_iso}.html"
    html_path.write_text(build_html(data))
    print(f"  ✅ HTML saved → {html_path}")

    # ── Step 3b: Convert HTML → PDF for Telegram ─────────────────────────
    pdf_path = pathlib.Path(__file__).parent / f"market_report_{date_iso}.pdf"
    _converted = False

    # 1. Headless Chrome (best fidelity — matches HTML exactly)
    _chrome_paths = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium-browser",
        "google-chrome",
        "chromium",
    ]
    for _chrome in _chrome_paths:
        try:
            import subprocess
            result = subprocess.run(
                [_chrome, "--headless", "--disable-gpu",
                 f"--print-to-pdf={pdf_path}",
                 "--no-margins", "--print-to-pdf-no-header",
                 html_path.as_uri()],
                capture_output=True, timeout=60
            )
            if result.returncode == 0 and pdf_path.exists():
                print(f"  ✅ PDF (headless Chrome) → {pdf_path}")
                _converted = True
                break
        except Exception:
            continue

    # 2. wkhtmltopdf fallback
    if not _converted:
        try:
            import subprocess
            result = subprocess.run(
                ["wkhtmltopdf", "--page-size", "A4", "--enable-local-file-access",
                 "--no-stop-slow-scripts", "--javascript-delay", "200",
                 str(html_path), str(pdf_path)],
                capture_output=True, timeout=60
            )
            if result.returncode == 0:
                print(f"  ✅ PDF (wkhtmltopdf) → {pdf_path}")
                _converted = True
        except Exception:
            pass

    # 3. weasyprint fallback
    if not _converted:
        try:
            import weasyprint
            weasyprint.HTML(filename=str(html_path)).write_pdf(str(pdf_path))
            print(f"  ✅ PDF (weasyprint) → {pdf_path}")
            _converted = True
        except Exception:
            pass

    # 4. reportlab dark-theme fallback
    if not _converted:
        from gen_report import build_pdf
        build_pdf(data, str(pdf_path))
        print(f"  ℹ️  PDF (reportlab fallback) → {pdf_path}")

    # ── Step 4: Send Telegram ─────────────────────────────────────────────
    if not (tg_token and tg_chat_id):
        print("⚠  Telegram not configured — skipping send")
        return

    from alerts.telegram import send, send_document

    sig    = data.get("hawala_signal", {})
    bn     = data.get("banknifty_analysis", {})
    overall = sig.get("overall", "—")
    gap_pts = bn.get("gap_pts", "—")
    vix     = data.get("india_vix", {}).get("price", "—")
    sp_chg  = sig.get("sp_chg", "—")
    fii_net = sig.get("fii_net", "—")
    date_str = data.get("date_str", date_iso)
    gen_at   = data.get("generated_at", "—")

    def _fmt_pct(v):
        try:
            f = float(str(v).replace("%","").replace("+",""))
            return f"+{f:.2f}%" if f >= 0 else f"{f:.2f}%"
        except:
            return str(v)

    def _sign(v):
        try:
            return "+" if float(str(v).replace("%","").replace("+","")) >= 0 else ""
        except:
            return ""

    gap_str = f"{_sign(gap_pts)}{float(gap_pts):,.0f} pts" if gap_pts not in ("—", None) else "—"
    fii_str = f"₹{float(fii_net):,.0f} Cr" if fii_net not in ("—", None) else "pending"

    signal_emoji = "🔴" if overall == "NO TRADE" else "🟢"
    summary = (
        f"📊 <b>HAWALA v2 — {date_str}</b>  |  {gen_at}\n\n"
        f"<b>BankNifty:</b> {bn.get('prev_close','—'):,.0f}  |  Gap est: <b>{gap_str}</b>\n"
        f"<b>Signal:</b> {signal_emoji} <b>{overall}</b>\n"
        f"<i>{sig.get('reason','')}</i>\n\n"
        f"VIX: {vix}  |  S&amp;P: {_fmt_pct(sp_chg)}  |  FII: {fii_str}\n\n"
        f"Full report ↓"
    )

    print("\n📤 Sending Telegram summary...")
    send(tg_token, tg_chat_id, summary)

    print("📤 Sending PDF...")
    send_document(
        tg_token,
        tg_chat_id,
        str(pdf_path),
        caption=f"Hawala v2 Pre-Market Report — {date_str}",
    )

    print("\n✅  Done.")


if __name__ == "__main__":
    main()
