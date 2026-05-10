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
    # Newsletter goes to the MACRO bot (briefs only). The TRADE bot is
    # reserved for entries/exits/P&L messages.
    tg_token   = env.get("TELEGRAM_BOT_TOKEN_MACRO", "")
    TG_CHAT_IDS = env.get('TELEGRAM_CHAT_IDS_MACRO', '').split(',')
    TG_CHAT_IDS = [cid.strip() for cid in TG_CHAT_IDS if cid.strip()]

    if not tg_token or (TG_CHAT_IDS and not TG_CHAT_IDS[0]):
        print("⚠  TELEGRAM_BOT_TOKEN_MACRO / TELEGRAM_CHAT_IDS_MACRO not set "
              "in token.env — PDF will be generated but NOT sent")

    # ── Step 0: Refresh all v3 data caches before fetching report data ────
    # Runs morning_fetch.sh (FII F&O + FII cash) and daily_fetch.sh
    # (candles, option OI, bhavcopy/PCR). Both are safe to run at 7:30 AM —
    # they fetch the previous trading day's data published by NSE overnight.
    import subprocess
    _project_root = pathlib.Path(__file__).parent

    def _run_shell(label: str, script_path: pathlib.Path) -> None:
        print(f"\n{'─'*55}")
        print(f"  {label}")
        print(f"{'─'*55}")
        if not script_path.exists():
            print(f"  ⚠  {script_path} not found — skipping")
            return
        try:
            result = subprocess.run(
                ["bash", str(script_path)],
                cwd=str(_project_root),
                capture_output=False,   # print live to stdout
                timeout=300,            # 5 min hard limit per script
            )
            if result.returncode != 0:
                print(f"  ⚠  {label} exited with code {result.returncode} — continuing")
        except subprocess.TimeoutExpired:
            print(f"  ⚠  {label} timed out after 5 min — continuing")
        except Exception as e:
            print(f"  ⚠  {label} error: {e} — continuing")

    _run_shell("morning_fetch.sh  (FII F&O + FII cash)",
               _project_root / "v3" / "scripts" / "morning_fetch.sh")
    _run_shell("daily_fetch.sh  (candles + option OI + bhavcopy/PCR)",
               _project_root / "v3" / "scripts" / "daily_fetch.sh")

    print(f"\n{'─'*55}")
    print("  All caches refreshed — starting report fetch")
    print(f"{'─'*55}\n")

    # ── Step 1: Fetch all data ────────────────────────────────────────────
    from data.fetch_report_data import fetch_all
    data = fetch_all()

    # ── Step 2: Save JSON signal file ────────────────────────────────────
    signals_dir = pathlib.Path(__file__).parent / "data_dumps" / "signals"
    signals_dir.mkdir(parents=True, exist_ok=True)
    date_iso  = data.get("date_iso", datetime.date.today().isoformat())
    json_path = signals_dir / f"market_signal_{date_iso}.json"
    json_path.write_text(json.dumps(data, indent=2, default=str))
    print(f"  ✅ JSON saved → {json_path}")

    # ── Step 3: Build HTML (source of truth) ─────────────────────────────
    from gen_html_report import build_html
    html_dir = pathlib.Path(__file__).parent / "data_dumps" / "newsletters_archive"
    html_dir.mkdir(parents=True, exist_ok=True)
    html_path = html_dir / f"market_report_{date_iso}.html"
    html_path.write_text(build_html(data))
    print(f"  ✅ HTML saved → {html_path}")

    # ── Step 3b: Convert HTML → PDF for Telegram ─────────────────────────
    # Build a friendly newsletter filename: "Newsletter_13th May 26.pdf"
    _today = datetime.date.fromisoformat(date_iso) if date_iso else datetime.date.today()
    _day = _today.day
    if 10 <= _day % 100 <= 20:
        _suffix = 'th'
    else:
        _suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(_day % 10, 'th')
    _newsletter_name = (
        f"Newsletter {_day}{_suffix} {_today.strftime('%B')} "
        f"{_today.strftime('%y')}.pdf"
    )
    newsletter_dir = pathlib.Path(__file__).parent / "data_dumps" / "newsletters"
    newsletter_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = newsletter_dir / _newsletter_name
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

    # ── Step 4: Send Newsletter PDF to MACRO channel (no summary message) ──
    if not (tg_token and TG_CHAT_IDS and TG_CHAT_IDS[0]):
        print("⚠  MACRO Telegram not configured — skipping send")
        return

    from alerts.telegram import send_document

    print(f"\n📤 Sending Newsletter PDF → MACRO channel "
          f"({len(TG_CHAT_IDS)} chat ids)...")
    for chat_id in TG_CHAT_IDS:
        send_document(
            tg_token,
            chat_id,
            str(pdf_path),
            caption="",   # no caption — PDF only
        )

    print("\n✅  Done.")


if __name__ == "__main__":
    main()
