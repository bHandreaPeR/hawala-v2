"""
Run each morning to refresh the Groww API key:
    python set_tokens.py

Telegram details are stored once and never overwritten.
"""
import getpass, os, pathlib, re

ENV_FILE = pathlib.Path(__file__).parent / 'token.env'

# ── Read existing values so Telegram creds survive across runs ────────────────
existing = {}
if ENV_FILE.exists():
    for line in ENV_FILE.read_text().splitlines():
        if '=' in line:
            k, _, v = line.partition('=')
            existing[k.strip()] = v.strip()

# ── Groww key: always prompt (changes daily) ──────────────────────────────────
import subprocess
groww_key = subprocess.check_output('pbpaste', text=True).strip()
if not groww_key:
    print('❌  Clipboard is empty — copy your Groww API key first, then run this.')
    raise SystemExit(1)
print(f'✅  Key read from clipboard ({len(groww_key)} chars)')

# ── Telegram: prompt only if not already set ──────────────────────────────────
tg_token = existing.get('TELEGRAM_BOT_TOKEN', '')
tg_chat  = existing.get('TELEGRAM_CHAT_ID', '')

if not tg_token:
    tg_token = input('Telegram bot token (from @BotFather): ').strip()
if not tg_chat:
    tg_chat  = input('Telegram chat_id: ').strip()

# ── Write ─────────────────────────────────────────────────────────────────────
ENV_FILE.write_text(
    f"GROWW_API_KEY={groww_key}\n"
    f"TELEGRAM_BOT_TOKEN={tg_token}\n"
    f"TELEGRAM_CHAT_ID={tg_chat}\n"
)
print(f"✅  token.env written  ({ENV_FILE})")
