"""
Run each morning to refresh the Groww API key + TOTP secret:
    python set_tokens.py

Telegram details and TOTP secret are stored once and never overwritten.
"""
import getpass, os, pathlib, subprocess

ENV_FILE = pathlib.Path(__file__).parent / 'token.env'

# ── Read existing values so creds survive across runs ────────────────────────
existing = {}
if ENV_FILE.exists():
    for line in ENV_FILE.read_text().splitlines():
        if '=' in line:
            k, _, v = line.partition('=')
            existing[k.strip()] = v.strip()

# ── Groww API key: always read from clipboard (changes daily) ─────────────────
groww_key = subprocess.check_output('pbpaste', text=True).strip()
if not groww_key:
    print('❌  Clipboard is empty — copy your Groww API key first, then run this.')
    raise SystemExit(1)
print(f'✅  Groww API key read from clipboard ({len(groww_key)} chars)')

# ── GROWW TOTP secret: prompt only if not already set ─────────────────────────
totp_secret = existing.get('GROWW_TOTP_SECRET', '')
if not totp_secret:
    totp_secret = input('Groww TOTP secret (from Groww 2FA setup): ').strip()

# ── Telegram: prompt only if not already set ──────────────────────────────────
tg_token   = existing.get('TELEGRAM_BOT_TOKEN', '')
# Support both old single key and new multi-key
tg_chat_ids = existing.get('TELEGRAM_CHAT_IDS', existing.get('TELEGRAM_CHAT_ID', ''))

if not tg_token:
    tg_token = input('Telegram bot token (from @BotFather): ').strip()
if not tg_chat_ids:
    tg_chat_ids = input('Telegram chat_id(s) — comma-separated for multiple: ').strip()

# ── Write ─────────────────────────────────────────────────────────────────────
ENV_FILE.write_text(
    f"GROWW_API_KEY={groww_key}\n"
    f"GROWW_TOTP_SECRET={totp_secret}\n"
    f"TELEGRAM_BOT_TOKEN={tg_token}\n"
    f"TELEGRAM_CHAT_IDS={tg_chat_ids}\n"
)
print(f"✅  token.env written  ({ENV_FILE})")
