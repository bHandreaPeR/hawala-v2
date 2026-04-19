import requests


def send(token: str, chat_id: str, text: str) -> bool:
    """Send a Telegram message. Returns True on success."""
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
        if not r.ok:
            print(f"  ⚠ Telegram error {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"  ⚠ Telegram send failed: {e}")
        return False


def send_document(token: str, chat_id: str, file_path: str, caption: str = "") -> bool:
    """Send a file (e.g. PDF) via Telegram sendDocument. Returns True on success."""
    try:
        with open(file_path, "rb") as f:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
                files={"document": f},
                timeout=60,
            )
        if not r.ok:
            print(f"  ⚠ Telegram sendDocument error {r.status_code}: {r.text[:200]}")
            return False
        return True
    except Exception as e:
        print(f"  ⚠ Telegram sendDocument failed: {e}")
        return False
