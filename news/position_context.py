"""Read v3 runner state from their log files (no edits to runners required).

Returns one dict per runner with the latest known state:
  {
    "instrument": "NIFTY" | "BANKNIFTY",
    "in_position": bool,
    "side": "CE"|"PE"|None,
    "strike": int|None,
    "entry_price": float|None,
    "ltp": float|None,
    "pnl_pct": float|None,
    "pnl_inr": float|None,
    "direction": +1|-1|0,
    "smoothed_score": float|None,
    "last_log_ts": str (ISO),
    "exited": bool,        # last seen exit (since last entry)
    "exit_reason": str|None,
  }
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT      = Path(__file__).resolve().parent.parent
NIFTY_LOG = ROOT / "v3" / "live" / "runner.log"
BN_LOG    = ROOT / "v3" / "live" / "runner_banknifty.log"

IST = timezone(timedelta(hours=5, minutes=30))


_TS_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
_ENTER_RE = re.compile(
    r"\[(?P<mode>PAPER|LIVE)\]\s+ENTER\s+(?P<side>CE|PE)\s+BUY\s+"
    r"strike=(?P<strike>\d+)\s+@\s+(?P<entry>[\d.]+)\s+qty=(?P<qty>\d+)\s+"
    r"score=(?P<score>[-+\d.]+)"
)
_POS_RE = re.compile(
    r"\[POSITION\]\s+(?P<side>CE|PE)\s+strike=(?P<strike>\d+)\s+"
    r"entry=(?P<entry>[\d.]+)\s+ltp=(?P<ltp>[\d.]+)\s+"
    r"pnl=(?P<pnl_pts>[-+\d.]+)\s+pts\s+\((?P<pnl_pct>[-+\d.]+)%\)\s+"
    r"₹(?P<pnl_inr>[-+\d.]+)"
)
_EXIT_RE = re.compile(
    r"\[EXIT\s+(?P<reason>TP|SL|REVERSAL|EOD|NEWS)\]"
)
_NOENTRY_RE = re.compile(
    r"No entry this bar:\s+effective_dir=(?P<dir>[-+\d]+)\s+"
    r"\(smoothed=(?P<smoothed>[-+\d]+)\s+score=(?P<score>[-+\d.]+)"
)
_DAYSTART_RE = re.compile(r"Live Runner starting")


def _read_tail(path: Path, max_lines: int = 1500) -> list[str]:
    if not path.exists():
        return []
    try:
        # Lightweight tail — read the last ~256KB and split
        size = path.stat().st_size
        with path.open("rb") as f:
            if size > 256 * 1024:
                f.seek(-256 * 1024, 2)
                f.readline()  # discard partial
            data = f.read().decode("utf-8", errors="replace")
        lines = data.splitlines()
        return lines[-max_lines:]
    except Exception:
        return []


def _parse_ts(line: str) -> str | None:
    m = _TS_RE.match(line)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group("ts"), "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST)
        return dt.isoformat()
    except Exception:
        return None


def _parse_runner(log_path: Path, instrument: str) -> dict:
    state = {
        "instrument":     instrument,
        "in_position":    False,
        "side":           None,
        "strike":         None,
        "entry_price":    None,
        "ltp":            None,
        "pnl_pct":        None,
        "pnl_inr":        None,
        "direction":      0,
        "smoothed_score": None,
        "last_log_ts":    None,
        "exited":         False,
        "exit_reason":    None,
    }

    lines = _read_tail(log_path)
    if not lines:
        return state

    state["last_log_ts"] = _parse_ts(lines[-1]) or None

    # Walk lines in order — most recent ENTRY/EXIT/POSITION/NOENTRY wins per kind.
    last_entry_idx = -1
    last_exit_idx  = -1

    for i, ln in enumerate(lines):
        if _DAYSTART_RE.search(ln):
            # Reset on a new day's startup banner — older state irrelevant
            state["in_position"] = False
            state["exited"] = False
            state["exit_reason"] = None
            last_entry_idx = -1
            last_exit_idx  = -1
            continue

        m = _ENTER_RE.search(ln)
        if m:
            last_entry_idx = i
            state["in_position"] = True
            state["side"]        = m.group("side")
            state["strike"]      = int(m.group("strike"))
            state["entry_price"] = float(m.group("entry"))
            state["direction"]   = +1 if m.group("side") == "CE" else -1
            state["exited"]      = False
            state["exit_reason"] = None
            state["last_log_ts"] = _parse_ts(ln) or state["last_log_ts"]
            continue

        m = _POS_RE.search(ln)
        if m and state["in_position"]:
            state["ltp"]     = float(m.group("ltp"))
            state["pnl_pct"] = float(m.group("pnl_pct"))
            state["pnl_inr"] = float(m.group("pnl_inr"))
            state["last_log_ts"] = _parse_ts(ln) or state["last_log_ts"]
            continue

        m = _EXIT_RE.search(ln)
        if m and last_entry_idx >= 0:
            last_exit_idx = i
            if last_exit_idx > last_entry_idx:
                state["in_position"] = False
                state["exited"]      = True
                state["exit_reason"] = m.group("reason")
                state["last_log_ts"] = _parse_ts(ln) or state["last_log_ts"]
            continue

        m = _NOENTRY_RE.search(ln)
        if m and not state["in_position"]:
            state["smoothed_score"] = float(m.group("score"))
            try:
                d = int(m.group("smoothed").replace("+", ""))
                state["direction"] = d
            except Exception:
                pass
            state["last_log_ts"] = _parse_ts(ln) or state["last_log_ts"]

    return state


def read_v3_state() -> dict:
    """Read both runners. Returns {nifty: {...}, banknifty: {...}}."""
    return {
        "nifty":     _parse_runner(NIFTY_LOG, "NIFTY"),
        "banknifty": _parse_runner(BN_LOG,    "BANKNIFTY"),
    }


def impact_for(news_direction: int, runner_state: dict) -> str:
    """Plain-English line summarizing how news affects this runner."""
    if news_direction == 0:
        return "neutral news"
    label_dir = "BULLISH" if news_direction == +1 else "BEARISH"

    if runner_state["in_position"]:
        pos_dir = runner_state["direction"]
        if pos_dir == news_direction:
            return f"news AGREES with open {runner_state['side']} → consider holding / let TP run"
        else:
            return f"news CONTRADICTS open {runner_state['side']} → consider tightening exit / partial book"

    # No position
    smoothed = runner_state.get("smoothed_score")
    if smoothed is None:
        return f"no open position; runner has no recent signal data"
    same_side = (smoothed > 0 and news_direction == +1) or (smoothed < 0 and news_direction == -1)
    if same_side:
        return (f"no position yet; runner score={smoothed:+.2f} aligns with {label_dir} news — "
                f"watch next bars for entry trigger")
    return (f"no position; runner score={smoothed:+.2f} OPPOSES {label_dir} news — "
            f"news may delay or veto a v3 entry")


if __name__ == "__main__":
    import json
    print(json.dumps(read_v3_state(), indent=2))
