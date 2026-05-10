"""Three-layer dedup: exact-hash → event-key clusters → Jaccard fallback.

State files (under news/state/):
  seen.json            { hash: ts_iso }                    rolling 24h
  event_clusters.json  { event_key: cluster_dict }         rolling 90 min
  alerted.json         { event_key: ts_iso }               rolling 6h

A `cluster_dict` is:
  {
    "event_key":    str,
    "first_seen":   ts_iso,
    "last_seen":    ts_iso,
    "headlines":    [ {headline, source, tier, ts_seen, score, confidence,
                       event_class, direction}, ... ],
    "tokens":       [str, ...]   # union token set, used for Jaccard against
                                 # newcomers
  }
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import normalize as N


STATE_DIR = Path(__file__).parent / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

SEEN_PATH      = STATE_DIR / "seen.json"
CLUSTERS_PATH  = STATE_DIR / "event_clusters.json"
ALERTED_PATH   = STATE_DIR / "alerted.json"

SEEN_TTL_H        = 24
CLUSTER_TTL_MIN   = 90
ALERTED_TTL_H     = 6
JACCARD_THRESHOLD = 0.35
MIN_SHARED_TOKENS = 2  # 2 distinctive content tokens shared = same event

IST = timezone(timedelta(hours=5, minutes=30))


def _now() -> datetime:
    return datetime.now(IST)


def _load(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _save(path: Path, obj: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2, default=str))
    tmp.replace(path)


def _parse_ts(s: str | datetime) -> datetime:
    if isinstance(s, datetime):
        return s if s.tzinfo else s.replace(tzinfo=IST)
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=IST)
    except Exception:
        return _now()


def prune_state() -> None:
    """Drop expired entries from all three state files."""
    now = _now()

    seen = _load(SEEN_PATH)
    cutoff = now - timedelta(hours=SEEN_TTL_H)
    seen = {h: ts for h, ts in seen.items() if _parse_ts(ts) > cutoff}
    _save(SEEN_PATH, seen)

    clusters = _load(CLUSTERS_PATH)
    cutoff = now - timedelta(minutes=CLUSTER_TTL_MIN)
    clusters = {k: c for k, c in clusters.items() if _parse_ts(c.get("last_seen")) > cutoff}
    _save(CLUSTERS_PATH, clusters)

    alerted = _load(ALERTED_PATH)
    cutoff = now - timedelta(hours=ALERTED_TTL_H)
    pruned = {}
    for k, rec in alerted.items():
        ts = rec if isinstance(rec, str) else rec.get("ts")
        if _parse_ts(ts) > cutoff:
            pruned[k] = rec
    _save(ALERTED_PATH, pruned)


# ── Layer 1: exact-hash dedup ────────────────────────────────────────────────
def already_seen(headline: str) -> bool:
    h = N.headline_hash(headline)
    seen = _load(SEEN_PATH)
    return h in seen


def mark_seen(headline: str) -> None:
    h = N.headline_hash(headline)
    seen = _load(SEEN_PATH)
    seen[h] = _now().isoformat()
    _save(SEEN_PATH, seen)


# ── Layer 2 & 3: cluster assignment ──────────────────────────────────────────
def assign_cluster(headline: str) -> tuple[str, bool]:
    """Return (event_key, is_new_cluster).

    Tries exact event-key match first; if no match, falls back to Jaccard
    similarity against every active cluster. If no candidate ≥ 0.5, opens
    a new cluster keyed by this headline's event_key.
    """
    clusters = _load(CLUSTERS_PATH)
    ek = N.event_key(headline)
    if not ek:
        # Empty event_key (very short headline) — treat as its own bucket
        ek = N.headline_hash(headline)[:16]

    if ek in clusters:
        return ek, False

    # Jaccard fallback (Layer 3) — also accept if a cluster shares ≥ N
    # distinctive content tokens with the new headline. Real-world paraphrases
    # often have low Jaccard but a strong shared "anchor" (iran + sanctions +
    # peace), and we want those merged.
    new_tokens = N.token_set(headline)
    best_key, best_jac, best_shared = None, 0.0, 0
    for k, c in clusters.items():
        c_tokens = set(c.get("tokens", []))
        shared = len(new_tokens & c_tokens)
        j = N.jaccard(new_tokens, c_tokens)
        if (j > best_jac) or (shared > best_shared):
            best_jac, best_shared, best_key = j, shared, k
    if best_key and (best_jac >= JACCARD_THRESHOLD or best_shared >= MIN_SHARED_TOKENS):
        return best_key, False

    return ek, True


def upsert_cluster(event_key: str, item: dict) -> dict:
    """Insert `item` into cluster `event_key`, creating it if absent. Returns the cluster."""
    clusters = _load(CLUSTERS_PATH)
    now = _now().isoformat()

    if event_key not in clusters:
        clusters[event_key] = {
            "event_key":  event_key,
            "first_seen": now,
            "last_seen":  now,
            "headlines": [],
            "tokens":    [],
        }
    c = clusters[event_key]

    # Don't double-add same exact headline
    h_hash = N.headline_hash(item["headline"])
    if any(N.headline_hash(h["headline"]) == h_hash for h in c["headlines"]):
        c["last_seen"] = now
    else:
        c["headlines"].append(item)
        c["tokens"] = sorted(set(c["tokens"]) | N.token_set(item["headline"]))
        c["last_seen"] = now

    clusters[event_key] = c
    _save(CLUSTERS_PATH, clusters)
    return c


def active_clusters() -> dict:
    """Return all clusters whose last_seen is within CLUSTER_TTL_MIN."""
    prune_state()
    return _load(CLUSTERS_PATH)


# ── Layer 4: alerted-event suppression ───────────────────────────────────────
# Stored as { event_key: {ts, direction} } where direction is +1 / -1 / 0.
# A re-alert is allowed if the direction has flipped (e.g. previously alerted
# bullish on Iran/peace, now an Iran/strike escalation arrives — that's a
# fresh, opposite-sign event and must fire).
def already_alerted(event_key: str, direction: int = 0) -> bool:
    a = _load(ALERTED_PATH)
    rec = a.get(event_key)
    if rec is None:
        return False
    # Backward-compat: old records were just an ISO timestamp string.
    if isinstance(rec, str):
        return True
    prev_dir = int(rec.get("direction", 0))
    if direction != 0 and prev_dir != 0 and prev_dir != direction:
        return False  # direction has flipped — allow re-alert
    return True


def mark_alerted(event_key: str, direction: int = 0) -> None:
    a = _load(ALERTED_PATH)
    a[event_key] = {"ts": _now().isoformat(), "direction": int(direction)}
    _save(ALERTED_PATH, a)
