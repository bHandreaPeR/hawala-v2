"""Keyword-based headline classifier and per-headline scorer.

Loads `news/keywords.yml` once at import. To force a reload during a long-
running process, call `reload_keywords()`.
"""
from __future__ import annotations

import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from . import normalize as N


_YML_PATH = Path(__file__).parent / "keywords.yml"
_KW: dict = {}
_KW_MTIME: float = 0.0

IST = timezone(timedelta(hours=5, minutes=30))

# Scoring constants — half-life chosen to match the live runner's
# ACTIVE_WINDOW_SEC (default 120s = 2 min). Items older than ~3 min
# decay below 0.35 so they don't dominate the live signal even if
# they remain in cluster history.
RECENCY_HALF_LIFE_MIN = float(os.environ.get("NEWS_DECAY_HALF_LIFE_MIN", "3.0"))


def reload_keywords() -> None:
    global _KW, _KW_MTIME
    _KW = yaml.safe_load(_YML_PATH.read_text()) or {}
    _KW_MTIME = _YML_PATH.stat().st_mtime


def _maybe_reload() -> None:
    try:
        m = _YML_PATH.stat().st_mtime
        if m != _KW_MTIME:
            reload_keywords()
    except FileNotFoundError:
        pass


reload_keywords()


def _phrase_in(phrase: str, normalized: str) -> bool:
    """Substring match — phrases in keywords.yml are pre-lowercased."""
    return phrase in normalized


def _negation_blocks(spec: dict, headline_norm: str) -> bool:
    """True if any negation_blocker token-pair fires within 6 tokens."""
    blockers = spec.get("negation_blockers") or []
    for pair in blockers:
        if len(pair) < 2:
            continue
        a, b = pair[0].lower(), pair[1].lower()
        # Allow 3-token blockers (e.g. [missile, intercepted, no casualties])
        if len(pair) >= 3:
            extra = pair[2].lower()
            if (
                N.near_pair(headline_norm, a, b, window=6)
                and extra in headline_norm
            ):
                return True
        else:
            if N.near_pair(headline_norm, a, b, window=6):
                return True
    return False


def _region_multiplier(spec: dict, headline_norm: str) -> float:
    """Pick the largest matching region_boost; default 1.0."""
    boosts = spec.get("region_boost") or {}
    best = 1.0
    for region, mult in boosts.items():
        if region.lower() in headline_norm:
            best = max(best, float(mult))
    return best


def _match_class(spec: dict, headline_norm: str) -> tuple[bool, int]:
    """Return (matched, n_triggers_matched). Stops at negation_blocker."""
    if _negation_blocks(spec, headline_norm):
        return False, 0
    triggers = spec.get("triggers") or {}
    n = 0
    for _group, phrases in triggers.items():
        for p in phrases:
            if _phrase_in(p.lower(), headline_norm):
                n += 1
    return n > 0, n


def _match_corporate(spec: dict, headline_norm: str) -> dict | None:
    """Special handler for corporate_india_megacap. Returns dict or None."""
    entities = [e.lower() for e in (spec.get("entities") or [])]
    pos = [p.lower() for p in (spec.get("positive_triggers") or [])]
    neg = [p.lower() for p in (spec.get("negative_triggers") or [])]

    if not any(e in headline_norm for e in entities):
        return None

    n_pos = sum(1 for p in pos if p in headline_norm)
    n_neg = sum(1 for p in neg if p in headline_norm)
    if n_pos == 0 and n_neg == 0:
        return None

    direction = +1 if n_pos > n_neg else -1
    n_triggers = max(n_pos, n_neg)
    return {
        "direction":   direction,
        "n_triggers":  n_triggers,
        "magnitude":   float(spec.get("base_magnitude", 0.4)),
    }


def classify(headline: str) -> dict | None:
    """Classify a headline into one event class.

    Returns:
        {
          "event_class":   str,
          "direction":     int (+1/-1),
          "magnitude_eff": float (base_magnitude × region_boost),
          "n_triggers":    int,
        }
        or None if no class matched.

    Tie-breaking rule (action > framing):
      A real-world military/escalation action overrides diplomatic framing in
      the same headline. e.g. "US fires on Iranian oil tanker as Trump
      pressures Tehran for deal to end war" must classify as escalation, not
      de-escalation, even though "deal to end war" matches a de-escalation
      trigger. We track all matches, then apply the rule before picking.
    """
    _maybe_reload()
    headline_norm = N.normalize(headline)
    if not headline_norm:
        return None

    matches: dict[str, dict] = {}

    for cls_name, spec in _KW.items():
        if not isinstance(spec, dict):
            continue

        if cls_name == "corporate_india_megacap":
            r = _match_corporate(spec, headline_norm)
            if r is None:
                continue
            eff = r["magnitude"]
            matches[cls_name] = {
                "event_class":   cls_name,
                "direction":     r["direction"],
                "magnitude_eff": float(eff),
                "n_triggers":    r["n_triggers"],
            }
            continue

        matched, n = _match_class(spec, headline_norm)
        if not matched:
            continue
        base = float(spec.get("base_magnitude", 0.5))
        region_mult = _region_multiplier(spec, headline_norm)
        eff = base * region_mult
        matches[cls_name] = {
            "event_class":   cls_name,
            "direction":     int(spec.get("direction", 0)),
            "magnitude_eff": float(eff),
            "n_triggers":    n,
        }

    if not matches:
        return None

    # Action-beats-framing rule: when both escalation AND de-escalation fire,
    # escalation always wins (the kinetic act is the news).
    if ("geopolitical_escalation" in matches
            and "geopolitical_deescalation" in matches):
        matches.pop("geopolitical_deescalation", None)
    # Same logic for monetary: hawkish action over dovish rhetoric in
    # the same line is rare enough we don't need a hard override yet.

    # Pick the remaining class with the largest effective magnitude.
    best = max(matches.values(), key=lambda p: p["magnitude_eff"])
    return best


def _recency_decay(ts_seen: datetime, now: datetime | None = None) -> float:
    if now is None:
        now = datetime.now(IST)
    if ts_seen.tzinfo is None:
        ts_seen = ts_seen.replace(tzinfo=IST)
    minutes = (now - ts_seen).total_seconds() / 60.0
    if minutes < 0:
        minutes = 0.0
    return math.exp(-minutes / RECENCY_HALF_LIFE_MIN)


def score_headline(headline: str, source: str, tier: float,
                   ts_seen: datetime | None = None) -> dict:
    """Return a full per-headline score dict.

    Always returns a dict; if no class matches, score=0 and event_class=None.
    """
    if ts_seen is None:
        ts_seen = datetime.now(IST)

    cls = classify(headline)
    if cls is None:
        return {
            "headline":     headline,
            "source":       source,
            "tier":         float(tier),
            "ts_seen":      ts_seen.isoformat(),
            "event_class":  None,
            "direction":    0,
            "score":        0.0,
            "confidence":   0.0,
            "n_triggers":   0,
        }

    decay = _recency_decay(ts_seen)
    n = max(1, int(cls["n_triggers"]))
    # Floor at 0.6 so a single clear trigger ("Iran peace deal") doesn't get
    # penalized as if it were ambiguous.
    keyword_density = max(0.6, min(1.0, 0.6 + 0.2 * (n - 1)))

    mag = min(1.0, float(cls["magnitude_eff"]))  # clamp to keep score in [-1, 1]
    score      = cls["direction"] * mag * tier * decay
    confidence = mag * tier * keyword_density

    return {
        "headline":     headline,
        "source":       source,
        "tier":         float(tier),
        "ts_seen":      ts_seen.isoformat(),
        "event_class":  cls["event_class"],
        "direction":    cls["direction"],
        "score":        round(score, 4),
        "confidence":   round(min(1.0, confidence), 4),
        "n_triggers":   n,
    }
