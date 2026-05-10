"""Cluster-level + global aggregation of scored headlines.

A "cluster" is a group of headlines about the same event (assigned by
news.dedup). N corroborating headlines boost confidence up to 1.5×.
"""
from __future__ import annotations

import statistics


def aggregate_cluster(headlines: list[dict]) -> dict:
    """Aggregate the headlines belonging to one cluster into one score dict.

    cluster_score      = score with the largest |.| (preserves sign of dominant)
    cluster_confidence = mean(confidence) × min(1.5, 1 + 0.15 * (N-1))
    """
    if not headlines:
        return {"score": 0.0, "confidence": 0.0, "n": 0,
                "direction": 0, "event_class": None, "top": None}

    # Pick the headline with the largest |score| as the cluster anchor
    anchor = max(headlines, key=lambda h: abs(float(h.get("score", 0.0))))
    # Only scored items contribute to base confidence
    scored = [h for h in headlines if h.get("event_class")]
    confs  = [float(h.get("confidence", 0.0)) for h in scored]
    base   = statistics.fmean(confs) if confs else 0.0
    # But corroboration count includes ALL items in the cluster (more reports = stronger)
    n      = len(headlines)
    boost  = min(1.5, 1.0 + 0.15 * (n - 1))

    return {
        "score":       round(float(anchor.get("score", 0.0)), 4),
        "confidence":  round(min(1.0, base * boost), 4),
        "n":           n,
        "direction":   int(anchor.get("direction", 0)),
        "event_class": anchor.get("event_class"),
        "top":         anchor,
        "all":         headlines,
    }


def aggregate_global(clusters: list[dict]) -> dict:
    """Combine all active cluster aggregates into a single signal.

    Active clusters = those with non-zero score (event_class is not None).
    Final score is a confidence-weighted mean.
    """
    active = [c for c in clusters if c.get("event_class") and abs(c.get("score", 0)) > 0]
    if not active:
        return {
            "score":      0.0,
            "confidence": 0.0,
            "n_clusters": 0,
            "top_cluster": None,
            "clusters":   [],
        }

    num = sum(c["score"] * c["confidence"] for c in active)
    den = sum(c["confidence"]              for c in active)
    agg_score = (num / den) if den > 0 else 0.0

    # Confidence: anchor on the strongest cluster (one big event = full conf),
    # then nudge upward when multiple corroborating clusters are active.
    top_conf  = max(c["confidence"] for c in active)
    multi_lift = min(1.2, 1.0 + 0.10 * (len(active) - 1))
    agg_conf  = min(1.0, top_conf * multi_lift)

    top = max(active, key=lambda c: abs(c["score"]) * c["confidence"])

    return {
        "score":       round(agg_score, 4),
        "confidence":  round(min(1.0, agg_conf), 4),
        "n_clusters":  len(active),
        "top_cluster": top,
        "clusters":    active,
    }
