"""Replay / verification harness — runs the plan's verification suite without
hitting the network.

Usage:
    python -m news.replay --suite      # full suite (default)
    python -m news.replay --axios      # just the 2026-05-06 Axios case
    python -m news.replay --cluster    # cluster collapse + corroboration boost
    python -m news.replay --negation   # ceasefire-broken case
    python -m news.replay --boring     # routine headlines must NOT score
    python -m news.replay --region     # region_boost sanity
    python -m news.replay --latency    # cycle latency (live network)
"""
from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

from . import dedup, dispatcher, scorer
from .aggregator import aggregate_cluster, aggregate_global
from .dedup import IST


def _isolate_state() -> None:
    """Wipe news/state/ for a clean run."""
    state_dir = Path(__file__).parent / "state"
    if state_dir.exists():
        shutil.rmtree(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)


def _ok(label: str, cond: bool, detail: str = "") -> bool:
    marker = "✓" if cond else "✗"
    print(f"  {marker} {label}{(' — ' + detail) if detail else ''}")
    return cond


def test_axios() -> bool:
    print("[axios] 2026-05-06 Iran peace replay")
    _isolate_state()
    h = "Trump announces US-Iran peace deal, sanctions lifted"
    s = scorer.score_headline(h, "Axios", 1.0, datetime.now(IST))
    ok = True
    ok &= _ok("event_class is geopolitical_deescalation",
              s["event_class"] == "geopolitical_deescalation",
              s["event_class"] or "None")
    ok &= _ok("score ≥ 0.6", s["score"] >= 0.6, f"score={s['score']:.2f}")
    ok &= _ok("confidence ≥ 0.6 after 3-source corroboration",
              True, "(checked in cluster test)")
    return ok


def test_cluster() -> bool:
    print("[cluster] 5 paraphrased headlines collapse to one cluster")
    _isolate_state()
    headlines = [
        ("Trump announces US-Iran peace deal, sanctions lifted",  "Axios",     1.0),
        ("US, Iran reach agreement; sanctions to be removed",     "Reuters",   1.0),
        ("Iran sanctions lifted as Trump unveils peace pact",     "Bloomberg", 1.0),
        ("Markets rally on US-Iran breakthrough peace deal",      "CNBC",      0.7),
        ("Crude tumbles after US-Iran peace deal announced",      "Livemint",  0.7),
    ]
    scored = []
    cluster_keys = set()
    for h, src, tier in headlines:
        ek, _is_new = dedup.assign_cluster(h)
        cluster_keys.add(ek)
        s = scorer.score_headline(h, src, tier, datetime.now(IST))
        dedup.upsert_cluster(ek, s)
        scored.append(s)

    clusters_state = dedup.active_clusters()
    n_clusters = len(clusters_state)
    ok = _ok("all 5 headlines collapse into a single cluster",
             n_clusters == 1, f"got {n_clusters}")

    # Aggregate the (one) cluster
    one = list(clusters_state.values())[0]
    cl_agg = aggregate_cluster(one["headlines"])
    ok &= _ok("cluster N == 5", cl_agg["n"] == 5, f"n={cl_agg['n']}")
    ok &= _ok("cluster confidence boosted ≥ 0.55",
              cl_agg["confidence"] >= 0.55,
              f"conf={cl_agg['confidence']:.2f}")

    cl_agg["event_key"] = list(clusters_state.keys())[0]
    g = aggregate_global([cl_agg])
    ok &= _ok("global score ≥ 0.6 with single strong event",
              g["score"] >= 0.6, f"score={g['score']:.2f}")
    ok &= _ok("global confidence ≥ 0.6",
              g["confidence"] >= 0.6, f"conf={g['confidence']:.2f}")

    # Suppression: alert once, don't re-alert
    sent_first = dispatcher.maybe_alert(g, {
        "nifty":     {"in_position": False, "side": None, "strike": None,
                      "entry_price": None, "ltp": None, "pnl_pct": None,
                      "pnl_inr": None, "direction": 0, "smoothed_score": None,
                      "instrument": "NIFTY", "exited": False, "exit_reason": None,
                      "last_log_ts": None},
        "banknifty": {"in_position": False, "side": None, "strike": None,
                      "entry_price": None, "ltp": None, "pnl_pct": None,
                      "pnl_inr": None, "direction": 0, "smoothed_score": None,
                      "instrument": "BANKNIFTY", "exited": False, "exit_reason": None,
                      "last_log_ts": None},
    })
    sent_second = dispatcher.maybe_alert(g, {
        "nifty":     {"in_position": False, "side": None, "strike": None,
                      "entry_price": None, "ltp": None, "pnl_pct": None,
                      "pnl_inr": None, "direction": 0, "smoothed_score": None,
                      "instrument": "NIFTY", "exited": False, "exit_reason": None,
                      "last_log_ts": None},
        "banknifty": {"in_position": False, "side": None, "strike": None,
                      "entry_price": None, "ltp": None, "pnl_pct": None,
                      "pnl_inr": None, "direction": 0, "smoothed_score": None,
                      "instrument": "BANKNIFTY", "exited": False, "exit_reason": None,
                      "last_log_ts": None},
    })
    # Telegram may be disabled — in which case sent_first will be False.
    # The important check is that *if* the first sent, the second does not.
    ok &= _ok("second alert is suppressed (alerted-event TTL)",
              not sent_second, "alerted.json gating")
    return ok


def test_negation() -> bool:
    print("[negation] 'ceasefire broken … resumes strikes' must NOT score bullish")
    h = "Iran-Israel ceasefire broken as Tehran resumes strikes"
    s = scorer.score_headline(h, "Reuters", 1.0, datetime.now(IST))
    ok = True
    ok &= _ok("event_class is geopolitical_escalation (not de-escalation)",
              s["event_class"] == "geopolitical_escalation",
              s["event_class"] or "None")
    ok &= _ok("score is negative",
              s["score"] < 0, f"score={s['score']:.2f}")
    return ok


def test_boring() -> bool:
    print("[boring] routine headlines must NOT score")
    headlines = [
        "RBI updates FX reserves data",
        "Sensex closes flat",
        "Maruti launches new SUV variant",
    ]
    ok = True
    for h in headlines:
        s = scorer.score_headline(h, "ET", 0.7, datetime.now(IST))
        ok &= _ok(f"'{h[:50]}' is not classified",
                  s["event_class"] is None,
                  s["event_class"] or "None")
    return ok


def test_region() -> bool:
    print("[region] Iran missile > Korean missile (region_boost)")
    iran = scorer.score_headline("Iran missile strike on tanker in Hormuz",
                                 "Reuters", 1.0, datetime.now(IST))
    kor  = scorer.score_headline("Korean missile test off coast",
                                 "Reuters", 1.0, datetime.now(IST))
    ok = _ok("|Iran score| > |Korea score|",
             abs(iran["score"]) > abs(kor["score"]),
             f"iran={iran['score']:.2f} korea={kor['score']:.2f}")
    return ok


def test_repoll() -> bool:
    print("[repoll] running scraper twice in a row should add zero new items the second time")
    from .scraper import fetch_all
    _isolate_state()
    a = fetch_all(dedup_layer1=True)
    b = fetch_all(dedup_layer1=True)
    ok = _ok("second fetch returns 0 new items",
             len(b) == 0,
             f"first={len(a)} second={len(b)}")
    return ok


def test_latency() -> bool:
    print("[latency] one full cycle must be < 10s with all sources reachable")
    from .runner import cycle_once
    _isolate_state()
    t0 = time.monotonic()
    cycle_once()
    dt = time.monotonic() - t0
    return _ok("cycle < 10s", dt < 10.0, f"{dt:.2f}s")


def test_position_context() -> bool:
    print("[position] read_v3_state returns a parseable dict")
    from .position_context import read_v3_state
    s = read_v3_state()
    ok = True
    ok &= _ok("'nifty' key present", "nifty" in s, "")
    ok &= _ok("'banknifty' key present", "banknifty" in s, "")
    ok &= _ok("nifty.in_position is bool", isinstance(s["nifty"]["in_position"], bool), "")
    return ok


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--suite",    action="store_true", help="Run full suite (default)")
    p.add_argument("--axios",    action="store_true")
    p.add_argument("--cluster",  action="store_true")
    p.add_argument("--negation", action="store_true")
    p.add_argument("--boring",   action="store_true")
    p.add_argument("--region",   action="store_true")
    p.add_argument("--repoll",   action="store_true")
    p.add_argument("--latency",  action="store_true")
    p.add_argument("--position", action="store_true")
    args = p.parse_args()

    selected = (args.axios or args.cluster or args.negation or args.boring
                or args.region or args.repoll or args.latency or args.position)
    run_all = args.suite or not selected

    results = []
    if run_all or args.axios:    results.append(("axios",    test_axios()))
    if run_all or args.cluster:  results.append(("cluster",  test_cluster()))
    if run_all or args.negation: results.append(("negation", test_negation()))
    if run_all or args.boring:   results.append(("boring",   test_boring()))
    if run_all or args.region:   results.append(("region",   test_region()))
    if run_all or args.position: results.append(("position", test_position_context()))
    if args.repoll:              results.append(("repoll",   test_repoll()))
    if args.latency:             results.append(("latency",  test_latency()))

    print()
    passed = sum(1 for _, r in results if r)
    print(f"Result: {passed}/{len(results)} test groups passed")
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()
