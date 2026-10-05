"""Persistent Golden IDs - the core MDM guarantee that IDs are STABLE across runs.

Downstream systems store golden_id, so re-running matching must not reshuffle
IDs. Each new clustering is reconciled with the previous cross-reference:

  * cluster with no previously-known records          -> CREATE new golden_id
  * cluster containing records of exactly one old id  -> keep id (UPDATE)
  * cluster containing records of several old ids     -> MERGE: survivor keeps id
    (the one with most records, ties -> lexicographically smallest = oldest),
    others are retired with a pointer to the survivor (redirect for consumers)
  * an old id whose records are now split across clusters -> largest piece keeps
    the id, other pieces get new ids (SPLIT event)
"""
from __future__ import annotations

import hashlib
from collections import Counter


def new_golden_id(member_keys: set[str]) -> str:
    return "GR-" + hashlib.sha1(min(member_keys).encode()).hexdigest()[:10].upper()


def assign_golden_ids(clusters: list[set[str]], prev_xref: dict[str, str], run_id: str):
    """Returns (record_key -> golden_id, events)."""
    events, assignment, claimed = [], {}, set()
    cluster_candidates, claimed_by_merge = [], set()
    prev_ids_all = set(prev_xref.values())
    # big clusters first so they claim their historical id
    for cluster in sorted(clusters, key=lambda c: (-len(c), min(c))):
        counts = Counter(prev_xref[k] for k in cluster if k in prev_xref)
        candidates = sorted(counts, key=lambda g: (-counts[g], g))
        gid = next((g for g in candidates if g not in claimed), None)
        if gid is None:
            gid = new_golden_id(cluster)
            while gid in claimed or gid in prev_ids_all:
                gid = "GR-" + hashlib.sha1((gid + run_id).encode()).hexdigest()[:10].upper()
            if candidates:
                events.append({"run_id": run_id, "event": "SPLIT", "golden_id": gid, "related_golden_id": candidates[0],
                               "record_keys": sorted(cluster), "reason": "records split from existing golden record"})
            else:
                events.append({"run_id": run_id, "event": "CREATE", "golden_id": gid, "related_golden_id": None,
                               "record_keys": sorted(cluster), "reason": "new entity"})
        else:
            new_members = sorted(k for k in cluster if prev_xref.get(k) != gid)
            if new_members:
                events.append({"run_id": run_id, "event": "UPDATE", "golden_id": gid, "related_golden_id": None,
                               "record_keys": new_members, "reason": "records attached to existing golden record"})
        claimed.add(gid)
        cluster_candidates.append((gid, cluster, candidates))
        for k in cluster:
            assignment[k] = gid
    # second pass: an old id that no cluster claimed was merged into its survivor
    for gid, cluster, candidates in cluster_candidates:
        for g in candidates:
            if g != gid and g not in claimed and g not in claimed_by_merge:
                events.append({"run_id": run_id, "event": "MERGE", "golden_id": gid, "related_golden_id": g,
                               "record_keys": sorted(k for k in cluster if prev_xref.get(k) == g),
                               "reason": f"{g} merged into {gid}"})
                claimed_by_merge.add(g)
    for g in sorted(prev_ids_all - claimed - claimed_by_merge):
        events.append({"run_id": run_id, "event": "RETIRE", "golden_id": g, "related_golden_id": None,
                       "record_keys": [], "reason": "no eligible source records remain"})
    return assignment, events
