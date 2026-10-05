"""Identity Resolution engine: blocking -> scoring -> clustering -> persistent golden ids.

Incremental by design: pairs where *neither* record changed since the last run
reuse the previous decision, so daily cost is proportional to the change volume,
not the size of the master. Steward decisions (from the review queue) override
the model and are never re-litigated by later runs.
"""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd

from mdm.matching.blocking import candidate_pairs
from mdm.matching.clustering import ConstrainedUnionFind
from mdm.matching.comparators import NicknameIndex
from mdm.matching.golden_ids import assign_golden_ids
from mdm.matching.scorer import MATCH, NO_MATCH, REVIEW, PairScorer
from mdm.observability.logging import get_logger
from mdm.observability.metrics import Metrics

log = get_logger("matching")

MATCH_COLUMNS = ["first_name", "last_name", "name_suffix", "email", "phone", "date_of_birth", "address_line1",
                 "postal_code", "source_system", "record_key"]


def load_steward_decisions(path: Path) -> dict[tuple, str]:
    """CSV: record_key_a,record_key_b,decision(MATCH|NO_MATCH),steward,reason"""
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as fh:
        return {tuple(sorted((r["record_key_a"], r["record_key_b"]))): r["decision"].upper() for r in csv.DictReader(fh)}


class IdentityResolutionEngine:
    def __init__(self, match_cfg: dict, ref, metrics: Metrics | None = None, steward_file: Path | None = None):
        self.cfg = match_cfg
        self.nick = NicknameIndex(ref.tables.get("nicknames", []))
        self.scorer = PairScorer(match_cfg, self.nick)
        self.m = metrics
        self.steward = load_steward_decisions(steward_file) if steward_file else {}

    def run(self, silver: pd.DataFrame, prev_xref: pd.DataFrame, prev_pairs: pd.DataFrame,
            changed_keys: set[str], run_id: str):
        eligible = silver[silver["_dq_status"] != "QUARANTINED"]
        records = {r["record_key"]: {c: (None if (isinstance(r.get(c), float) and r.get(c) != r.get(c)) else r.get(c))
                                     for c in MATCH_COLUMNS}
                   for r in eligible.to_dict("records")}
        pairs, bstats = candidate_pairs(list(records.values()), self.cfg["blocking"], self.nick)

        prev = {}
        if not prev_pairs.empty:
            for r in prev_pairs.to_dict("records"):
                prev[(r["record_key_a"], r["record_key_b"])] = r

        results, reused = [], 0
        for (a, b), passes in pairs.items():
            key = (a, b)
            if key in self.steward:
                res = self.scorer.score(records[a], records[b], passes)
                res.decision, res.method = self.steward[key], "steward_override"
                results.append(res.__dict__)
                continue
            if (a not in changed_keys and b not in changed_keys and key in prev
                    and not str(prev[key]["method"]).startswith(("cluster_guard", "steward"))):
                results.append({**prev[key], "a": a, "b": b, "reused": True})
                reused += 1
                continue
            results.append(self.scorer.score(records[a], records[b], passes).__dict__)

        # clustering ------------------------------------------------------
        def conflict(x: str, y: str) -> bool:
            if tuple(sorted((x, y))) in self.steward and self.steward[tuple(sorted((x, y)))] == NO_MATCH:
                return True
            return bool(self.scorer._vetoes(records[x], records[y]))

        uf = ConstrainedUnionFind(records.keys(), conflict, int(self.cfg["clustering"]["max_cluster_size"]))
        for r in sorted((r for r in results if r["decision"] == MATCH), key=lambda r: -r["probability"]):
            ok, why = uf.union(r["a"], r["b"])
            if not ok:
                r["decision"], r["method"] = REVIEW, f"cluster_guard:{why}"
                log.info("union refused - sent to review", a=r["a"], b=r["b"], reason=why)
        clusters = uf.clusters()

        prev_map = dict(zip(prev_xref["record_key"], prev_xref["golden_id"])) if not prev_xref.empty else {}
        assignment, events = assign_golden_ids(clusters, prev_map, run_id)

        pairs_df = pd.DataFrame([{
            "record_key_a": r["a"], "record_key_b": r["b"], "probability": r["probability"], "weight": r["weight"],
            "decision": r["decision"], "method": r["method"],
            "blocking_passes": json.dumps(r["blocking_passes"]) if not isinstance(r["blocking_passes"], str) else r["blocking_passes"],
            "evidence": json.dumps(r["evidence"]) if not isinstance(r["evidence"], str) else r["evidence"],
            "vetoes": json.dumps(r["vetoes"]) if not isinstance(r["vetoes"], str) else r["vetoes"],
            "scored_run_id": r.get("scored_run_id", run_id) if r.get("reused") else run_id,
        } for r in results])
        xref = pd.DataFrame([{"record_key": k, "golden_id": g, "source_system": k.split(":", 1)[0],
                              "source_record_id": k.split(":", 1)[1], "run_id": run_id} for k, g in assignment.items()])
        events_df = pd.DataFrame(events)
        if not events_df.empty:
            events_df["record_keys"] = events_df["record_keys"].map(json.dumps)
        review = pairs_df[pairs_df["decision"] == REVIEW].copy() if not pairs_df.empty else pd.DataFrame()

        if self.m:
            dec = pairs_df["decision"].value_counts().to_dict() if not pairs_df.empty else {}
            self.m.set("match.eligible_records", len(records))
            self.m.set("match.blocks", bstats["blocks"])
            self.m.set("match.oversized_blocks", bstats["oversized_blocks"])
            self.m.set("match.candidate_pairs", bstats["pairs"])
            self.m.set("match.pairs_reused", reused)
            self.m.set("match.auto_match_pairs", dec.get(MATCH, 0))
            self.m.set("match.review_pairs", dec.get(REVIEW, 0))
            self.m.set("match.non_match_pairs", dec.get(NO_MATCH, 0))
            self.m.set("match.clusters", len(clusters))
            self.m.set("match.multi_record_clusters", sum(1 for c in clusters if len(c) > 1))
            n = len(records)
            self.m.set("match.reduction_ratio", round(1 - bstats["pairs"] / (n * (n - 1) / 2), 6) if n > 1 else 0)
            for ev in ("CREATE", "UPDATE", "MERGE", "SPLIT", "RETIRE"):
                self.m.set(f"golden.events.{ev.lower()}", sum(1 for e in events if e["event"] == ev))
        log.info("identity resolution complete", records=len(records), pairs=len(pairs_df), clusters=len(clusters),
                 reused=reused)
        return pairs_df, xref, events_df, review
