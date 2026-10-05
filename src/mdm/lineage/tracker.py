"""Lineage & traceability.

Two complementary levels:

1. **Dataset-level lineage** (which job read which table and wrote which
   table) emitted as OpenLineage run events -> lakehouse/_lineage/*.jsonl.
   In Azure these are picked up natively by Unity Catalog lineage and can be
   pushed to Microsoft Purview, so the catalog shows bronze -> silver -> gold.

2. **Record-level lineage** (why does golden record X look like this?)
   reconstructed by `build_trace(golden_id)` by joining:
      xref                -> which source records belong to X
      silver._raw / bronze -> original values, file and position
      silver._transformations / _std_errors -> every standardization step
      silver._dq_failures -> validation failures
      match_pairs         -> merge decisions with per-attribute evidence
      survivorship_log    -> which record/rule won each attribute
      golden_events       -> CREATE / MERGE / SPLIT history
      golden_history      -> previous versions (SCD2)
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from mdm.storage.lake import LakeStore


class LineageTracker:
    def __init__(self, lake_root: Path, run_id: str, namespace: str = "mdm"):
        self.dir = Path(lake_root) / "_lineage"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.run_id, self.namespace = run_id, namespace
        self.events = []

    def emit(self, job: str, inputs: list[str], outputs: list[str], facets: dict | None = None) -> None:
        self.events.append({
            "eventType": "COMPLETE",
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {"runId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{self.run_id}/{job}")),
                    "facets": {"mdm_run_id": self.run_id, **(facets or {})}},
            "job": {"namespace": self.namespace, "name": job},
            "inputs": [{"namespace": self.namespace, "name": i} for i in inputs],
            "outputs": [{"namespace": self.namespace, "name": o} for o in outputs],
            "producer": "https://github.com/your-org/enterprise-mdm-platform",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent",
        })

    def flush(self) -> None:
        with open(self.dir / f"openlineage_{self.run_id}.jsonl", "w", encoding="utf-8") as fh:
            for e in self.events:
                fh.write(json.dumps(e) + "\n")


def _j(v):
    if isinstance(v, str) and v[:1] in "[{":
        try:
            return json.loads(v)
        except json.JSONDecodeError:
            return v
    if isinstance(v, float) and v != v:
        return None
    return v


def _rows(df: pd.DataFrame) -> list[dict]:
    return [{k: _j(v) for k, v in r.items()} for r in df.to_dict("records")]


def build_trace(lake: LakeStore, golden_id: str) -> dict | None:
    redirects = lake.read("gold", "golden_redirects")
    if not redirects.empty and golden_id in set(redirects["old_golden_id"]):
        target = redirects.loc[redirects["old_golden_id"] == golden_id, "new_golden_id"].iloc[0]
        t = build_trace(lake, target)
        if t:
            t["redirected_from"] = golden_id
        return t
    golden = lake.read("gold", "golden_record")
    if golden.empty or golden_id not in set(golden["golden_id"]):
        return None
    xref = lake.read("gold", "xref")
    keys = set(xref.loc[xref["golden_id"] == golden_id, "record_key"])
    silver = lake.read("silver", "party")
    members = silver[silver["record_key"].isin(keys)]
    pairs = lake.read("gold", "match_pairs")
    surv = lake.read("gold", "survivorship_log")
    events = lake.read("gold", "golden_events")
    hist = lake.read("gold", "golden_record_history")

    source_records = []
    for r in _rows(members):
        source_records.append({
            "record_key": r["record_key"],
            "source_system": r["source_system"],
            "origin": {"file": r.get("_source_file"), "position": r.get("_source_position"),
                       "ingest_run_id": r.get("_ingest_run_id"), "record_hash": r.get("_record_hash")},
            "raw_values": r.get("_raw"),
            "standardized_values": {k: r.get(k) for k in ("first_name", "last_name", "email", "phone", "date_of_birth",
                                                         "address_line1", "city", "state_code", "postal_code", "country_code", "status")},
            "transformations": r.get("_transformations"),
            "standardization_errors": r.get("_std_errors"),
            "dq_status": r.get("_dq_status"),
            "dq_failures": r.get("_dq_failures"),
        })
    rel_pairs = pairs[pairs["record_key_a"].isin(keys) | pairs["record_key_b"].isin(keys)] if not pairs.empty else pairs
    return {
        "golden_record": _rows(golden[golden["golden_id"] == golden_id])[0],
        "source_records": source_records,
        "merge_decisions": _rows(rel_pairs.sort_values("probability", ascending=False)) if not rel_pairs.empty else [],
        "survivorship_decisions": _rows(surv[surv["golden_id"] == golden_id]) if not surv.empty else [],
        "lifecycle_events": _rows(events[(events["golden_id"] == golden_id) | (events["related_golden_id"] == golden_id)]) if not events.empty else [],
        "version_history": _rows(hist[hist["golden_id"] == golden_id][["golden_id", "version", "valid_from_run", "record_hash"]]) if not hist.empty else [],
    }
