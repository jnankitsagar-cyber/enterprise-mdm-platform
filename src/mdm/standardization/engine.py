"""Silver standardization: source schema -> canonical party model.

For every bronze record:
  1. map source fields to canonical attributes (data-contract `mapping`, dotted paths for JSON)
  2. run `derive` steps (record-level, e.g. split full name)
  3. run each attribute's function chain from config
  4. run `post_derive` steps
  5. capture EVERY change (attr, function, before, after) -> `_transformations`
     and every failure -> `_std_errors`  (field-level lineage)

Silver is maintained as a *current-state* table keyed by
(source_system, source_record_id): a newer version of a source record replaces
the old one (SCD1 at source level); unchanged re-sends are detected by
`_record_hash` and skipped. History is preserved in bronze.
"""
from __future__ import annotations

import json

import pandas as pd

from mdm.config import PlatformConfig
from mdm.observability.logging import get_logger
from mdm.observability.metrics import Metrics
from mdm.standardization.functions import DERIVE_REGISTRY, REGISTRY, StandardizationError, StdContext

log = get_logger("standardization")

CANONICAL_ATTRIBUTES = [
    "first_name", "last_name", "full_name", "name_suffix", "email", "phone", "date_of_birth",
    "address_line1", "city", "state_code", "postal_code", "country_code", "status", "source_updated_at",
]


def _get_path(obj: dict, path: str):
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


class StandardizationEngine:
    def __init__(self, cfg: PlatformConfig, metrics: Metrics | None = None):
        self.cfg = cfg
        self.settings = cfg.standardization
        self.m = metrics
        self._validate_config()

    def _validate_config(self) -> None:
        """Fail fast at startup if config references an unknown function (no runtime surprises)."""
        for attr, steps in self.settings.get("attributes", {}).items():
            for step in steps:
                name = step if isinstance(step, str) else step["fn"]
                if name not in REGISTRY:
                    raise ValueError(f"standardization.yaml: unknown function '{name}' for '{attr}'")
        for name in self.settings.get("derive", []) + self.settings.get("post_derive", []):
            if name not in DERIVE_REGISTRY:
                raise ValueError(f"standardization.yaml: unknown derive step '{name}'")

    # ------------------------------------------------------------------
    def standardize_record(self, source_name: str, payload: dict) -> tuple[dict, list, list]:
        src = self.cfg.source(source_name)
        raw = {attr: _get_path(payload, path) for attr, path in src.mapping.items()}
        rec = {a: raw.get(a) for a in CANONICAL_ATTRIBUTES}
        rec["source_record_id"] = raw.get("source_record_id")
        transformations, errors = [], []
        base_ctx = dict(record=rec, ref=self.cfg.reference, settings=self.settings, date_formats=src.date_formats)

        for name in self.settings.get("derive", []):
            for attr, before, after in DERIVE_REGISTRY[name](rec, StdContext(**base_ctx)):
                transformations.append({"attr": attr, "fn": name, "before": before, "after": after})

        for attr, steps in self.settings.get("attributes", {}).items():
            for step in steps:
                name, args = (step, {}) if isinstance(step, str) else (step["fn"], step.get("args", {}))
                before = rec.get(attr)
                try:
                    after = REGISTRY[name](before, StdContext(**base_ctx, args=args))
                except StandardizationError as e:
                    errors.append({"attr": attr, "fn": name, "value": before, "error": str(e)})
                    after = None
                if after != before:
                    transformations.append({"attr": attr, "fn": name, "before": before, "after": after})
                    rec[attr] = after
                if after is None and before is not None and errors and errors[-1]["attr"] == attr:
                    break                                   # stop the chain for this attribute

        for name in self.settings.get("post_derive", []):
            for attr, before, after in DERIVE_REGISTRY[name](rec, StdContext(**base_ctx)):
                transformations.append({"attr": attr, "fn": name, "before": before, "after": after})
        return rec, transformations, errors

    # ------------------------------------------------------------------
    def run(self, bronze: dict[str, pd.DataFrame], previous_silver: pd.DataFrame, run_id: str, dlq) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Returns (silver_current, changed_rows_this_run)."""
        new_rows = []
        for source, df in bronze.items():
            n_ok = n_fail = 0
            for row in df.to_dict("records"):
                try:
                    payload = json.loads(row["_payload"])
                    rec, tf, errs = self.standardize_record(source, payload)
                except Exception as e:  # unmappable record -> DLQ, never silently dropped
                    dlq.put("standardize", source, "STANDARDIZATION_FAILURE", row["_payload"], str(e))
                    n_fail += 1
                    continue
                rec.update({
                    "source_system": source,
                    "source_record_id": row["_source_record_id"],
                    "source_trust": self.cfg.source(source).trust_score,
                    "record_key": f"{source}:{row['_source_record_id']}",
                    "_record_hash": row["_record_hash"],
                    "_ingest_run_id": run_id,
                    "_source_file": row["_source_file"],
                    "_source_position": row["_source_position"],
                    "_transformations": json.dumps(tf, default=str),
                    "_std_errors": json.dumps(errs, default=str),
                    "_raw": json.dumps({a: _get_path(payload, p) for a, p in self.cfg.source(source).mapping.items()}, default=str),
                })
                new_rows.append(rec)
                n_ok += 1
            if self.m:
                self.m.set(f"standardize.{source}.standardized", n_ok)
                self.m.set(f"standardize.{source}.failed", n_fail)
            log.info("standardized", source=source, ok=n_ok, failed=n_fail)

        changed = pd.DataFrame(new_rows)
        if self.m and not changed.empty:
            self.m.set("standardize.transformations", int(changed["_transformations"].map(lambda s: len(json.loads(s))).sum()))
            self.m.set("standardize.field_errors", int(changed["_std_errors"].map(lambda s: len(json.loads(s))).sum()))
        silver = self._merge_current(previous_silver, changed)
        return silver, changed

    @staticmethod
    def _merge_current(previous: pd.DataFrame, changed: pd.DataFrame) -> pd.DataFrame:
        """Upsert by record_key; latest source_updated_at wins (out-of-order safe)."""
        if changed.empty:
            return previous
        frames = [f for f in (previous, changed) if not f.empty]
        allrows = pd.concat(frames, ignore_index=True)
        allrows["_order"] = range(len(allrows))
        allrows = allrows.sort_values(["record_key", "source_updated_at", "_order"], na_position="first")
        current = allrows.drop_duplicates("record_key", keep="last").drop(columns="_order")
        return current.reset_index(drop=True)
