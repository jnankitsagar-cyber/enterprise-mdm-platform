"""Data-quality engine.

Outputs
  * per record:  _dq_status  VALID | WARNING | QUARANTINED
                 _dq_failures (json list of rule hits)   -> lineage
                 _dq_score    1.0 minus a penalty per warning -> survivorship confidence
  * dq_results          one row per rule failure (drill-down, stewardship work queue)
  * dq_dataset_results  dataset-level checks per source batch (completeness %, volume)
  * scorecard           pass-rate per rule / dimension / source -> dashboard

QUARANTINED records stay in silver (nothing is lost) but are excluded from
identity resolution until fixed at source or by a steward.
"""
from __future__ import annotations

import json

import pandas as pd

from mdm.observability.logging import get_logger
from mdm.observability.metrics import Metrics
from mdm.quality.rules import RULE_TYPES, DatasetRule

log = get_logger("quality")

WARNING_PENALTY = 0.1


class DataQualityEngine:
    def __init__(self, dq_cfg: dict, ref, metrics: Metrics | None = None):
        self.rules = []
        for spec in dq_cfg.get("rules", []):
            if spec["type"] not in RULE_TYPES:
                raise ValueError(f"dq_rules.yaml: unknown rule type '{spec['type']}' ({spec['id']})")
            self.rules.append(RULE_TYPES[spec["type"]](spec, ref))
        ids = [r.id for r in self.rules]
        if len(ids) != len(set(ids)):
            raise ValueError("dq_rules.yaml: duplicate rule ids")
        self.m = metrics

    def run(self, df: pd.DataFrame, run_id: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if df.empty:
            return df, pd.DataFrame(), pd.DataFrame()
        records = df.to_dict("records")
        for r in records:
            r["_raw_dict"] = json.loads(r.get("_raw") or "{}")
        record_rules = [r for r in self.rules if not isinstance(r, DatasetRule)]
        dataset_rules = [r for r in self.rules if isinstance(r, DatasetRule)]
        for rule in record_rules:
            rule.prepare(records)

        failures, statuses, scores, fail_json = [], [], [], []
        for rec in records:
            hits = []
            for rule in record_rules:
                if not rule.applies(rec):
                    continue
                try:
                    ok = rule.check(rec)
                except Exception as e:  # a broken rule must not break the pipeline
                    ok = False
                    log.warning("rule evaluation error", rule=rule.id, error=str(e))
                if not ok:
                    col = rule.columns[0] if rule.columns else None
                    hit = {"rule_id": rule.id, "rule": rule.name, "severity": rule.severity,
                           "dimension": rule.dimension, "column": col,
                           "value": rec.get(col) if col else None}
                    hits.append(hit)
                    failures.append({"run_id": run_id, "record_key": rec["record_key"],
                                     "source_system": rec["source_system"], **hit})
            n_err = sum(h["severity"] == "error" for h in hits)
            n_warn = len(hits) - n_err
            statuses.append("QUARANTINED" if n_err else ("WARNING" if n_warn else "VALID"))
            scores.append(round(max(0.5, 1 - WARNING_PENALTY * n_warn), 2) if not n_err else 0.0)
            fail_json.append(json.dumps(hits, default=str))

        out = df.copy()
        out["_dq_status"], out["_dq_score"], out["_dq_failures"] = statuses, scores, fail_json
        out["_dq_run_id"] = run_id

        ds_rows = []
        for source, grp in out.groupby("source_system"):
            recs = [r for r in records if r["source_system"] == source]
            for rule in dataset_rules:
                ok, observed = rule.evaluate(recs)
                ds_rows.append({"run_id": run_id, "source_system": source, "rule_id": rule.id, "rule": rule.name,
                                "severity": rule.severity, "passed": ok, "observed": observed})
                if not ok:
                    log.warning("dataset rule failed", rule=rule.id, source=source, observed=observed)

        fails_df = pd.DataFrame(failures)
        ds_df = pd.DataFrame(ds_rows)
        if self.m:
            n = len(out)
            valid = int((out["_dq_status"] != "QUARANTINED").sum())
            self.m.set("dq.records_checked", n)
            self.m.set("dq.valid", int((out["_dq_status"] == "VALID").sum()))
            self.m.set("dq.warning", int((out["_dq_status"] == "WARNING").sum()))
            self.m.set("dq.quarantined", n - valid)
            self.m.set("dq.pass_rate", round(valid / n, 4) if n else 1.0)
            self.m.set("dq.rule_failures", len(fails_df))
            for source, grp in out.groupby("source_system"):
                self.m.set(f"dq.{source}.checked", len(grp))
                self.m.set(f"dq.{source}.quarantined", int((grp["_dq_status"] == "QUARANTINED").sum()))
        log.info("data quality evaluated", records=len(out), failures=len(fails_df))
        return out, fails_df, ds_df

    @staticmethod
    def scorecard(dq_results: pd.DataFrame, checked_by_source: dict[str, int]) -> pd.DataFrame:
        if dq_results.empty:
            return pd.DataFrame()
        g = dq_results.groupby(["source_system", "rule_id", "rule", "dimension", "severity"]).size().reset_index(name="failures")
        g["checked"] = g["source_system"].map(checked_by_source)
        g["pass_rate"] = (1 - g["failures"] / g["checked"]).round(4)
        return g.sort_values(["pass_rate", "source_system"])
