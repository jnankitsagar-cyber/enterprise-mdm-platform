"""Stage-by-stage reconciliation.

Every record that enters the platform must be accounted for at every stage:

    source (control total)  = bronze + ingest dead-letter                      [R1]
    bronze                  = standardized + standardization dead-letter        [R2]
    standardized            = unchanged re-sends + DQ-checked                   [R3]
    DQ-checked              = valid + warning + quarantined                     [R3b]
    eligible silver records = cross-referenced records (each exactly once)      [R4]
    golden records          = distinct golden ids in xref, none orphaned        [R5]
    golden(t) - golden(t-1) = creates + splits - merges - retires               [R6]

A break is an *exception* with severity, the numbers, and an investigation
playbook (where to look first). Errors fail the run's health status and fire
the `reconciliation_break` alert.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from mdm.observability.logging import get_logger
from mdm.observability.metrics import Metrics

log = get_logger("reconciliation")

PLAYBOOK = {
    "R1": "Records lost between source and bronze. 1) Compare the .ctl control total with the file line count "
          "(truncated transfer?). 2) Check ingest dead-letter (lakehouse/_dead_letter/<run>.jsonl, stage=ingest) "
          "for MALFORMED_ROW / MISSING_REQUIRED_FIELD. 3) For DB/API sources confirm the watermark predicate "
          "used for COUNT(*) equals the extract predicate.",
    "R2": "Records lost during standardization. Inspect dead-letter stage=standardize; a mapping that references "
          "a renamed source field (schema drift) is the usual cause - check ingestion_state.json schema_registry.",
    "R3": "Standardized rows do not add up to DQ outcomes. Look for duplicate source keys in the batch (DQ050) "
          "and for rows filtered as unchanged re-sends (same _record_hash).",
    "R4": "Valid records missing from (or duplicated in) the cross-reference. Query silver.party left-anti-join "
          "gold.xref on record_key; check matching logs for oversized/skipped blocks.",
    "R5": "Golden records without members (orphans) or xref pointing to unknown golden ids. Check golden_events "
          "for MERGE/RETIRE in this run and verify golden_redirects.",
    "R6": "Golden count movement not explained by lifecycle events. Diff golden_record versions "
          "(_versions/<prev_run> vs current) and compare with golden_events.",
    "Q1": "High quarantine rate for one source usually means an upstream format change. Group dq_results by "
          "rule_id for that source; compare with the previous run's scorecard.",
}


class ReconciliationEngine:
    def __init__(self, metrics: Metrics, quarantine_warn_ratio: float = 0.15):
        self.m = metrics
        self.q_ratio = quarantine_warn_ratio
        self.exceptions: list[dict] = []
        self.rows: list[dict] = []

    def _check(self, cid: str, desc: str, source: str, expected: float, actual: float, severity="error"):
        ok = expected == actual
        row = {"check": cid, "description": desc, "source_system": source, "expected": expected, "actual": actual,
               "difference": actual - expected, "status": "PASS" if ok else "BREAK"}
        self.rows.append(row)
        if not ok:
            self.exceptions.append({**row, "severity": severity, "investigation": PLAYBOOK[cid]})
            log.warning("reconciliation break", check=cid, source=source, expected=expected, actual=actual)

    def run(self, sources: list[str], stage_counts: dict[str, dict], silver: pd.DataFrame, xref: pd.DataFrame,
            golden: pd.DataFrame, events: pd.DataFrame, prev_golden_count: int, run_id: str) -> dict:
        per_source = []
        for s in sources:
            c = stage_counts.get(s, {})
            per_source.append({"source_system": s, **c})
            if not c:
                continue
            self._check("R1", "source control total = bronze + ingest DLQ", s, c["source_count"], c["bronze"] + c["ingest_dlq"])
            self._check("R2", "bronze = standardized + standardize DLQ", s, c["bronze"], c["standardized"] + c["std_dlq"])
            self._check("R3", "standardized = unchanged + DQ-checked", s, c["standardized"], c["unchanged"] + c["dq_checked"])
            self._check("R3", "DQ-checked = valid + warning + quarantined", s, c["dq_checked"], c["valid"] + c["warning"] + c["quarantined"])
            if c["dq_checked"] and c["quarantined"] / c["dq_checked"] > self.q_ratio:
                self.exceptions.append({"check": "Q1", "description": "quarantine rate above threshold", "source_system": s,
                                        "expected": f"<= {self.q_ratio:.0%}", "actual": f"{c['quarantined'] / c['dq_checked']:.1%}",
                                        "difference": None, "status": "WARN", "severity": "warning", "investigation": PLAYBOOK["Q1"]})

        eligible = silver[silver["_dq_status"] != "QUARANTINED"] if not silver.empty else silver
        elig_keys = set(eligible["record_key"]) if not eligible.empty else set()
        xref_keys = list(xref["record_key"]) if not xref.empty else []
        self._check("R4", "eligible silver records = xref records", "*", len(elig_keys), len(set(xref_keys) & elig_keys))
        self._check("R4", "xref has no duplicate record keys", "*", len(xref_keys), len(set(xref_keys)))
        g_ids = set(golden["golden_id"]) if not golden.empty else set()
        x_ids = set(xref["golden_id"]) if not xref.empty else set()
        self._check("R5", "golden records = distinct golden ids in xref", "*", len(x_ids), len(g_ids))
        self._check("R5", "orphan golden records (no members)", "*", 0, len(g_ids - x_ids))

        ev = events["event"].value_counts().to_dict() if not events.empty else {}
        expected_delta = ev.get("CREATE", 0) + ev.get("SPLIT", 0) - ev.get("MERGE", 0) - ev.get("RETIRE", 0)
        self._check("R6", "golden delta explained by lifecycle events", "*", expected_delta, len(g_ids) - prev_golden_count)

        funnel = {
            "source_records_this_run": sum(c.get("source_count", 0) for c in stage_counts.values()),
            "bronze_this_run": sum(c.get("bronze", 0) for c in stage_counts.values()),
            "standardized_this_run": sum(c.get("standardized", 0) for c in stage_counts.values()),
            "silver_current": len(silver),
            "valid_eligible": len(elig_keys),
            "quarantined_current": len(silver) - len(elig_keys),
            "golden_records": len(g_ids),
            "dedup_ratio": round(1 - len(g_ids) / len(elig_keys), 4) if elig_keys else 0,
        }
        n_err = sum(1 for e in self.exceptions if e["severity"] == "error")
        self.m.set("recon.checks", len(self.rows))
        self.m.set("recon.exceptions_error", n_err)
        self.m.set("recon.exceptions_warning", len(self.exceptions) - n_err)
        return {"run_id": run_id, "status": "BALANCED" if n_err == 0 else "BREAK", "funnel": funnel,
                "per_source": per_source, "checks": self.rows, "exceptions": self.exceptions}

    @staticmethod
    def write(report: dict, out_dir: Path) -> Path:
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / f"reconciliation_{report['run_id']}.json").write_text(json.dumps(report, indent=2, default=str))
        md = [f"# Reconciliation report - run `{report['run_id']}`", "", f"**Status: {report['status']}**", "",
              "## Funnel", "", "| stage | count |", "|---|---|"]
        md += [f"| {k} | {v} |" for k, v in report["funnel"].items()]
        md += ["", "## Per source (this run)", ""]
        if report["per_source"]:
            cols = list(report["per_source"][0].keys())
            md += ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
            md += ["| " + " | ".join(str(r.get(c, "")) for c in cols) + " |" for r in report["per_source"]]
        md += ["", "## Checks", "", "| check | description | source | expected | actual | status |", "|---|---|---|---|---|---|"]
        md += [f"| {c['check']} | {c['description']} | {c['source_system']} | {c['expected']} | {c['actual']} | {c['status']} |" for c in report["checks"]]
        md += ["", "## Exceptions", ""]
        if not report["exceptions"]:
            md.append("None.")
        for e in report["exceptions"]:
            md += [f"### {e['check']} [{e['severity']}] {e['description']} ({e['source_system']})",
                   f"expected `{e['expected']}`, actual `{e['actual']}`", "", f"**Investigate:** {e['investigation']}", ""]
        p = out_dir / f"reconciliation_{report['run_id']}.md"
        p.write_text("\n".join(md))
        return p
