"""Golden Record survivorship engine (attribute-level, rule-chain based).

Why attribute-level (not record-level) survivorship?
  The best *name* usually comes from the KYC'd billing system, the best *email*
  from whichever system was updated last, and the best *DOB* is what most
  systems agree on. Picking one "best record" would throw away good data.

Guard-rails
  * values that failed a DQ rule on that column are not eligible
  * address fields survive as a GROUP from one source record (no Frankenstein addresses)
  * every decision is logged (winner, deciding rule, rejected alternatives)
  * golden records are versioned: a content hash change bumps `version` and
    appends to golden_record_history (SCD Type 2) for point-in-time queries.
"""
from __future__ import annotations

import hashlib
import json

import pandas as pd

from mdm.observability.logging import get_logger
from mdm.observability.metrics import Metrics
from mdm.survivorship.rules import RULES, Candidate, resolve

log = get_logger("survivorship")


def _clean(v):
    if v is None or (isinstance(v, float) and v != v) or v == "":
        return None
    return v


class SurvivorshipEngine:
    def __init__(self, cfg: dict, metrics: Metrics | None = None):
        self.cfg = cfg
        self.priority = {s: i for i, s in enumerate(cfg.get("source_priority", []))}
        self.m = metrics
        for spec in list(cfg.get("attributes", {}).values()) + list(cfg.get("groups", {}).values()):
            for r in spec["rules"]:
                if r not in RULES:
                    raise ValueError(f"survivorship.yaml: unknown rule '{r}'")

    def _candidates(self, members: list[dict], attr: str | None, group: list[str] | None = None) -> list[Candidate]:
        out, clean = [], []
        for r in members:
            failed_cols = {f.get("column") for f in json.loads(r.get("_dq_failures") or "[]")}
            if group:
                value = {a: _clean(r.get(a)) for a in group}
                if all(v is None for v in value.values()):
                    continue
            else:
                value = _clean(r.get(attr))
                if value is None or attr in failed_cols:      # DQ-failed values never survive
                    continue
            conf = float(r.get("source_trust") or 0.5) * float(r.get("_dq_score") or 1.0)
            c = Candidate(r["record_key"], r["source_system"], value, _clean(r.get("source_updated_at")),
                          round(conf, 4), self.priority.get(r["source_system"], 99))
            out.append(c)
            if group and not (set(group) & failed_cols):
                clean.append(c)
        # for groups: prefer candidates whose group passed DQ entirely, if any exist
        return clean if (group and clean) else out

    def run(self, silver: pd.DataFrame, xref: pd.DataFrame, prev_golden: pd.DataFrame, pairs: pd.DataFrame, run_id: str):
        members_df = silver.merge(xref[["record_key", "golden_id"]], on="record_key", how="inner")
        prev = {r["golden_id"]: r for r in prev_golden.to_dict("records")} if not prev_golden.empty else {}

        match_conf = {}
        if not pairs.empty:
            mp = pairs[pairs["decision"].isin(["MATCH"])]
            k2g = dict(zip(xref["record_key"], xref["golden_id"]))
            for r in mp.to_dict("records"):
                g = k2g.get(r["record_key_a"])
                if g and g == k2g.get(r["record_key_b"]):
                    match_conf.setdefault(g, []).append(float(r["probability"]))

        golden_rows, surv_log, history = [], [], []
        for gid, grp in members_df.groupby("golden_id", sort=True):
            members = grp.to_dict("records")
            g = {"golden_id": gid}
            confs = []
            for attr, spec in self.cfg.get("attributes", {}).items():
                win, rule = resolve(self._candidates(members, attr), spec["rules"])
                g[attr] = win.value if win else None
                if win:
                    confs.append(win.confidence)
                surv_log.append(self._log_row(run_id, gid, attr, win, rule, self._candidates(members, attr)))
            for gname, spec in self.cfg.get("groups", {}).items():
                cands = self._candidates(members, None, spec["attributes"])
                win, rule = resolve(cands, spec["rules"])
                for a in spec["attributes"]:
                    g[a] = win.value.get(a) if win else None
                if win:
                    confs.append(win.confidence)
                surv_log.append(self._log_row(run_id, gid, gname, win, rule, cands))
            for d, tmpl in self.cfg.get("derived", {}).items():
                g[d] = " ".join(tmpl.format(**{k: (v or "") for k, v in g.items()}).split()) or None

            g["source_record_count"] = len(members)
            g["source_systems"] = json.dumps(sorted({m["source_system"] for m in members}))
            g["member_record_keys"] = json.dumps(sorted(m["record_key"] for m in members))
            g["attribute_confidence"] = round(sum(confs) / len(confs), 4) if confs else 0.0
            mc = match_conf.get(gid)
            g["match_confidence"] = round(min(mc), 4) if mc else 1.0      # weakest link in the cluster
            content = {k: g.get(k) for k in list(self.cfg.get("attributes", {})) + [a for s in self.cfg.get("groups", {}).values() for a in s["attributes"]]}
            g["record_hash"] = hashlib.sha256(json.dumps(content, sort_keys=True, default=str).encode()).hexdigest()[:20]
            old = prev.get(gid)
            if old is None:
                g["version"], g["created_run_id"], g["updated_run_id"] = 1, run_id, run_id
            elif old["record_hash"] != g["record_hash"]:
                g["version"], g["created_run_id"], g["updated_run_id"] = int(old["version"]) + 1, old["created_run_id"], run_id
            else:
                g["version"], g["created_run_id"], g["updated_run_id"] = int(old["version"]), old["created_run_id"], old["updated_run_id"]
            g["status_mdm"] = "ACTIVE"
            golden_rows.append(g)
            if old is None or old["record_hash"] != g["record_hash"]:
                history.append({**g, "valid_from_run": run_id})

        golden = pd.DataFrame(golden_rows)
        if self.m:
            self.m.set("golden.records", len(golden))
            self.m.set("golden.changed", len(history))
            self.m.set("golden.multi_source", int((golden["source_record_count"] > 1).sum()) if not golden.empty else 0)
        log.info("golden records built", golden=len(golden), changed=len(history))
        return golden, pd.DataFrame(surv_log), pd.DataFrame(history)

    @staticmethod
    def _log_row(run_id, gid, attr, win, rule, cands):
        return {
            "run_id": run_id, "golden_id": gid, "attribute": attr,
            "value": json.dumps(win.value, default=str) if win and isinstance(win.value, dict) else (win.value if win else None),
            "winner_record_key": win.record_key if win else None,
            "winner_source": win.source_system if win else None,
            "deciding_rule": rule,
            "candidates": json.dumps([{"record_key": c.record_key, "value": c.value, "source": c.source_system,
                                       "updated_at": c.updated_at, "confidence": c.confidence} for c in cands], default=str),
        }
