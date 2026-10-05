"""End-to-end pipeline orchestration (one run = one batch through the medallion).

    landing ──► BRONZE ──► SILVER (standardize + DQ) ──► GOLD (match, survive) ──► SERVING
                 raw         canonical, current-state      xref, golden, lineage     SQLite/API
                                                                 │
                                       reconciliation + metrics + alerts + dashboard

Locally this class runs every stage in-process. In Azure the same stage
boundaries are separate Databricks Workflow tasks triggered by ADF
(orchestration/), and each stage reads/writes Delta tables instead of files.
Because every stage persists its output, any stage can be re-run on its own.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone

import pandas as pd

from mdm.config import PlatformConfig
from mdm.ingestion.engine import IngestionEngine
from mdm.ingestion.state import StateStore
from mdm.lineage.tracker import LineageTracker
from mdm.matching.engine import IdentityResolutionEngine
from mdm.observability.dashboard import build_dashboard
from mdm.observability.dead_letter import DeadLetterQueue
from mdm.observability.logging import configure_logging, get_logger, set_run_context
from mdm.observability.metrics import Metrics
from mdm.quality.engine import DataQualityEngine
from mdm.reconciliation.engine import ReconciliationEngine
from mdm.standardization.engine import StandardizationEngine
from mdm.storage.lake import LakeStore
from mdm.survivorship.engine import SurvivorshipEngine

log = get_logger("pipeline")


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


class Pipeline:
    def __init__(self, cfg: PlatformConfig, log_format: str | None = None):
        self.cfg = cfg
        self.run_id = new_run_id()
        reports = cfg.path("reports")
        (reports / "logs").mkdir(parents=True, exist_ok=True)
        configure_logging(cfg.get("observability.log_level", "INFO"),
                          log_format or cfg.get("observability.log_format", "json"),
                          logfile=str(reports / "logs" / f"run_{self.run_id}.jsonl"))
        set_run_context(run_id=self.run_id, platform=cfg.get("platform.name"))
        self.lake = LakeStore(cfg.path("lakehouse"), cfg.get("storage.format", "auto"))
        self.state = StateStore(cfg.path("state"))
        self.metrics = Metrics(self.run_id)
        self.dlq = DeadLetterQueue(cfg.path("lakehouse"), self.run_id)
        self.lineage = LineageTracker(cfg.path("lakehouse"), self.run_id)

    # ------------------------------------------------------------------
    def run(self) -> dict:
        started = datetime.now(timezone.utc).isoformat()
        m, lake, run_id = self.metrics, self.lake, self.run_id
        log.info("pipeline run started", lake_format=lake.fmt)
        status = "SUCCESS"
        try:
            # 1. BRONZE -------------------------------------------------------
            with m.stage("ingest"):
                bronze = IngestionEngine(self.cfg, lake, self.state, self.dlq, m, run_id).run()
                self.lineage.emit("ingest", [f"landing.{s.name}" for s in self.cfg.sources],
                                  [f"bronze.{s}" for s in bronze])

            # 2. SILVER: standardize ---------------------------------------------
            with m.stage("standardize"):
                prev_silver = lake.read("silver", "party")
                std = StandardizationEngine(self.cfg, m)
                _, changed = std.run(bronze, pd.DataFrame(), run_id, self.dlq)
                unchanged_mask = pd.Series(False, index=changed.index)
                if not prev_silver.empty and not changed.empty:
                    known = set(zip(prev_silver["record_key"], prev_silver["_record_hash"]))
                    unchanged_mask = pd.Series([(k, h) in known for k, h in zip(changed["record_key"], changed["_record_hash"])],
                                               index=changed.index)
                to_check = changed[~unchanged_mask] if not changed.empty else changed
                m.set("standardize.unchanged_resends", int(unchanged_mask.sum()))

            # 3. SILVER: data quality --------------------------------------------
            with m.stage("data_quality"):
                dq = DataQualityEngine(self.cfg.dq, self.cfg.reference, m)
                checked, dq_fail, dq_ds = dq.run(to_check, run_id)
                if not dq_fail.empty:
                    lake.write("silver", "dq_results", dq_fail, mode="append", run_id=run_id)
                if not dq_ds.empty:
                    lake.write("silver", "dq_dataset_results", dq_ds, mode="append", run_id=run_id)
                silver = StandardizationEngine._merge_current(prev_silver, checked)
                if not silver.empty:
                    lake.write("silver", "party", silver, mode="overwrite", run_id=run_id)
                self.lineage.emit("standardize_and_validate", [f"bronze.{s}" for s in bronze],
                                  ["silver.party", "silver.dq_results"])

            # 4. GOLD: identity resolution ----------------------------------------
            with m.stage("identity_resolution"):
                prev_xref = lake.read("gold", "xref")
                prev_pairs = lake.read("gold", "match_pairs")
                changed_keys = set(checked["record_key"]) if not checked.empty else set()
                ire = IdentityResolutionEngine(self.cfg.matching, self.cfg.reference, m,
                                               steward_file=self.cfg.path("state") / "steward_decisions.csv")
                pairs, xref, events, review = ire.run(silver, prev_xref, prev_pairs, changed_keys, run_id)
                lake.write("gold", "match_pairs", pairs, mode="overwrite", run_id=run_id)
                lake.write("gold", "xref", xref, mode="overwrite", run_id=run_id)
                lake.write("gold", "review_queue", review if not review.empty else pd.DataFrame(columns=pairs.columns),
                           mode="overwrite", run_id=run_id)
                if not events.empty:
                    lake.write("gold", "golden_events", events, mode="append", run_id=run_id)

            # 5. GOLD: survivorship ------------------------------------------------
            with m.stage("survivorship"):
                prev_golden = lake.read("gold", "golden_record")
                surv = SurvivorshipEngine(self.cfg.survivorship, m)
                golden, surv_log, history = surv.run(silver, xref, prev_golden, pairs, run_id)
                lake.write("gold", "golden_record", golden, mode="overwrite", run_id=run_id)
                lake.write("gold", "survivorship_log", surv_log, mode="overwrite", run_id=run_id)
                if not history.empty:
                    lake.write("gold", "golden_record_history", history, mode="append", run_id=run_id)
                self._update_redirects(events)
                self.lineage.emit("identity_resolution_and_survivorship", ["silver.party"],
                                  ["gold.xref", "gold.match_pairs", "gold.golden_record", "gold.survivorship_log"])

            # 6. Reconciliation ----------------------------------------------------
            with m.stage("reconciliation"):
                stage_counts = self._stage_counts(changed, unchanged_mask, checked)
                recon = ReconciliationEngine(m).run(
                    [s.name for s in self.cfg.sources], stage_counts, silver, xref, golden, events,
                    len(prev_golden), run_id)
                ReconciliationEngine.write(recon, self.cfg.path("reports"))
                if not dq_fail.empty:
                    checked_by_src = checked.groupby("source_system").size().to_dict()
                    DataQualityEngine.scorecard(dq_fail, checked_by_src).to_csv(
                        self.cfg.path("reports") / f"dq_scorecard_{run_id}.csv", index=False)

            # 7. Serving -------------------------------------------------------------
            with m.stage("publish_serving"):
                self._publish_serving(golden, xref)
                self.lineage.emit("publish_serving", ["gold.golden_record", "gold.xref"], ["serving.golden_record"])
        except Exception as e:
            status = "FAILED"
            log.exception("pipeline run failed", error=str(e))
            raise
        finally:
            set_run_context(stage="finalize")
            m.evaluate_alerts(self.cfg.get("observability.alerts", []))
            self.dlq.flush()
            self.lineage.flush()
            self.state.save()
            run_dir = self.cfg.path("reports") / "runs" / run_id
            m.write(run_dir)
            manifest = {"run_id": run_id, "status": status, "started": started,
                        "finished": datetime.now(timezone.utc).isoformat(), **m.to_dict()}
            runs = self.cfg.path("lakehouse") / "_runs.jsonl"
            with open(runs, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(manifest, default=str) + "\n")
            try:
                build_dashboard(self.cfg, self.lake)
            except Exception as e:  # dashboard must never fail a run
                log.warning("dashboard build failed", error=str(e))
        log.info("pipeline run finished", status=status, alerts=len(m.alerts))
        return {"run_id": run_id, "status": status, "reconciliation": recon, "metrics": m.to_dict()}

    # ------------------------------------------------------------------
    def _stage_counts(self, changed: pd.DataFrame, unchanged_mask: pd.Series, checked: pd.DataFrame) -> dict:
        out = {}
        for s in self.cfg.sources:
            n = s.name
            ch = changed[changed["source_system"] == n] if not changed.empty else changed
            ck = checked[checked["source_system"] == n] if not checked.empty else checked
            st = ck["_dq_status"].value_counts().to_dict() if not ck.empty else {}
            out[n] = {
                "source_count": int(self.metrics.get(f"ingest.{n}.source_count")),
                "bronze": int(self.metrics.get(f"ingest.{n}.bronze")),
                "ingest_dlq": int(self.metrics.get(f"ingest.{n}.dead_letter")),
                "standardized": len(ch),
                "std_dlq": self.dlq.count("standardize", n),
                "unchanged": int(unchanged_mask[ch.index].sum()) if len(ch) else 0,
                "dq_checked": len(ck),
                "valid": int(st.get("VALID", 0)),
                "warning": int(st.get("WARNING", 0)),
                "quarantined": int(st.get("QUARANTINED", 0)),
            }
        return out

    def _update_redirects(self, events: pd.DataFrame) -> None:
        if events.empty:
            return
        merges = events[events["event"] == "MERGE"]
        if merges.empty:
            return
        new = pd.DataFrame({"old_golden_id": merges["related_golden_id"], "new_golden_id": merges["golden_id"],
                            "run_id": self.run_id})
        prev = self.lake.read("gold", "golden_redirects")
        allr = pd.concat([f for f in (prev, new) if not f.empty], ignore_index=True)
        # collapse chains A->B, B->C  =>  A->C so consumers resolve in one hop
        mp = dict(zip(allr["old_golden_id"], allr["new_golden_id"]))
        for k in list(mp):
            seen = {k}
            while mp[k] in mp and mp[k] not in seen:
                seen.add(mp[k])
                mp[k] = mp[mp[k]]
        allr["new_golden_id"] = allr["old_golden_id"].map(mp)
        self.lake.write("gold", "golden_redirects", allr.drop_duplicates("old_golden_id", keep="last"),
                        mode="overwrite", run_id=self.run_id)

    def _publish_serving(self, golden: pd.DataFrame, xref: pd.DataFrame) -> None:
        """Publish gold to the low-latency serving store (SQLite here; Azure SQL/Cosmos in prod)."""
        path = self.cfg.root / self.cfg.get("storage.serving_db")
        path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(path)
        try:
            golden.to_sql("golden_record", con, if_exists="replace", index=False)
            xref.to_sql("xref", con, if_exists="replace", index=False)
            red = self.lake.read("gold", "golden_redirects")
            (red if not red.empty else pd.DataFrame(columns=["old_golden_id", "new_golden_id", "run_id"])).to_sql(
                "golden_redirects", con, if_exists="replace", index=False)
            con.execute("CREATE INDEX IF NOT EXISTS ix_g_email ON golden_record(email)")
            con.execute("CREATE INDEX IF NOT EXISTS ix_g_phone ON golden_record(phone)")
            con.execute("CREATE INDEX IF NOT EXISTS ix_x_src ON xref(source_system, source_record_id)")
            con.commit()
        finally:
            con.close()
