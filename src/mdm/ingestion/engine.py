"""Bronze ingestion: land raw data exactly as received + technical metadata.

Design choices
--------------
* **Schema-on-read.** The raw record is stored whole in `_payload` (JSON). Bronze
  never loses information, so we can always re-standardize history after a
  bug-fix or rule change (replayability).
* **Data contract enforcement at the edge.** Required fields missing -> DLQ.
  Unknown fields -> handled per `schema_evolution` policy:
      rescue : keep record, copy unknown fields to `_rescued_data`, register new schema version
      evolve : accept new columns silently, register new schema version
      fail   : stop this source (contract breach), other sources continue
* **Idempotent & incremental.** File manifest (name+checksum) and high-water marks
  are committed only after the bronze write succeeds.
* **Isolation.** One failing source never stops the others (bulkhead pattern);
  the failure is logged, counted and alerted.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import pandas as pd

from mdm.config import PlatformConfig, SourceConfig
from mdm.ingestion.readers import READERS, ReadResult
from mdm.ingestion.state import StateStore
from mdm.observability.dead_letter import DeadLetterQueue
from mdm.observability.logging import get_logger, set_run_context
from mdm.observability.metrics import Metrics
from mdm.storage.lake import LakeStore

log = get_logger("ingestion")


class SchemaContractError(Exception):
    pass


def record_hash(payload: dict) -> str:
    """Stable content hash used for change detection (CDC on snapshots)."""
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()[:20]


class IngestionEngine:
    def __init__(self, cfg: PlatformConfig, lake: LakeStore, state: StateStore, dlq: DeadLetterQueue, metrics: Metrics, run_id: str):
        self.cfg, self.lake, self.state, self.dlq, self.m, self.run_id = cfg, lake, state, dlq, metrics, run_id

    def run(self) -> dict[str, pd.DataFrame]:
        out = {}
        for src in self.cfg.sources:
            set_run_context(source=src.name)
            try:
                out[src.name] = self._ingest_source(src)
            except SchemaContractError as e:
                self.m.inc("ingest.sources_failed")
                log.error("data contract breach - source skipped", error=str(e))
            except Exception as e:  # bulkhead: isolate failures per source
                self.m.inc("ingest.sources_failed")
                log.exception("source ingestion failed", error=str(e))
        set_run_context(source="*")
        total_src = sum(self.m.get(f"ingest.{s.name}.source_count") for s in self.cfg.sources)
        total_dlq = self.dlq.count("ingest")
        self.m.set("ingest.dead_letter_rate", round(total_dlq / total_src, 4) if total_src else 0.0)
        return out

    # ------------------------------------------------------------------
    def _check_schema(self, src: SourceConfig, observed: set[str]) -> set[str]:
        expected = set(src.schema)
        new_cols = observed - expected
        registered = self.state.schema(src.name)
        if registered is None:
            self.state.register_schema(src.name, sorted(expected | (new_cols if src.schema_evolution == "evolve" else set())), self.run_id, "initial")
        if new_cols:
            if src.schema_evolution == "fail":
                raise SchemaContractError(f"{src.name}: unexpected columns {sorted(new_cols)}")
            known = set((self.state.schema(src.name) or {}).get("columns", []))
            if not new_cols <= known:
                v = self.state.register_schema(src.name, sorted(known | new_cols), self.run_id,
                                               f"{src.schema_evolution}: added {sorted(new_cols - known)}")
                self.m.inc("ingest.schema_changes")
                log.warning("schema drift detected", new_columns=sorted(new_cols - known), policy=src.schema_evolution, schema_version=v)
        return new_cols

    def _ingest_source(self, src: SourceConfig) -> pd.DataFrame:
        reader_cls = READERS[src.format]
        reader = reader_cls(src, self.cfg.path("landing"), self.state, self.dlq, self.cfg.get("ingestion", {}))
        result: ReadResult = reader.read()

        observed = set().union(*[set(r.payload) for r in result.records]) if result.records else set()
        new_cols = self._check_schema(src, observed)
        schema_version = (self.state.schema(src.name) or {}).get("version", 1)

        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for r in result.records:
            missing = [f for f in src.required_fields if r.payload.get(f) in (None, "")]
            if missing:
                self.dlq.put("ingest", src.name, "MISSING_REQUIRED_FIELD", r.payload, f"missing {missing} at {r.source_file}#{r.position}")
                continue
            pk = "|".join(str(r.payload[k]) for k in src.primary_key)
            rescued = {k: r.payload[k] for k in new_cols if k in r.payload} if src.schema_evolution == "rescue" else {}
            rows.append({
                "_source_system": src.name,
                "_source_record_id": pk,
                "_ingest_run_id": self.run_id,
                "_ingest_ts": now,
                "_source_file": r.source_file,
                "_source_position": r.position,
                "_record_hash": record_hash(r.payload),
                "_schema_version": schema_version,
                "_payload": json.dumps(r.payload, default=str),
                "_rescued_data": json.dumps(rescued) if rescued else None,
            })
        df = pd.DataFrame(rows)
        if not df.empty:
            self.lake.write("bronze", src.name, df, mode="append", run_id=self.run_id)

        # commit incremental state only after a successful write (exactly-once)
        self.state.set_watermark(src.name, result.new_watermark)
        self.state.save()

        dlq_total = self.dlq.count("ingest", src.name)
        self.m.set(f"ingest.{src.name}.source_count", result.source_count)
        self.m.set(f"ingest.{src.name}.read", len(result.records))
        self.m.set(f"ingest.{src.name}.dead_letter", dlq_total)
        self.m.set(f"ingest.{src.name}.bronze", len(df))
        log.info("ingested", source_count=result.source_count, bronze=len(df), dead_letter=dlq_total,
                 units=len(result.units), watermark=result.new_watermark)
        return df
