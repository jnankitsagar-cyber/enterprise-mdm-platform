"""Read-side repository over the serving store + lakehouse (framework-agnostic).

Both the FastAPI app and the zero-dependency stdlib server call these methods,
so business logic (redirect resolution, lookups, lineage) lives in one place.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from mdm.config import PlatformConfig
from mdm.lineage.tracker import build_trace
from mdm.storage.lake import LakeStore


class GoldenRecordRepository:
    def __init__(self, cfg: PlatformConfig):
        self.cfg = cfg
        self.db = cfg.root / cfg.get("storage.serving_db")
        self.lake = LakeStore(cfg.path("lakehouse"), cfg.get("storage.format", "auto"))

    def _q(self, sql: str, params=()) -> list[dict]:
        if not Path(self.db).exists():
            return []
        con = sqlite3.connect(self.db)
        con.row_factory = sqlite3.Row
        try:
            return [self._decode(dict(r)) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()

    @staticmethod
    def _decode(r: dict) -> dict:
        for k in ("source_systems", "member_record_keys"):
            if isinstance(r.get(k), str):
                r[k] = json.loads(r[k])
        return r

    def resolve_id(self, golden_id: str) -> str:
        red = self._q("SELECT new_golden_id FROM golden_redirects WHERE old_golden_id = ?", (golden_id,))
        return red[0]["new_golden_id"] if red else golden_id

    def get(self, golden_id: str) -> dict | None:
        gid = self.resolve_id(golden_id)
        rows = self._q("SELECT * FROM golden_record WHERE golden_id = ?", (gid,))
        if not rows:
            return None
        out = rows[0]
        if gid != golden_id:
            out["_redirected_from"] = golden_id      # consumers update their stored id
        return out

    def search(self, email: str | None = None, phone: str | None = None, last_name: str | None = None,
               limit: int = 50, offset: int = 0) -> list[dict]:
        clauses, params = [], []
        if email:
            clauses.append("lower(email) = lower(?)"); params.append(email)
        if phone:
            clauses.append("phone = ?"); params.append(phone)
        if last_name:
            clauses.append("lower(last_name) = lower(?)"); params.append(last_name)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        return self._q(f"SELECT * FROM golden_record{where} ORDER BY golden_id LIMIT ? OFFSET ?", (*params, limit, offset))

    def by_source(self, source_system: str, source_record_id: str) -> dict | None:
        x = self._q("SELECT golden_id FROM xref WHERE source_system = ? AND source_record_id = ?", (source_system, source_record_id))
        return self.get(x[0]["golden_id"]) if x else None

    def lineage(self, golden_id: str) -> dict | None:
        return build_trace(self.lake, golden_id)

    def latest_reconciliation(self) -> dict | None:
        reports = sorted(self.cfg.path("reports").glob("reconciliation_*.json"))
        return json.loads(reports[-1].read_text()) if reports else None

    def stats(self) -> dict:
        g = self._q("SELECT COUNT(*) n FROM golden_record")
        x = self._q("SELECT COUNT(*) n FROM xref")
        return {"golden_records": g[0]["n"] if g else 0, "source_records": x[0]["n"] if x else 0}
