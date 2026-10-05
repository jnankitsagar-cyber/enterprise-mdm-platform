"""Ingestion state: high-water marks, processed-file manifest, schema registry.

Equivalent in production: Auto Loader checkpoints (files), a control table in
Azure SQL / Delta for watermarks, and the Unity Catalog / schema registry for
schema versions.
"""
from __future__ import annotations

import json
from pathlib import Path


class StateStore:
    def __init__(self, state_dir: Path):
        self.dir = Path(state_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / "ingestion_state.json"
        self.data = json.loads(self.path.read_text()) if self.path.exists() else {
            "watermarks": {}, "file_manifest": {}, "schema_registry": {}
        }

    # watermarks ------------------------------------------------------------
    def watermark(self, source: str):
        return self.data["watermarks"].get(source)

    def set_watermark(self, source: str, value) -> None:
        if value is not None:
            self.data["watermarks"][source] = value

    # file manifest ---------------------------------------------------------
    def is_processed(self, source: str, file_key: str) -> bool:
        return file_key in self.data["file_manifest"].get(source, {})

    def mark_processed(self, source: str, file_key: str, info: dict) -> None:
        self.data["file_manifest"].setdefault(source, {})[file_key] = info

    # schema registry -------------------------------------------------------
    def schema(self, source: str) -> dict | None:
        versions = self.data["schema_registry"].get(source)
        return versions[-1] if versions else None

    def register_schema(self, source: str, columns: list[str], run_id: str, change: str) -> int:
        versions = self.data["schema_registry"].setdefault(source, [])
        version = len(versions) + 1
        versions.append({"version": version, "columns": sorted(columns), "run_id": run_id, "change": change})
        return version

    def save(self) -> None:
        self.path.write_text(json.dumps(self.data, indent=2, default=str))
