"""Dead-letter queue: records that cannot be processed are never silently dropped.

Each entry keeps the raw payload, the stage, a machine-readable reason code and
the run id, so it can be fixed upstream and replayed. Reconciliation counts DLQ
entries so `source = processed + dead-lettered` always balances.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


class DeadLetterQueue:
    def __init__(self, root: Path, run_id: str):
        self.dir = root / "_dead_letter"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id
        self.entries: list[dict] = []

    def put(self, stage: str, source: str, reason: str, payload, detail: str = "") -> None:
        self.entries.append({
            "run_id": self.run_id,
            "stage": stage,
            "source_system": source,
            "reason": reason,
            "detail": detail,
            "payload": payload if isinstance(payload, (str, int, float)) else json.dumps(payload, default=str),
            "ts": datetime.now(timezone.utc).isoformat(),
        })

    def count(self, stage: str | None = None, source: str | None = None) -> int:
        return sum(1 for e in self.entries if (stage is None or e["stage"] == stage) and (source is None or e["source_system"] == source))

    def flush(self) -> Path | None:
        if not self.entries:
            return None
        path = self.dir / f"{self.run_id}.jsonl"
        with open(path, "a", encoding="utf-8") as fh:
            for e in self.entries:
                fh.write(json.dumps(e, default=str) + "\n")
        return path
