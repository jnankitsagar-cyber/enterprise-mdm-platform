"""Source readers - one per *format*, not per source.

A new CSV source therefore needs only a YAML contract. A new *format*
(e.g. Avro, Kafka) is one new class registered in READERS.

Each reader returns a ReadResult with:
  * records         - raw records as dicts (untouched, schema-on-read)
  * source_count    - the control total declared by the source
                      (.ctl manifest, COUNT(*) or API `total`) used by reconciliation
  * new_watermark   - the high-water mark to persist after a successful load
  * units           - files / pages / queries processed (lineage)
"""
from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from urllib import request as urlrequest
from urllib.parse import urlencode

from mdm.config import SourceConfig
from mdm.observability.dead_letter import DeadLetterQueue
from mdm.observability.logging import get_logger
from mdm.observability.retry import TransientError, retry
from mdm.ingestion.state import StateStore

log = get_logger("ingestion")


@dataclass
class RawRecord:
    payload: dict
    source_file: str
    position: int


@dataclass
class ReadResult:
    records: list[RawRecord] = field(default_factory=list)
    source_count: int = 0
    new_watermark: str | None = None
    units: list[dict] = field(default_factory=list)


def _file_checksum(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _control_total(path: Path) -> int | None:
    ctl = path.with_suffix(path.suffix + ".ctl")
    if ctl.exists():
        return int(json.loads(ctl.read_text())["record_count"])
    return None


class BaseReader:
    def __init__(self, src: SourceConfig, landing: Path, state: StateStore, dlq: DeadLetterQueue, settings: dict):
        self.src, self.landing, self.state, self.dlq, self.settings = src, landing, state, dlq, settings

    def read(self) -> ReadResult:  # pragma: no cover - interface
        raise NotImplementedError

    def _new_files(self) -> list[Path]:
        files = sorted(self.landing.glob(self.src.location))
        out = []
        for f in files:
            key = f"{f.name}:{_file_checksum(f)}"     # name + checksum => re-dropped changed file is reprocessed
            if self.state.is_processed(self.src.name, key):
                log.info("skip already-processed file", file=f.name)
                continue
            out.append(f)
        return out

    def _mark(self, f: Path, rows: int) -> None:
        self.state.mark_processed(self.src.name, f"{f.name}:{_file_checksum(f)}", {"rows": rows})


class CsvReader(BaseReader):
    def read(self) -> ReadResult:
        res = ReadResult()
        for f in self._new_files():
            n_ok = 0
            with open(f, newline="", encoding="utf-8-sig") as fh:
                reader = csv.reader(fh)
                header = [h.strip() for h in next(reader)]
                data_rows = 0
                for i, row in enumerate(reader, start=2):
                    if not row or all(not c.strip() for c in row):
                        continue
                    data_rows += 1
                    if len(row) != len(header):
                        self.dlq.put("ingest", self.src.name, "MALFORMED_ROW", ",".join(row),
                                     f"{f.name}:{i} expected {len(header)} fields, got {len(row)}")
                        continue
                    res.records.append(RawRecord(dict(zip(header, [c if c != "" else None for c in row])), f.name, i))
                    n_ok += 1
            declared = _control_total(f)
            res.source_count += declared if declared is not None else data_rows
            res.units.append({"file": f.name, "declared": declared, "read": data_rows})
            self._mark(f, n_ok)
        return res


class JsonReader(BaseReader):
    def read(self) -> ReadResult:
        res = ReadResult()
        for f in self._new_files():
            text = f.read_text(encoding="utf-8")
            try:
                data = json.loads(text) if text.lstrip().startswith("[") else [json.loads(l) for l in text.splitlines() if l.strip()]
            except json.JSONDecodeError as e:
                self.dlq.put("ingest", self.src.name, "UNPARSEABLE_FILE", f.name, str(e))
                declared = _control_total(f) or 0
                res.source_count += declared
                continue
            for i, rec in enumerate(data):
                if not isinstance(rec, dict):
                    self.dlq.put("ingest", self.src.name, "NOT_AN_OBJECT", rec, f"{f.name}[{i}]")
                    continue
                res.records.append(RawRecord(rec, f.name, i))
            declared = _control_total(f)
            res.source_count += declared if declared is not None else len(data)
            res.units.append({"file": f.name, "declared": declared, "read": len(data)})
            self._mark(f, len(data))
        return res


class DatabaseReader(BaseReader):
    """Incremental extract with a high-water mark (`WHERE modified_ts > :wm`).

    The control total is a COUNT(*) with the *same* predicate, taken in the same
    transaction - so reconciliation compares like with like.
    """

    def read(self) -> ReadResult:
        res = ReadResult()
        db = self.landing / self.src.location
        if not db.exists():
            log.warning("database not found", path=str(db))
            return res
        col = self.src.incremental.get("column")
        wm = self.state.watermark(self.src.name)
        table = self.src.raw["table"]
        pred, params = ("", ())
        if col and wm:
            pred, params = (f" WHERE {col} > ?", (wm,))
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
        try:
            con.execute("BEGIN")
            res.source_count = con.execute(f"SELECT COUNT(*) FROM {table}{pred}", params).fetchone()[0]
            rows = con.execute(f"SELECT * FROM {table}{pred} ORDER BY {col}", params).fetchall()
        finally:
            con.close()
        for i, r in enumerate(rows):
            rec = {k: (None if r[k] is None else str(r[k])) for k in r.keys()}
            res.records.append(RawRecord(rec, f"{db.name}:{table}", i))
        if rows and col:
            res.new_watermark = max(str(r[col]) for r in rows)
        res.units.append({"query": f"SELECT * FROM {table}{pred}", "watermark_from": wm, "rows": len(rows)})
        return res


class RestApiReader(BaseReader):
    """Paginated REST client with retry/backoff and `updated_since` incremental filter.

    If `api.base_url` is null the API is simulated from page files in the
    landing zone, which lets the demo exercise pagination + retries offline.
    """

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.api = self.src.raw.get("api", {})
        self._failures_to_simulate = int(self.api.get("simulate_transient_failures", 0))

    def _fetch_page(self, page: int, updated_since: str | None) -> dict:
        @retry(max_attempts=int(self.settings.get("max_retries", 3)),
               backoff_seconds=float(self.settings.get("backoff_seconds", 0.2)))
        def _call():
            if self._failures_to_simulate > 0:
                self._failures_to_simulate -= 1
                raise TransientError("HTTP 503 Service Unavailable (simulated)")
            if self.api.get("base_url"):
                q = {"page": page, "page_size": self.api.get("page_size", 100)}
                if updated_since:
                    q["updated_since"] = updated_since
                url = f"{self.api['base_url']}?{urlencode(q)}"
                with urlrequest.urlopen(url, timeout=self.api.get("timeout_seconds", 10)) as r:  # noqa: S310
                    if r.status in (429, 502, 503, 504):
                        raise TransientError(f"HTTP {r.status}")
                    return json.loads(r.read())
            return self._mock_page(page, updated_since)
        return _call()

    def _mock_page(self, page: int, updated_since: str | None) -> dict:
        pages = sorted((self.landing / self.src.location).glob("page_*.json"))
        all_rows = []
        for p in pages:
            all_rows.extend(json.loads(p.read_text())["data"])
        col = self.src.incremental.get("column", "updated")
        if updated_since:
            all_rows = [r for r in all_rows if str(r.get(col, "")) > updated_since]
        size = int(self.api.get("page_size", 100))
        chunk = all_rows[(page - 1) * size: page * size]
        return {"data": chunk, "page": page, "total": len(all_rows),
                "next_page": page + 1 if page * size < len(all_rows) else None}

    def read(self) -> ReadResult:
        res = ReadResult()
        wm = self.state.watermark(self.src.name)
        col = self.src.incremental.get("column", "updated")
        page, total = 1, None
        while page:
            body = self._fetch_page(page, wm)
            total = body.get("total", total)
            for i, rec in enumerate(body["data"]):
                res.records.append(RawRecord(rec, f"api:page={page}", i))
            res.units.append({"page": page, "rows": len(body["data"])})
            page = body.get("next_page")
        res.source_count = total or 0
        vals = [str(r.payload.get(col)) for r in res.records if r.payload.get(col)]
        res.new_watermark = max(vals) if vals else None
        return res


READERS = {"csv": CsvReader, "json": JsonReader, "database": DatabaseReader, "rest_api": RestApiReader}
