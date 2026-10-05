"""Local lakehouse storage abstraction (Medallion: bronze / silver / gold).

Production mapping
------------------
    LakeStore.write(layer, table, df, "append")     ->  df.write.format("delta").mode("append")
    LakeStore.write(layer, table, df, "overwrite")  ->  Delta MERGE / overwrite
    LakeStore.read(layer, table)                    ->  spark.read.table("mdm_<layer>.<table>")
    partitions (run_id=...)                          ->  Delta partition / _commit version

Locally we store Parquet (if pyarrow is installed) or JSON-lines, so the demo has
zero heavy dependencies. Every write also snapshots the table version per run,
which emulates Delta time-travel for audit ("what did the golden record look
like after run X?").
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pandas as pd

try:  # optional dependency
    import pyarrow  # noqa: F401
    _HAS_PARQUET = True
except Exception:  # pragma: no cover
    _HAS_PARQUET = False

LAYERS = ("bronze", "silver", "gold")


class LakeStore:
    def __init__(self, root: Path, fmt: str = "auto"):
        self.root = Path(root)
        self.fmt = ("parquet" if _HAS_PARQUET else "jsonl") if fmt == "auto" else fmt
        for layer in LAYERS:
            (self.root / layer).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ paths
    def _table_dir(self, layer: str, table: str) -> Path:
        assert layer in LAYERS, layer
        return self.root / layer / table

    def _file(self, d: Path, name: str) -> Path:
        return d / f"{name}.{self.fmt}"

    # ------------------------------------------------------------------ io
    @staticmethod
    def _serialise(df: pd.DataFrame) -> pd.DataFrame:
        """Nested python objects -> JSON strings (portable across parquet/jsonl/sql)."""
        out = df.copy()
        for c in out.columns:
            if out[c].map(lambda v: isinstance(v, (dict, list))).any():
                out[c] = out[c].map(lambda v: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v)
        return out

    def _write_file(self, df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        df = self._serialise(df)
        if self.fmt == "parquet":
            df.to_parquet(path, index=False)
        else:
            df.to_json(path, orient="records", lines=True, date_format="iso", force_ascii=False)

    def _read_file(self, path: Path) -> pd.DataFrame:
        if self.fmt == "parquet":
            return pd.read_parquet(path)
        if path.stat().st_size == 0:
            return pd.DataFrame()
        return pd.read_json(path, orient="records", lines=True, dtype=False, convert_dates=False)

    def write(self, layer: str, table: str, df: pd.DataFrame, mode: str = "overwrite", run_id: str | None = None) -> Path:
        d = self._table_dir(layer, table)
        if mode == "append":
            # append-only, partitioned by run (bronze semantics)
            assert run_id, "append requires run_id (partition)"
            path = self._file(d, f"run_id={run_id}")
            self._write_file(df, path)
            return path
        if mode == "overwrite":
            path = self._file(d, "current")
            self._write_file(df, path)
            if run_id:  # snapshot version for time-travel / audit
                self._write_file(df, self._file(d / "_versions", run_id))
            return path
        raise ValueError(mode)

    def read(self, layer: str, table: str, run_id: str | None = None) -> pd.DataFrame:
        d = self._table_dir(layer, table)
        if not d.exists():
            return pd.DataFrame()
        if run_id:
            p = self._file(d, f"run_id={run_id}")
            if not p.exists():
                p = self._file(d / "_versions", run_id)
            return self._read_file(p) if p.exists() else pd.DataFrame()
        current = self._file(d, "current")
        if current.exists():
            return self._read_file(current)
        parts = sorted(d.glob(f"run_id=*.{self.fmt}"))
        frames = [self._read_file(p) for p in parts]
        frames = [f for f in frames if not f.empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def exists(self, layer: str, table: str) -> bool:
        return self._table_dir(layer, table).exists()

    def reset(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)
        for layer in LAYERS:
            (self.root / layer).mkdir(parents=True, exist_ok=True)
