"""Configuration loading.

All behaviour of the platform is driven by YAML in /config. Code only provides
*capabilities* (rule types, standardizer functions, readers); configuration
decides *which* capabilities apply to *which* data. This is what lets a new
source, a new DQ rule or a new survivorship policy ship without a code change.
"""
from __future__ import annotations

import csv
import glob
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


def load_yaml(path: str | Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


@dataclass
class SourceConfig:
    """A source system's *data contract*."""

    name: str
    format: str
    location: str
    mapping: dict[str, str]
    schema: dict[str, dict]
    primary_key: list[str]
    trust_score: float = 0.5
    incremental: dict = field(default_factory=dict)
    schema_evolution: str = "rescue"
    date_formats: list[str] = field(default_factory=list)
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, d: dict) -> "SourceConfig":
        return cls(
            name=d["name"],
            format=d["format"],
            location=d["location"],
            mapping=d["mapping"],
            schema=d.get("schema", {}),
            primary_key=d.get("primary_key", []),
            trust_score=float(d.get("trust_score", 0.5)),
            incremental=d.get("incremental", {}),
            schema_evolution=d.get("schema_evolution", "rescue"),
            date_formats=d.get("date_formats", []),
            raw=d,
        )

    @property
    def required_fields(self) -> list[str]:
        return [k for k, v in self.schema.items() if (v or {}).get("required")]


class ReferenceData:
    """Reference tables (country codes, states, nicknames ...) loaded from CSV.

    In production these are mastered reference-data tables (e.g. a Delta table
    owned by data governance); the interface stays the same.
    """

    def __init__(self, ref_dir: str | Path):
        self.tables: dict[str, list[dict]] = {}
        for path in glob.glob(os.path.join(ref_dir, "*.csv")):
            with open(path, encoding="utf-8") as fh:
                self.tables[Path(path).stem] = list(csv.DictReader(fh))
        self._lookup_cache: dict[str, dict] = {}

    def lookup(self, table: str, key_col: str = "alias", val_col: str = "code") -> dict:
        ck = f"{table}:{key_col}:{val_col}"
        if ck not in self._lookup_cache:
            self._lookup_cache[ck] = {
                r[key_col].strip().upper(): r[val_col] for r in self.tables.get(table, [])
            }
        return self._lookup_cache[ck]

    def values(self, table: str, col: str) -> set:
        return {r[col] for r in self.tables.get(table, [])}


@dataclass
class PlatformConfig:
    root: Path
    platform: dict
    sources: list[SourceConfig]
    standardization: dict
    dq: dict
    matching: dict
    survivorship: dict
    reference: ReferenceData

    def path(self, key: str) -> Path:
        p = Path(self.platform["paths"][key])
        return p if p.is_absolute() else self.root / p

    def source(self, name: str) -> SourceConfig:
        return next(s for s in self.sources if s.name == name)

    @classmethod
    def load(cls, root: str | Path = ".", platform_file: str = "config/platform.yaml") -> "PlatformConfig":
        root = Path(root).resolve()
        plat = load_yaml(root / platform_file)
        src_dir = root / plat["sources_dir"]
        sources = [SourceConfig.from_dict(load_yaml(p)) for p in sorted(src_dir.glob("*.yaml"))]
        return cls(
            root=root,
            platform=plat,
            sources=sources,
            standardization=load_yaml(root / plat["standardization"]),
            dq=load_yaml(root / plat["data_quality"]),
            matching=load_yaml(root / plat["matching"]),
            survivorship=load_yaml(root / plat["survivorship"]),
            reference=ReferenceData(root / plat["reference_dir"]),
        )

    def get(self, dotted: str, default: Any = None) -> Any:
        node: Any = self.platform
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node
