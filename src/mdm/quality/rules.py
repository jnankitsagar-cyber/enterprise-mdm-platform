"""Data-quality rule types.

A rule *type* is code (written once, unit-tested); a rule *instance* is config.
New checks of an existing type = YAML only. Truly novel logic = an `expression`
rule (still YAML) or, rarely, a new rule type registered with @rule_type.
"""
from __future__ import annotations

import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone

from mdm.quality.safe_eval import compile_expression, evaluate

RULE_TYPES: dict[str, type] = {}


def rule_type(name: str):
    def deco(cls):
        RULE_TYPES[name] = cls
        cls.type_name = name
        return cls
    return deco


def _null(v) -> bool:
    return v is None or (isinstance(v, float) and v != v) or (isinstance(v, str) and v.strip() == "")


def resolve_relative(spec: str):
    """'today-16y', 'now+1d', '2020-01-01' -> date / datetime."""
    m = re.fullmatch(r"(today|now)(?:([+-])(\d+)([yd]))?", str(spec))
    if not m:
        return date.fromisoformat(spec)
    base = datetime.now(timezone.utc)
    if m.group(2):
        n = int(m.group(3)) * (365 if m.group(4) == "y" else 1)
        base = base + timedelta(days=n if m.group(2) == "+" else -n)
    return base.date() if m.group(1) == "today" else base


class Rule:
    scope = "record"

    def __init__(self, spec: dict, ref=None):
        self.spec = spec
        self.id, self.name = spec["id"], spec["name"]
        self.severity = spec.get("severity", "error")
        self.dimension = spec.get("dimension", "validity")
        self.sources = spec.get("sources")
        self.ref = ref
        self.when = compile_expression(spec["when"]) if spec.get("when") else None
        self.columns = spec.get("columns") or ([spec["column"]] if spec.get("column") else [])

    def applies(self, rec: dict) -> bool:
        if self.sources and rec.get("source_system") not in self.sources:
            return False
        return self.when is None or evaluate(self.when, rec)

    def check(self, rec: dict) -> bool:  # True = pass
        raise NotImplementedError

    def prepare(self, records: list[dict]) -> None:
        """Hook for batch-aware record rules (e.g. uniqueness)."""


@rule_type("not_null")
class NotNull(Rule):
    def check(self, rec):
        return all(not _null(rec.get(c)) for c in self.columns)


@rule_type("at_least_one_of")
class AtLeastOneOf(Rule):
    def check(self, rec):
        return any(not _null(rec.get(c)) for c in self.columns)


@rule_type("regex")
class Regex(Rule):
    def __init__(self, spec, ref=None):
        super().__init__(spec, ref)
        self.pattern = re.compile(spec["pattern"])

    def check(self, rec):
        v = rec.get(self.columns[0])
        return _null(v) or bool(self.pattern.fullmatch(str(v)))


@rule_type("datatype")
class Datatype(Rule):
    """Raw value present but standardized value null => the value could not be typed."""

    def check(self, rec):
        col = self.columns[0]
        v = rec.get(col)
        raw = (rec.get("_raw_dict") or {}).get(self.spec.get("raw_column", col))
        if _null(v):
            return _null(raw)
        try:
            if self.spec["datatype"] == "date":
                date.fromisoformat(str(v))
            elif self.spec["datatype"] == "int":
                int(v)
            return True
        except ValueError:
            return False


@rule_type("standardization_error")
class StandardizationErrorRule(Rule):
    """Fails when the standardizer could not normalise a value that WAS supplied
    (e.g. phone '12345'). The raw value is preserved in lineage for the steward."""

    def check(self, rec):
        errs = rec.get("_std_errors_list")
        if errs is None:
            import json as _json
            errs = _json.loads(rec.get("_std_errors") or "[]")
            rec["_std_errors_list"] = errs
        return not any(e.get("attr") in self.columns for e in errs)


@rule_type("allowed_values")
class AllowedValues(Rule):
    def check(self, rec):
        v = rec.get(self.columns[0])
        if _null(v):
            return self.spec.get("allow_null", True)
        return v in set(self.spec["values"])


@rule_type("range")
class Range(Rule):
    def check(self, rec):
        v = rec.get(self.columns[0])
        if _null(v):
            return True
        lo = resolve_relative(self.spec["min"]) if "min" in self.spec else None
        hi = resolve_relative(self.spec["max"]) if "max" in self.spec else None
        sample = lo if lo is not None else hi
        if isinstance(sample, datetime):
            val = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        elif isinstance(sample, date):
            val = date.fromisoformat(str(v)[:10])
        else:
            val = float(v)
        return (lo is None or val >= lo) and (hi is None or val <= hi)


@rule_type("reference")
class Reference(Rule):
    """Referential integrity against a reference table."""

    def check(self, rec):
        v = rec.get(self.columns[0])
        if _null(v):
            return self.spec.get("allow_null", True)
        return v in self.ref.values(self.spec["table"], self.spec.get("reference_column", "code"))


@rule_type("unique")
class Unique(Rule):
    """Duplicate business keys within a batch: all but the latest copy fail."""

    def prepare(self, records):
        self._latest = {}
        for i, r in enumerate(records):
            key = tuple(r.get(c) for c in self.columns)
            ts = r.get("source_updated_at") or ""
            if key not in self._latest or (ts, i) >= self._latest[key]:
                self._latest[key] = (ts, i)
        self._pos = {id(r): i for i, r in enumerate(records)}
        self._counts = Counter(tuple(r.get(c) for c in self.columns) for r in records)

    def check(self, rec):
        key = tuple(rec.get(c) for c in self.columns)
        if self._counts.get(key, 0) <= 1:
            return True
        return self._latest[key][1] == self._pos.get(id(rec))


@rule_type("expression")
class Expression(Rule):
    def __init__(self, spec, ref=None):
        super().__init__(spec, ref)
        self.code = compile_expression(spec["expression"])

    def check(self, rec):
        return evaluate(self.code, rec)


@rule_type("timeliness")
class Timeliness(Rule):
    def check(self, rec):
        v = rec.get(self.columns[0])
        if _null(v):
            return True
        ts = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - ts).days <= int(self.spec["max_age_days"])


# ---------------------------------------------------------------- dataset rules
class DatasetRule(Rule):
    scope = "dataset"

    def evaluate(self, records: list[dict]) -> tuple[bool, float]:
        raise NotImplementedError


@rule_type("completeness")
class Completeness(DatasetRule):
    def evaluate(self, records):
        if not records:
            return True, 1.0
        ratio = sum(1 for r in records if not _null(r.get(self.columns[0]))) / len(records)
        return ratio >= float(self.spec["min_ratio"]), round(ratio, 4)


@rule_type("row_count")
class RowCount(DatasetRule):
    def evaluate(self, records):
        n = len(records)
        ok = n >= self.spec.get("min", 0) and n <= self.spec.get("max", float("inf"))
        return ok, n
