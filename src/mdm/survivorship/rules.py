"""Survivorship rule catalogue.

Each rule takes the current candidate list and returns the subset that
survives. Rules are composed into chains in config/survivorship.yaml; the
first rule that leaves exactly one *distinct value* decides the attribute and
is recorded in lineage as the deciding rule.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass


@dataclass
class Candidate:
    record_key: str
    source_system: str
    value: object             # scalar, or dict for attribute groups
    updated_at: str | None
    confidence: float
    priority: int             # lower = more trusted


def _norm(v):
    if isinstance(v, dict):
        return tuple(sorted((k, str(x).lower() if x else None) for k, x in v.items()))
    return str(v).strip().lower()


def source_priority(cands):
    best = min(c.priority for c in cands)
    return [c for c in cands if c.priority == best]


def most_recent(cands):
    dated = [c for c in cands if c.updated_at]
    if not dated:
        return cands
    best = max(c.updated_at for c in dated)
    return [c for c in dated if c.updated_at == best]


def highest_confidence(cands):
    best = max(c.confidence for c in cands)
    return [c for c in cands if abs(c.confidence - best) < 1e-9]


def most_frequent(cands):
    counts = Counter(_norm(c.value) for c in cands)
    best = max(counts.values())
    return [c for c in cands if counts[_norm(c.value)] == best]


def most_complete(cands):
    def filled(c):
        return sum(1 for x in c.value.values() if x not in (None, "")) if isinstance(c.value, dict) else int(c.value is not None)
    best = max(filled(c) for c in cands)
    return [c for c in cands if filled(c) == best]


def longest(cands):
    best = max(len(str(c.value)) for c in cands)
    return [c for c in cands if len(str(c.value)) == best]


RULES = {f.__name__: f for f in (source_priority, most_recent, highest_confidence, most_frequent, most_complete, longest)}


def resolve(cands: list[Candidate], chain: list[str]) -> tuple[Candidate | None, str]:
    """Apply the chain; returns (winner, deciding_rule)."""
    if not cands:
        return None, "no_valid_candidates"
    if len({_norm(c.value) for c in cands}) == 1:
        return sorted(cands, key=lambda c: (c.priority, c.record_key))[0], "unanimous"
    for name in chain:
        cands = RULES[name](cands)
        if len({_norm(c.value) for c in cands}) == 1:
            return sorted(cands, key=lambda c: (c.priority, c.record_key))[0], name
    return sorted(cands, key=lambda c: (c.priority, c.record_key))[0], "tie_break:source_priority,record_key"
