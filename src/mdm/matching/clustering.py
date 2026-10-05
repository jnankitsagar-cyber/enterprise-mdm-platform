"""Constrained clustering (transitive closure with guard-rails).

Plain union-find over MATCH edges suffers from *chaining*: A~B and B~C merge
A and C even if A and C clearly conflict (e.g. different DOB). We therefore:
  1. process edges from highest to lowest probability,
  2. refuse a union if ANY pair across the two clusters is vetoed,
  3. refuse a union that would exceed `max_cluster_size`.
Refused edges are downgraded to REVIEW so a data steward can decide.
"""
from __future__ import annotations

from typing import Callable


class ConstrainedUnionFind:
    def __init__(self, keys, conflict: Callable[[str, str], bool], max_size: int):
        self.parent = {k: k for k in keys}
        self.members = {k: {k} for k in keys}
        self.conflict = conflict
        self.max_size = max_size

    def find(self, k: str) -> str:
        while self.parent[k] != k:
            self.parent[k] = self.parent[self.parent[k]]
            k = self.parent[k]
        return k

    def union(self, a: str, b: str) -> tuple[bool, str]:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return True, "already_linked"
        ma, mb = self.members[ra], self.members[rb]
        if len(ma) + len(mb) > self.max_size:
            return False, "max_cluster_size"
        for x in ma:
            for y in mb:
                if self.conflict(x, y):
                    return False, f"transitive_conflict:{x}~{y}"
        if len(ma) < len(mb):
            ra, rb, ma, mb = rb, ra, mb, ma
        self.parent[rb] = ra
        ma |= mb
        del self.members[rb]
        return True, "merged"

    def clusters(self) -> list[set[str]]:
        return [set(m) for m in self.members.values()]
