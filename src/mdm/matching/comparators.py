"""String / value comparators used by blocking, rules and scoring.

Implemented in pure Python (no C extensions) so they run anywhere, including
as Spark UDFs. For tens of millions of records the production job swaps in
vectorised implementations (rapidfuzz / Spark native `levenshtein`, `soundex`)
behind the same names - see docs/MATCHING.md.
"""
from __future__ import annotations

from datetime import date
from functools import lru_cache


@lru_cache(maxsize=200_000)
def jaro_winkler(a: str, b: str, p: float = 0.1) -> float:
    if a is None or b is None:
        return 0.0
    a, b = a.lower(), b.lower()
    if a == b:
        return 1.0
    la, lb = len(a), len(b)
    if not la or not lb:
        return 0.0
    window = max(la, lb) // 2 - 1
    a_m, b_m = [False] * la, [False] * lb
    matches = 0
    for i in range(la):
        lo, hi = max(0, i - window), min(i + window + 1, lb)
        for j in range(lo, hi):
            if not b_m[j] and a[i] == b[j]:
                a_m[i] = b_m[j] = True
                matches += 1
                break
    if not matches:
        return 0.0
    t, k = 0, 0
    for i in range(la):
        if a_m[i]:
            while not b_m[k]:
                k += 1
            if a[i] != b[k]:
                t += 1
            k += 1
    t /= 2
    jaro = (matches / la + matches / lb + (matches - t) / matches) / 3
    prefix = 0
    for x, y in zip(a[:4], b[:4]):
        if x != y:
            break
        prefix += 1
    return round(jaro + prefix * p * (1 - jaro), 4)


class NicknameIndex:
    def __init__(self, rows: list[dict]):
        self.map = {r["nickname"].lower(): r["canonical"].lower() for r in rows}

    def root(self, name: str | None) -> str | None:
        if not name:
            return None
        n = name.lower()
        return self.map.get(n, n)


def name_similarity(a: str, b: str, nicknames: NicknameIndex | None = None) -> float:
    """Jaro-Winkler, but 'Bob' vs 'Robert' scores as an exact match via nickname roots."""
    if not a or not b:
        return 0.0
    if nicknames and nicknames.root(a) == nicknames.root(b):
        return 1.0
    return jaro_winkler(a, b)


def token_jaccard(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    ta, tb = set(a.upper().split()), set(b.upper().split())
    return len(ta & tb) / len(ta | tb)


def date_similarity(a: str, b: str) -> float:
    """1.0 equal | 0.9 day/month transposed | 0.5 same year | 0.0 different."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    try:
        da, db = date.fromisoformat(a), date.fromisoformat(b)
    except ValueError:
        return 0.0
    if da.year == db.year and da.month == db.day and da.day == db.month:
        return 0.9
    return 0.5 if da.year == db.year else 0.0


def exact(a, b) -> float:
    if a is None or b is None or a == "" or b == "":
        return 0.0
    return 1.0 if str(a).lower() == str(b).lower() else 0.0


@lru_cache(maxsize=200_000)
def soundex(name: str) -> str | None:
    if not name:
        return None
    name = "".join(c for c in name.upper() if c.isalpha())
    if not name:
        return None
    codes = {**dict.fromkeys("BFPV", "1"), **dict.fromkeys("CGJKQSXZ", "2"), **dict.fromkeys("DT", "3"),
             "L": "4", **dict.fromkeys("MN", "5"), "R": "6"}
    out, last = name[0], codes.get(name[0], "")
    for c in name[1:]:
        code = codes.get(c, "")
        if code and code != last:
            out += code
        if c not in "HW":
            last = code
    return (out + "000")[:4]
