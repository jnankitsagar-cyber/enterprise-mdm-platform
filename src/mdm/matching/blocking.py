"""Blocking: generate candidate pairs cheaply.

Comparing every record with every other record is O(n^2) - 10M records would
be 5 * 10^13 comparisons. Blocking only compares records that share a cheap
key (same email, same soundex(last name) + DOB, ...). Several independent
passes are unioned so a typo in one attribute does not lose the pair.

On Spark this is literally a self-join on the blocking key column; oversized
blocks (e.g. a placeholder phone shared by 50k records) are dropped and logged
because they generate huge numbers of low-value pairs.
"""
from __future__ import annotations

from collections import defaultdict
from itertools import combinations

from mdm.matching.comparators import NicknameIndex, soundex
from mdm.observability.logging import get_logger

log = get_logger("blocking")


def _key_value(rec: dict, spec, nick: NicknameIndex):
    if isinstance(spec, str):
        return rec.get(spec)
    v = rec.get(spec["column"])
    if not v:
        return None
    fn = spec["fn"]
    if fn == "soundex":
        return soundex(v)
    if fn == "first_char":
        return v[0].upper()
    if fn == "lower":
        return v.lower()
    if fn == "nickname_root":
        return nick.root(v)
    if fn == "year":
        return str(v)[:4]
    raise ValueError(f"unknown blocking fn {fn}")


def candidate_pairs(records: list[dict], blocking_cfg: dict, nick: NicknameIndex) -> tuple[dict, dict]:
    """Returns ({(key_a, key_b): [pass names]}, stats)."""
    max_block = int(blocking_cfg.get("max_block_size", 200))
    pairs: dict[tuple, list[str]] = defaultdict(list)
    stats = {"blocks": 0, "oversized_blocks": 0, "pairs": 0}
    for p in blocking_cfg["passes"]:
        blocks = defaultdict(list)
        for r in records:
            vals = [_key_value(r, k, nick) for k in p["keys"]]
            if any(v in (None, "") for v in vals):
                continue
            blocks[tuple(vals)].append(r["record_key"])
        for key, members in blocks.items():
            if len(members) < 2:
                continue
            stats["blocks"] += 1
            if len(members) > max_block:
                stats["oversized_blocks"] += 1
                log.warning("oversized block skipped", pass_name=p["name"], key=str(key), size=len(members))
                continue
            for a, b in combinations(sorted(members), 2):
                pairs[(a, b)].append(p["name"])
    stats["pairs"] = len(pairs)
    return pairs, stats
