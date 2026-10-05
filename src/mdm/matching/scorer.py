"""Pairwise scoring: deterministic rules + Fellegi-Sunter probabilistic model + vetoes.

Scoring model
-------------
For each compared attribute we pick the agreement level (exact / high / ... /
else) and add its log-likelihood weight  w = log2(m / u):

    m = P(level | records are the same entity)
    u = P(level | records are different entities)

Missing values on either side contribute 0 (absence of evidence is not
evidence). The total weight W plus the prior log-odds is turned into a
probability:   P(match) = 1 / (1 + 2^-(W + log2(prior / (1 - prior))))

This makes every decision *explainable*: the per-attribute weights are stored
with the pair, so a steward sees exactly why two records were (not) merged.
m/u can be hand-set (config) or learned with EM on unlabeled pairs (Splink).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

from mdm.matching.comparators import (NicknameIndex, date_similarity, exact, jaro_winkler, name_similarity,
                                      token_jaccard)

MATCH, REVIEW, NO_MATCH = "MATCH", "REVIEW", "NO_MATCH"


@dataclass
class PairResult:
    a: str
    b: str
    probability: float
    weight: float
    decision: str
    method: str                                 # deterministic:<rule> | probabilistic | veto:<rule>
    blocking_passes: list[str] = field(default_factory=list)
    evidence: dict = field(default_factory=dict)
    vetoes: list[str] = field(default_factory=list)


class PairScorer:
    def __init__(self, cfg: dict, nicknames: NicknameIndex):
        self.cfg = cfg
        self.nick = nicknames
        prior = float(cfg.get("prior_match_probability", 0.01))
        self.prior_logodds = math.log2(prior / (1 - prior))
        self.t_auto = float(cfg["thresholds"]["auto_match"])
        self.t_review = float(cfg["thresholds"]["review"])
        self.requires_any = cfg["thresholds"].get("auto_match_requires_any", [])

    # ------------------------------------------------------------ comparators
    def _sim(self, method: str, a, b) -> float:
        if method == "name_similarity":
            return name_similarity(a, b, self.nick)
        if method == "jaro_winkler":
            return jaro_winkler(a, b) if a and b else 0.0
        if method == "date":
            return date_similarity(a, b)
        if method == "token_jaccard":
            return token_jaccard(a, b)
        if method == "exact":
            return exact(a, b)
        raise ValueError(method)

    @staticmethod
    def _present(v) -> bool:
        return v is not None and v == v and v != ""

    # ------------------------------------------------------------ rules
    def _deterministic(self, ra: dict, rb: dict) -> str | None:
        for rule in self.cfg.get("deterministic_rules", []):
            ok = True
            for cond in rule["all"]:
                a, b = ra.get(cond["column"]), rb.get(cond["column"])
                if not (self._present(a) and self._present(b)):
                    ok = False
                    break
                if cond["op"] == "exact":
                    ok = exact(a, b) == 1.0
                else:
                    ok = self._sim(cond["op"], a, b) >= cond.get("min", 1.0)
                if not ok:
                    break
            if ok:
                return rule["name"]
        return None

    def _vetoes(self, ra: dict, rb: dict) -> list[str]:
        hits = []
        for v in self.cfg.get("veto_rules", []):
            t = v["type"]
            if t == "dob_conflict":
                a, b = ra.get("date_of_birth"), rb.get("date_of_birth")
                if self._present(a) and self._present(b) and date_similarity(a, b) < 0.9:
                    hits.append(v["name"])
            elif t == "generational_suffix_conflict":
                a, b = ra.get("name_suffix"), rb.get("name_suffix")
                if self._present(a) and self._present(b) and a != b:
                    hits.append(v["name"])
            elif t == "first_name_conflict":
                a, b = ra.get("first_name"), rb.get("first_name")
                if self._present(a) and self._present(b) and name_similarity(a, b, self.nick) < float(v.get("max_similarity", 0.7)):
                    hits.append(v["name"])
            elif t == "same_source_conflict":
                if ra["source_system"] == rb["source_system"]:
                    diff_email = self._present(ra.get("email")) and self._present(rb.get("email")) and ra["email"] != rb["email"]
                    diff_phone = self._present(ra.get("phone")) and self._present(rb.get("phone")) and ra["phone"] != rb["phone"]
                    if diff_email and diff_phone:
                        hits.append(v["name"])
        return hits

    # ------------------------------------------------------------ score
    def score(self, ra: dict, rb: dict, passes: list[str] | None = None) -> PairResult:
        evidence, W = {}, 0.0
        agreeing = set()
        for attr, c in self.cfg["comparisons"].items():
            a, b = ra.get(attr), rb.get(attr)
            if not (self._present(a) and self._present(b)):
                evidence[attr] = {"level": "null", "weight": 0.0}
                continue
            s = self._sim(c["method"], a, b)
            chosen = None
            for lvl in c["levels"]:
                if s >= lvl["min"]:
                    chosen = lvl
                    break
            lvl = chosen or {"name": "else", **c["else"]}
            w = math.log2(lvl["m"] / lvl["u"])
            W += w
            if chosen:
                agreeing.add(attr)
            evidence[attr] = {"level": lvl["name"], "similarity": round(s, 3), "weight": round(w, 2)}

        prob = 1 / (1 + 2 ** -(W + self.prior_logodds))
        vetoes = self._vetoes(ra, rb)
        det = self._deterministic(ra, rb)

        if vetoes:
            decision, method = NO_MATCH, f"veto:{vetoes[0]}"
        elif det:
            decision, method, prob = MATCH, f"deterministic:{det}", max(prob, 0.999)
        elif prob >= self.t_auto:
            if self.requires_any and not (agreeing & set(self.requires_any)):
                decision, method = REVIEW, "probabilistic:capped_name_only"
            else:
                decision, method = MATCH, "probabilistic"
        elif prob >= self.t_review:
            decision, method = REVIEW, "probabilistic"
        else:
            decision, method = NO_MATCH, "probabilistic"
        return PairResult(ra["record_key"], rb["record_key"], round(prob, 6), round(W, 3), decision, method,
                          passes or [], evidence, vetoes)
