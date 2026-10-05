"""Identity-resolution accuracy against the synthetic ground truth.

Pairwise metrics over all eligible source records:
  precision = correctly merged pairs / all merged pairs     (false merges hurt this)
  recall    = correctly merged pairs / all true pairs       (missed duplicates hurt this)
In MDM a false merge is usually worse than a missed one (it mixes two
customers' data), which is why thresholds favour precision and route the grey
zone to stewards.
"""
from __future__ import annotations

from collections import defaultdict
from itertools import combinations

import pandas as pd


def _pairs(groups: dict) -> set:
    out = set()
    for members in groups.values():
        out.update(combinations(sorted(members), 2))
    return out


def evaluate(xref: pd.DataFrame, truth: pd.DataFrame) -> dict:
    truth = truth.drop_duplicates(["source_system", "source_record_id"])
    truth["record_key"] = truth["source_system"] + ":" + truth["source_record_id"].astype(str)
    t = dict(zip(truth["record_key"], truth["person_id"]))
    keys = [k for k in xref["record_key"] if k in t]
    pred_g, true_g = defaultdict(list), defaultdict(list)
    g = dict(zip(xref["record_key"], xref["golden_id"]))
    for k in keys:
        pred_g[g[k]].append(k)
        true_g[t[k]].append(k)
    pp, tp_pairs = _pairs(pred_g), _pairs(true_g)
    tp = len(pp & tp_pairs)
    precision = tp / len(pp) if pp else 1.0
    recall = tp / len(tp_pairs) if tp_pairs else 1.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "records_evaluated": len(keys),
        "true_entities": len(true_g),
        "golden_records": len(pred_g),
        "predicted_pairs": len(pp),
        "true_pairs": len(tp_pairs),
        "true_positive_pairs": tp,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "f1": round(f1, 4),
        "false_merges": sorted(pp - tp_pairs)[:20],
        "missed_matches": sorted(tp_pairs - pp)[:20],
    }
