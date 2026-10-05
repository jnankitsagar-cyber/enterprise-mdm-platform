# Identity resolution: strategy, scoring model, thresholds, false-positive mitigation

## Matching strategy

Matching runs as a layered pipeline. Each layer is cheap, explainable and configurable in `config/matching.yaml`.

```
eligible silver records (DQ status ≠ QUARANTINED)
   │
   ├─ 1. BLOCKING ............ which pairs are worth comparing?
   ├─ 2. DETERMINISTIC RULES . obvious matches, high precision
   ├─ 3. PROBABILISTIC SCORE . Fellegi-Sunter log-likelihood → probability
   ├─ 4. VETO RULES .......... hard "these are different people" evidence
   ├─ 5. THRESHOLDS .......... MATCH / REVIEW / NO_MATCH
   ├─ 6. CONSTRAINED CLUSTERING  transitive closure without chaining
   └─ 7. PERSISTENT GOLDEN IDS   stable across runs; merges become redirects
```

**1. Blocking.** Comparing all pairs is O(n²): 10M records would be 5×10¹³ comparisons. We compare only records that share at least one key, across five independent passes:

| pass | key | catches |
|---|---|---|
| `email` | exact email | most cross-system duplicates |
| `phone` | E.164 phone | people with no or changed email |
| `lastname_dob` | soundex(last) + DOB | typo'd surnames |
| `name_postal` | first initial + soundex(last) + postal | no DOB, no shared contact |
| `canonical_firstname_lastname` | nickname-root(first) + last | Bob/Robert, Kate/Katherine |

Blocks bigger than `max_block_size` (for example a placeholder phone shared by thousands of records) are skipped and logged. Placeholders are also nulled during standardization. In the demo, blocking eliminates **99.8%** of comparisons (`match.reduction_ratio`).

**2. Deterministic rules** short-circuit for near-certain cases: `email exact AND last_name JW ≥ 0.88`, or `phone exact AND DOB exact AND first name ≥ 0.80`. Vetoes still apply on top.

## Scoring model (Fellegi-Sunter)

For each attribute we pick the highest agreement level whose similarity threshold is met, and add the weight

```
w = log2( m / u )        m = P(level | same entity)     u = P(level | different entities)
```

| attribute | comparator | levels (weight) |
|---|---|---|
| first_name | Jaro-Winkler + nickname roots | exact +5.1 · ≥0.90 +2.7 · ≥0.80 +0.3 · else −4.5 |
| last_name | Jaro-Winkler | exact +6.3 · ≥0.92 +3.9 · else −4.3 |
| date_of_birth | transposition-aware | exact +9.8 · day/month swapped +5.3 · same year −0.7 · else −5.0 |
| email | exact | +10.6 · else −2.0 |
| phone | exact (E.164) | +10.5 · else −1.7 |
| address_line1 | token Jaccard | exact +9.2 · ≥0.6 +5.3 · else −2.3 |
| postal_code | exact | +6.3 · else −2.3 |

- **Nulls contribute 0.** Absence of evidence isn't evidence, which matters because sources are sparsely populated.
- **Disagreement weights are mild for volatile attributes** (email and phone change over time, m(else)=0.25–0.30) and **strong for stable ones** (DOB, name).
- Total `W` plus prior log-odds (prior = 0.02 within a block) gives a probability: `p = 1 / (1 + 2^-(W + log2(prior/(1-prior))))`.
- Every pair stores its **evidence** (`level`, `similarity` and `weight` per attribute), so any decision can be explained to a steward or auditor.

*Where do m and u come from?* They are hand-set from domain knowledge to start. **u** is roughly the chance that two random people agree (for example, about 1 in 100 share a common surname). **m** reflects source quality: how often the same person's email differs between systems. In production they are re-estimated with EM on unlabeled pairs (Splink implements this on Spark), and steward decisions provide labelled data to calibrate them.

## Threshold selection

| zone | rule | action |
|---|---|---|
| auto-match | p ≥ **0.95** *and* at least one strong identifier (email, phone, DOB, address) agrees | merged automatically |
| review | 0.60 ≤ p < 0.95, or name-only agreement above 0.95 (`capped_name_only`), or a union refused by the cluster guard | data-steward queue (`gold.review_queue`, `python -m mdm review`) |
| non-match | p < 0.60 or any veto | kept separate |

Thresholds were chosen **precision-first** (see assumption 5 in DESIGN.md) and checked against the labelled synthetic data with `python -m mdm evaluate`. The procedure for a new population:

1. Sample pairs across the probability range and have stewards label them (or use synthetic ground truth).
2. Plot precision and recall against the threshold, and set `auto_match` at the lowest value that keeps precision ≥ 99.5%.
3. Set `review` where recall gains stop justifying the steward workload (watch the `review_queue_growing` alert).
4. Re-evaluate whenever a source is added or m/u are re-estimated. The CI demo run is a regression gate.

**Demo results (400 people, 4 sources, 2 days, about 850 records):** precision **1.000**, recall **0.973**, F1 **0.986**. The missed matches are the deliberately "stale" e-commerce accounts (old address, old email, no phone or DOB), where only the name agrees. These *should* go to a human rather than be auto-merged, and they do: they show up in the review queue as `capped_name_only`.

## False-positive mitigation

| risk | mitigation | demo scenario |
|---|---|---|
| Common names (two John Smiths) | names alone can never auto-merge (`auto_match_requires_any`); name weights calibrated for in-block frequency | random same-name pairs → review / non-match |
| Father and son at the same address and phone | **V1 DOB conflict veto** (transposition-aware, so a day/month typo isn't a conflict) | Daniel Garcia 1991 vs 1962: separate |
| Jr / Sr | **V2 generational-suffix veto**; suffix parsed out of names (`WILSON SR, JAMES`) | James Wilson Jr vs Sr: separate |
| Household sharing one email | **V4 first-name conflict veto** (Mark vs Linda Thompson) | separate despite deterministic email rule |
| Same-system distinct customers | **V3** same source, different key, different email *and* phone | – |
| Placeholder values (`noemail@`, `0000000000`) | nulled in standardization; oversized blocks skipped | placeholder email nulled |
| **Chaining** (A~B, B~C ⇒ A=C although A≠C) | **constrained union-find**: edges processed by descending probability; a union is refused if *any* cross-pair is vetoed or the cluster would exceed `max_cluster_size`; refused edges go to review | – |
| Model drift | evaluation in CI; review-queue size and merge counts monitored; steward overrides | – |
| Human error / edge cases | steward decisions (`steward_decisions.csv`) override the model permanently and act as "cannot-link" constraints in clustering | – |

## Persistent golden IDs

Downstream systems store `golden_id`, so IDs must survive re-clustering:

- A cluster containing records of one old ID keeps that ID (**UPDATE** when members are added).
- A cluster containing several old IDs is a **MERGE**. The survivor is the ID with the most members, with the lexicographically smallest winning a tie. Losers get a row in `golden_redirects`, and `GET /golden-records/<old>` returns the survivor with `_redirected_from`.
- An old ID split across clusters is a **SPLIT**. The largest piece keeps the ID.
- An ID with no remaining eligible records is **RETIRED**.

*Demo: on day 1, Katherine Johnson exists twice: CRM (email A, phone A, address X) and e-commerce ("Kate", email B, phone B, address Y, no DOB). Their match probability is about 0.15, well below the review threshold, so they stay separate. On day 2 a partner-API record arrives carrying CRM's email and e-commerce's phone and address. It links to both, the two golden records MERGE, and the old ID redirects.*

## Incremental matching

A pair whose records are both unchanged since the last run reuses its stored decision (`match.pairs_reused`). Only pairs touching changed records are re-scored, so daily cost is proportional to change volume. Steward-overridden and cluster-guarded pairs are always re-evaluated.
