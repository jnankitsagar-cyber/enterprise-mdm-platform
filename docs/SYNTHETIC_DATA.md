# Synthetic dataset and how it validates the solution

`data_generator/generate.py` creates about 400 people, renders them into four source systems in their own "dialects", injects realistic defects, and writes **ground truth** (`data/ground_truth.csv`: source key → real person). The pipeline never reads the ground truth. Only `mdm evaluate` and the tests do, so accuracy is *measured*, not asserted.

| Requirement | What is generated | What it proves | Where to see it |
|---|---|---|---|
| **Duplicate records** | each person appears in 1–4 systems; 5% are keyed twice inside CRM | cross- and intra-source dedup | `golden_records` vs `valid_eligible`; dedup ratio ≈ 48% |
| **Inconsistent formatting** | 3 date formats (MM/DD, DD-MM, YYYYMMDD), 5 phone styles, `USA`/`U.S.A.`/`united states of america`, state names vs codes, `LAST, FIRST`, case/whitespace noise, `Street`/`St.`/`STREET` | standardization chains + source-specific date formats | `trace` → `transformations` |
| **Missing values** | 5–35% nulls per attribute and source, e-commerce DOB often missing | null-neutral matching, non-null survivorship, completeness rule | DQ900, evidence `level: null` |
| **Conflicting attributes** | ERP has a stale phone for 10%; nicknames (Bob/Robert); typos in surnames | fuzzy/nickname matching, survivorship (`most_recent`, `most_frequent`, `source_priority`) | `survivorship_log.candidates` |
| **Invalid values** | `not-an-email`, `31/31/1980`, future `updated_at`, placeholder `noemail@`, `test@test.com`, missing last name, no contact channel, phone `12345`, status `VIP` | DQ warnings vs quarantine; placeholders nulled | `silver.dq_results`, dashboard |
| **Broken records** | malformed CSV row (extra fields), JSON record without `account_no` | dead-letter queue; reconciliation still balances | `_dead_letter/`, R1 PASS |
| **Hard negatives** | father/son (same address and phone, different DOB), James Wilson Jr/Sr, Mark/Linda Thompson sharing an email | veto rules: precision stays 1.0 | `match_pairs.vetoes` |
| **Hard positives** | Priya Sharma with day/month-swapped DOB in e-commerce | transposition-aware comparator | merged |
| **Hard misses (realistic)** | 4% stale e-commerce accounts (old address and email, no phone or DOB) | name-only cap → steward review instead of risky auto-merge | review queue `capped_name_only` |
| **Changing source data (day 2)** | 25 CRM movers / new emails, 12 e-commerce updates (new `modified_ts`), 15 identical ERP re-sends, new people in every source | incremental load (manifest, watermarks, `updated_since`), unchanged detection, golden versioning | `standardize.unchanged_resends`, `match.pairs_reused`, `golden.changed` |
| **Schema evolution** | CRM adds `loyalty_tier` on day 2 | rescue policy, schema registry v2 | `_rescued_data`, `ingestion_state.json` |
| **Golden merge** | partner record bridges two day-1 golden records of Katherine Johnson | MERGE event + redirect, ID stability | `golden_events`, `GET /golden-records/<old id>` |
| **Transient failure** | first partner-API call returns 503 | retry with backoff | log line `transient failure, retrying` |
| **Reconciliation break** (opt-in) | `--simulate-truncated-file`: control total says one more record than delivered | R1 exception + critical alert + playbook | `reports/reconciliation_*.md` |

Typical result: **precision 1.000, recall ≈ 0.97, F1 ≈ 0.99**, both runs BALANCED. The tests in `tests/test_end_to_end.py` enforce precision ≥ 0.98 and recall ≥ 0.90 as a regression gate.
