# Design document

This document maps each requirement in the brief to how the platform addresses it. Technology choices are justified in [TECH_STACK.md](TECH_STACK.md). Matching is covered in depth in [MATCHING.md](MATCHING.md).

## 0. Assumptions

1. **Domain.** The master entity is a **Party (person/customer)**. The engine is domain-agnostic; other domains (product, employer, location) would be separate config sets.
2. **Sources.** Four simulated systems in four formats: CRM (CSV), ERP billing (nested JSON), e-commerce (database), partner (paginated REST). Together they stand in for "dozens".
3. **Cadence.** Daily batch is the primary mode. Near-real-time CDC is supported through the streaming path (`spark/jobs/05_streaming_ingest.py`) into the same bronze → silver → gold flow.
4. **Trust.** ERP billing is KYC-verified, so it is most trusted for legal name and DOB. Contact data is most reliable when most recent.
5. **Over-merging is worse than under-merging.** Mixing two customers' data is a privacy and regulatory incident. A missed duplicate is only a lost opportunity. Thresholds are set precision-first.
6. **Data stewards exist** to work the review queue and quarantined records.
7. **Source keys are stable** within a source system. Re-keying events would arrive as CDC deletes plus creates.

---

## 1. Data ingestion

**Strategy.** One reader per *format* (`csv`, `json`, `database`, `rest_api`) is selected by the source's data contract (`config/sources/*.yaml`). Adding a CSV source means adding a YAML file. Adding a new format (Avro, Kafka) means adding one class to `READERS`.

| Concern | Design |
|---|---|
| **Schema evolution** | The contract declares expected columns. Each source picks a policy: `rescue` (default) keeps the record and stores unknown fields in `_rescued_data`; `evolve` accepts them; `fail` rejects the batch (for strictly governed feeds). Every change is versioned in a schema registry (`_state/ingestion_state.json`, or the Auto Loader schema location in production). Bronze stores the whole raw payload, so evolution never loses data. *Demo: day-2 CRM adds `loyalty_tier`, which is rescued and creates schema version 2.* |
| **Metadata capture** | Every bronze row carries `_source_system`, `_source_record_id`, `_ingest_run_id`, `_ingest_ts`, `_source_file`, `_source_position`, `_record_hash` and `_schema_version`. Source control totals come from `.ctl` manifests, `COUNT(*)` with the same predicate, or the API's `total`. |
| **Audit logging** | Each run appends a manifest (`lakehouse/_runs.jsonl`: status, timings, metrics, alerts) and writes JSON logs with `run_id`, `stage` and `source`. OpenLineage events are emitted per stage. |
| **Incremental loading** | Files use a processed-file manifest keyed by name and checksum (Auto Loader semantics; a re-dropped *changed* file is reprocessed). Databases use a high-water mark (`WHERE modified_ts > :wm`). APIs use `updated_since`. Watermarks are committed **only after** the bronze write succeeds. Unchanged re-sends are detected by content hash and skipped before DQ. |
| **Error handling** | *Record-level* problems (malformed CSV row, missing PK, unparseable JSON file) go to the **dead-letter queue** with a reason code and payload, so they can be replayed. *Transient* failures (HTTP 503/429, timeouts) get **retry with exponential backoff and jitter** (demo: the first partner-API call fails and the retry recovers). *Source-level* failures are isolated (bulkhead): one broken source doesn't stop the others, and it is counted and alerted. *Contract breaches* under the `fail` policy stop that source only. |

## 2. Data standardization

Raw fields are mapped to the **canonical party model** through the contract's `mapping`, which uses dotted paths for nested JSON. Each canonical attribute then runs an ordered **function chain** from `config/standardization.yaml`:

| Attribute | Chain (abridged) | Example |
|---|---|---|
| names | trim → strip punctuation → collapse whitespace → name-aware title case | `"  o'BRIEN "` → `O'Brien`, `MCDONALD` → `McDonald` |
| full name | `split_full_name`, `extract_name_suffix` | `"WILSON SR, JAMES"` → first `James`, last `Wilson`, suffix `SR` |
| email | lower → fix domain typos → null placeholders | `J.Doe@GMIAL.com` → `j.doe@gmail.com`; `noemail@…` → null |
| phone | E.164 using the record's country, trunk prefix, `00` prefix, extensions | `(617) 555-0142`, `07700 900123` (GB) → `+16175550142`, `+447700900123` |
| dates | **source-declared formats first**, then common formats | ecommerce `04-07-1985` (day-first) vs CRM `04/07/1985` (month-first) |
| country / state | reference-table lookup (aliases → ISO-3166 / USPS) | `U.S.A.`, `united states of america` → `US`; `Texas` → `TX` |
| postal | country-aware (restore the US leading zero, GB spacing) | `2134` → `02134` |
| address | upper case, USPS suffix abbreviations | `12 Beacon Street` → `12 BEACON ST` |
| status | **controlled vocabulary** | `A`, `1`, `Active`, `enabled` → `ACTIVE` |

**Configurable and extensible.** Functions live in a registry (`@register("name")`). Adding behaviour means registering a function once and referencing it in YAML. The engine validates config at startup. **Field-level lineage**: every step that changes a value is recorded as `{attr, fn, before, after}` in `_transformations`, and failures go to `_std_errors`. The failed value is set to null, and DQ decides whether that is fatal.

## 3. Data quality framework

**Rule types are code. Rule instances are config.** Severity determines the outcome:

- `error` → the record is **QUARANTINED**. It stays in silver but is excluded from matching.
- `warning` → the record passes, the hit is logged, and its `_dq_score` drops, which lowers its weight in survivorship.

| Required check | Rule(s) in `config/dq_rules.yaml` |
|---|---|
| mandatory attributes | `DQ001` source key, `DQ002` last name (`not_null`); `DQ003` contactable (`at_least_one_of` email/phone) |
| datatype | `DQ012` DOB is a date (`datatype`, raw vs standardized); `DQ013` value supplied but not standardizable |
| business rules | `DQ020` age 16–120 (`range` with relative dates); `DQ021` timestamp not in the future |
| referential integrity | `DQ040` country in reference table; `DQ041` US state valid when country = US (`reference` + `when`) |
| domain validation | `DQ030` status ∈ vocabulary (`allowed_values`); `DQ010`/`DQ011` email and E.164 format (`regex`) |
| duplicate keys | `DQ050` same source key twice in a batch (`unique`; only the older copy is flagged) |
| completeness | `DQ900` email completeness ≥ 60% per source batch (dataset-level) |
| consistency | `DQ060` US postal shape, `DQ061` phone prefix agrees with country (`expression`) |
| timeliness | `DQ070` record touched in the last 3 years (`timeliness`) |

**Adding a rule without code changes.** Append an entry to `dq_rules.yaml`, either from the catalogue (`not_null`, `regex`, `range`, `reference`, `allowed_values`, `unique`, `timeliness`, `completeness`, `row_count`, …) or as an `expression`:

```yaml
- id: DQ080
  name: gb_postcode_shape
  type: expression
  expression: "country_code != 'GB' or matches(postal_code, '[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}')"
  severity: warning
  dimension: validity
```

Expressions are parsed into an AST and checked against a **whitelist** (comparisons, boolean logic, literals, fields, `len`, `matches`, a few string methods). Imports, dunders and arbitrary calls are rejected, so stewards can write rules safely. `when:` makes any rule conditional, and `sources:` scopes it to specific systems. CI runs `mdm validate-config`, so a bad rule fails the pull request, not production.

**Outputs.** `_dq_status`, `_dq_score` and `_dq_failures` per record; `silver.dq_results` (one row per hit, the steward work queue); `dq_dataset_results`; a per-run scorecard CSV (pass rate per source × rule × dimension).

## 4. Identity resolution and deduplication

This section is a summary; see [MATCHING.md](MATCHING.md) for the full treatment.

1. **Blocking** with 5 passes (email; phone; soundex(last) + DOB; first initial + soundex(last) + postal; nickname-root(first) + last). Oversized blocks are skipped.
2. **Deterministic rules** for high-precision shortcuts (exact email + similar last name; phone + DOB + similar first name).
3. **Probabilistic Fellegi-Sunter** scoring over 7 attributes with multi-level agreement. Nulls are neutral.
4. **Veto rules** for false-positive mitigation: DOB conflict (transposition-aware), Jr/Sr suffix conflict, first-name conflict (households sharing an email), and same-source strong conflict.
5. **Thresholds.** p ≥ 0.95 → auto-merge. 0.60–0.95 → steward review. Name-only agreement is capped at review.
6. **Constrained clustering.** Union-find in descending score order refuses unions that would put vetoed records together (this stops chaining) or exceed the maximum cluster size.
7. **Persistent golden IDs** with CREATE / UPDATE / MERGE / SPLIT / RETIRE events and redirects.
8. **Steward overrides** (`_state/steward_decisions.csv`) beat the model and are never re-scored.

## 5. Golden record management (survivorship)

Survivorship is **attribute-level** and driven by **rule chains** (`config/survivorship.yaml`). Null and DQ-failed values are removed first, which gives the non-null preference. Each rule narrows the candidates, and the first rule that leaves a single distinct value decides. The final tie-break is deterministic, so re-runs are idempotent.

| Attribute | Chain | Why this is appropriate |
|---|---|---|
| first / last name | `source_priority` → `highest_confidence` → `most_frequent` | Legal name is best from the KYC'd billing system. Confidence = source trust × record DQ score. |
| date of birth | `most_frequent` → `source_priority` | One mistyped DOB shouldn't win. Agreement across systems is the strongest signal. |
| email, phone | `most_recent` → `highest_confidence` | Contact points change. The latest update is most likely current. |
| status | `most_recent` → `source_priority` | Lifecycle status is time-dependent. |
| **address (group)** | `most_complete` → `most_recent` → `source_priority` | Street, city, state, postal and country survive **together from one record**, so you never get a street from one system and a postcode from another. |

Every decision is logged in `gold.survivorship_log`: winner record, deciding rule, all candidates with values, timestamps and confidence. Golden records are **versioned**. A content-hash change bumps `version` and appends to `golden_record_history` (SCD2). Each record carries `attribute_confidence` and `match_confidence` (the weakest link in the cluster), so consumers can apply their own trust thresholds.

## 6. Lineage and traceability

`python -m mdm trace <golden_id>` (or `GET /golden-records/{id}/lineage`) returns:

| Requirement | Where it comes from |
|---|---|
| original source records | `xref` → silver `_raw` + `_source_file`, `_source_position`, `_ingest_run_id`, `_record_hash` (which leads to the bronze row) |
| transformations applied | silver `_transformations` (`attr`, `fn`, `before`, `after`) and `_std_errors` |
| validation failures | silver `_dq_failures` + `silver.dq_results` |
| merge decisions | `gold.match_pairs`: probability, method (deterministic / probabilistic / veto / steward), per-attribute evidence and weights, blocking passes |
| survivorship decisions | `gold.survivorship_log`: winner, deciding rule, alternatives |
| lifecycle | `gold.golden_events` (CREATE/UPDATE/MERGE/SPLIT/RETIRE) and `golden_record_history` |

Dataset-level lineage is emitted as **OpenLineage** events (`lakehouse/_lineage/`). These are consumed natively by Unity Catalog lineage and Purview in production.

## 7. Reconciliation

Every record is accounted for at every stage, for every source, on every run:

| Check | Equation |
|---|---|
| R1 source vs ingestion | control total = bronze + ingest DLQ |
| R2 ingestion vs standardized | bronze = standardized + standardize DLQ |
| R3 standardized vs valid | standardized = unchanged + DQ-checked; DQ-checked = valid + warning + quarantined |
| R4 valid vs golden | every eligible silver record is in `xref` exactly once |
| R5 golden integrity | golden IDs = distinct IDs in `xref`; no orphan golden records |
| R6 golden movement | Δgolden = creates + splits − merges − retires |
| Q1 quarantine rate | per-source quarantine % above threshold (warning) |

Each break becomes an **exception** with severity, the numbers and an **investigation playbook** (see [RUNBOOK.md](RUNBOOK.md)). Errors fire the critical `reconciliation_break` alert. *Demo: `python -m mdm demo --simulate-truncated-file` makes the day-2 CRM control file declare one more record than was delivered. R1 breaks, the alert fires, and the report says where to look.*

## 8. Observability

| Capability | Implementation |
|---|---|
| monitoring / metrics | ~60 metrics per run (counts per stage and source, DQ pass rate, pairs, reuse, lifecycle events, timings) → `reports/runs/<run>/metrics.json` + `metrics.prom` |
| logging | structured JSON, correlated by `run_id`/`stage`/`source`, to stderr + `reports/logs/` |
| alerting | declarative rules in `platform.yaml`, evaluated at run end, mirrored as Azure Monitor alerts |
| retries | `@retry` with exponential backoff + jitter for transient errors only. Task-level retries in Databricks Workflows; activity retries in ADF. |
| dead-letter | `lakehouse/_dead_letter/<run>.jsonl` with stage, reason code, detail and payload. Fix and replay by dropping a corrected file. |
| dashboards | `reports/dashboard.html`: KPIs, funnel, top DQ failures, per-source reconciliation, exceptions, alerts, stage timings, review queue, run history |

## 9. Non-functional requirements

| NFR | How it is addressed |
|---|---|
| **Scalability** | Spark on Databricks. Blocking (>99.8% comparison reduction). Incremental processing everywhere. Per-golden-ID parallel survivorship. See ARCHITECTURE §5. |
| **Performance** | Arrow-batched pandas UDFs; native functions for hot paths; liquid clustering on keys; serving store for point reads |
| **Fault tolerance** | Idempotent stages (each persists output; state committed after writes); exactly-once file ingestion; retries; DLQ; bulkhead per source; Delta ACID; ADF and Workflows retries |
| **Security** | Managed identities, Key Vault, private endpoints, Unity Catalog grants per layer, API behind APIM + Entra ID |
| **Data privacy** | PII column masks (email, phone, DOB), Purview classification, synthetic data only in non-prod, erasure via the CDC delete path |
| **Extensibility** | New source = YAML; new rule = YAML; new standardizer / rule type / comparator = one registered function; new domain = new config set |
| **Configuration-driven** | All of `config/`, validated in CI |
| **Cost optimisation** | Job clusters, spot instances, autoscaling, change-proportional processing, storage tiering |
| **Cloud readiness** | Terraform (`infra/`), Asset Bundle (`orchestration/databricks`), ADF pipeline JSON, Docker image for the API, GitHub Actions CI/CD |
| **Batch + streaming** | Auto Loader `availableNow` (batch with streaming bookkeeping) and Event Hubs Structured Streaming, both landing in the same bronze tables |

## 10. Trade-offs

- **Hand-set m/u probabilities** are transparent but need tuning per population. EM estimation (Splink) is the next step.
- **Silver keeps current state per source record.** That simplifies matching; point-in-time history comes from bronze and Delta time travel.
- **A full blocking pass each run** (scoring is incremental) is cheap compared with scoring. At 10⁸ records, keep a persistent block index as well.
- **The local store is files, not Delta.** That keeps the demo dependency-free, and the interfaces mirror Delta operations.
- **Golden-ID assignment is serialised** (one concurrent run). That's correct by construction; throughput is scaled by sharding by domain or region if ever needed.

## 11. Future enhancements

1. **ML-assisted matching.** Train a gradient-boosted classifier on `match_pairs.evidence` with steward decisions as labels; use active learning to pick the most informative review pairs.
2. **EM-learned m/u** (Splink on Spark), with automatic threshold calibration from a precision target.
3. **AI-assisted DQ.** Profile new sources and *propose* `dq_rules.yaml` entries (value patterns, ranges, vocabularies) for steward approval. Use an LLM to explain review pairs in plain language.
4. **Steward UI** (Databricks App / Power Apps) over the review queue and quarantine, writing decisions back to the overrides table.
5. **GraphQL** endpoint for party hierarchies (households, employer → employee relationships).
6. **Address verification** against a postal authority API (USPS / Loqate) and geocoding.
7. **Data contracts as code** (ODCS) with producer-side CI checks, so breaking changes are caught before they ship.
8. **Real-time match-on-entry** API (search before create) using the same scorer against a blocking index.
