# Technology stack, and why

The platform has two runtimes that share **one codebase**:

| Layer | Local demo (this repo, runs on a laptop) | Production (Azure) |
|---|---|---|
| Language / engine | Python 3.10+ and pandas | PySpark on **Azure Databricks** (same `mdm` package as a wheel) |
| Landing / storage | Local folders + Parquet or JSONL | **ADLS Gen2** + **Delta Lake** |
| Table format / catalog | `LakeStore` (versioned files) | **Delta Lake** tables in **Unity Catalog** |
| Ingestion | Format readers (CSV, JSON, DB, REST) | **Azure Data Factory** (movement) + **Auto Loader** / JDBC / REST (bronze) |
| Streaming (optional) | not needed for the demo | **Azure Event Hubs** (Kafka API) + Structured Streaming |
| Orchestration | `Pipeline` class, CLI | **ADF** schedule → **Databricks Workflows** (Asset Bundle) |
| Configuration | YAML in `config/` | Same YAML, versioned in Git, synced to a UC Volume |
| Data quality | Config-driven rule engine | Same engine via `mapInPandas`. Results go to Delta, then dashboards |
| Identity resolution | Blocking + Fellegi-Sunter + constrained clustering | Same scorer, with Spark self-join blocking and connected components |
| Serving | SQLite + FastAPI (stdlib fallback) | **Azure SQL** / Cosmos DB + FastAPI on **Container Apps** behind **API Management** |
| Lineage / catalog | OpenLineage JSON + record-level trace | **Unity Catalog lineage** + **Microsoft Purview** |
| Observability | JSON logs, metrics JSON/Prometheus, HTML dashboard | **Azure Monitor / Log Analytics**, alert rules, Workbooks / Power BI |
| Secrets / security | none (synthetic data) | **Key Vault**, managed identities, UC grants, column masks |
| IaC / CI/CD | `Makefile`, GitHub Actions | **Terraform** + **GitHub Actions** → `databricks bundle deploy` |

Everything below explains *why* each choice was made, what else was considered, and what the trade-off is.

---

## 1. Python for all business logic, with Spark for scale

**Why.** Each standardizer, DQ rule, comparator and survivorship rule is a pure Python function over one record. That has three benefits:

1. **One implementation, two runtimes.** Locally the functions run over pandas. On Databricks the *same* functions run inside `mapInPandas` / `applyInPandas`. A rule that passes a unit test on a laptop behaves the same way on 50 million rows, so logic can't drift between test and production.
2. **Testability.** Pure functions are easy to unit test. The suite runs in about 3 seconds with no cluster.
3. **Hiring and readability.** Python is what data engineers, data scientists and stewards already read.

**Why Spark in production rather than pandas, Polars or DuckDB alone?** The brief says tens of millions of records from dozens of systems. Identity resolution is a self-join, and survivorship is a group-by over the whole master, so both need distributed shuffles. Polars and DuckDB are excellent single-node engines, and DuckDB is a good fit for the serving or analytics side. But they hit memory limits on a multi-way self-join at the 10⁸-pair scale, and they don't come with the governance layer (Unity Catalog) we need.

**Trade-off.** Python UDFs are slower than native Spark expressions. We reduce the cost in three ways. Hot paths (blocking keys, hashing, exact comparisons) use native functions such as `soundex`, `sha2` and `lower`. Arrow-batched `mapInPandas` replaces row-wise UDFs. Incremental processing means we only score pairs that touch changed records.

## 2. Azure Databricks + Delta Lake (Lakehouse) rather than a classic data warehouse

**Why Delta Lake.** MDM needs things a pile of files cannot give:

| Requirement | Delta feature |
|---|---|
| Upserts into silver, xref and golden | `MERGE INTO` (ACID) |
| "What did the golden record look like last Tuesday?" | Time travel (`VERSION AS OF`) |
| SCD2 history and pushing changes downstream | Change Data Feed |
| Schema evolution without breaking readers | `mergeSchema`, plus the Auto Loader rescued-data column |
| Concurrent writers (streaming + batch) | Optimistic concurrency |
| Fast lookups by `golden_id` / `record_key` | Liquid clustering, data skipping |

**Why Databricks.** It offers managed Spark, Auto Loader (exactly-once file ingestion with schema tracking), Workflows, Unity Catalog (lineage, column masks for PII, grants) and Photon, all in one platform. Building and operating the same pieces on vanilla Spark (HDInsight or AKS) would cost more engineering time than it saves.

**Alternatives considered.**

- **Synapse dedicated SQL pool / Snowflake.** Strong for SQL analytics, but fuzzy matching and rule-engine logic in SQL is awkward to write and to test.
- **Microsoft Fabric.** A credible future target, since OneLake is also Delta. We stay on Databricks for Asset Bundles, Unity Catalog maturity and job-cluster cost control. Because the storage is Delta on ADLS, a later move doesn't require migrating data.
- **Commercial MDM suites (Informatica MDM, Reltio, Tamr).** They give stewardship UIs out of the box. The cost is licensing, a proprietary rule language and lock-in. The brief asks us to demonstrate design thinking, and a code-and-config platform keeps matching logic transparent and versioned in Git. In a real enterprise, a hybrid is common: this platform for ingestion and DQ, with a steward UI on top.

## 3. Medallion architecture (bronze, silver, gold)

| Layer | Contents | Why it exists |
|---|---|---|
| **Bronze** | Raw payload exactly as received (`_payload` JSON), plus run id, file, position, hash | Replayability: a standardization bug can be fixed and history re-processed without asking sources to re-send. It's also the audit trail. |
| **Silver** | Canonical party model, standardized, with DQ status. Current state per source record. | One clean shape for every source. Quarantined rows are kept but excluded from matching, so nothing is lost. |
| **Gold** | `golden_record`, `xref`, `match_pairs`, `survivorship_log`, `golden_events`, `golden_redirects` | The trusted master, plus every decision that produced it |

Every layer persists, so any stage can be re-run in isolation. That matters for operations at 3 a.m.

## 4. ADLS Gen2

ADLS Gen2 is cheap, effectively unlimited object storage with a hierarchical namespace (atomic directory operations, POSIX ACLs). It integrates natively with ADF, Databricks and Purview, supports lifecycle tiering (landing files go to cool storage after 30 days and archive after 180), and supports private endpoints. It is the default storage choice on Azure, so there was no real alternative to weigh.

## 5. Azure Data Factory for movement, Databricks Workflows for processing

- **ADF** owns *getting bytes into landing*. It has 90+ managed connectors and a self-hosted integration runtime for on-prem databases and SFTP behind the firewall. That integration work is the part we don't want to build ourselves.
- **Databricks Workflows** owns *transforming*. It runs a DAG with per-task retries and job clusters (autoscaling, spot with on-demand fallback). `max_concurrent_runs: 1` is set because golden-ID assignment must never race.
- **Why not ADF Data Flows for the transforms?** Their visual logic is hard to unit test, it can't reuse our Python matching code, and it's harder to code-review.
- **Why not Airflow?** It works, but it adds a scheduler to operate. ADF is managed and already the enterprise standard on Azure. The DAG is defined in a portable bundle, so moving to Airflow later would be a contained change.

## 6. Configuration-driven design (YAML)

Everything that varies between sources or changes with business policy lives in `config/`:

- **Data contracts** (`sources/*.yaml`): format, location, schema, primary key, field mapping, date formats, schema-evolution policy and trust score. **A new source needs one YAML file and no code.**
- **Standardization** (`standardization.yaml`): an ordered function chain per attribute, plus vocabularies.
- **Data quality** (`dq_rules.yaml`): rule instances drawn from a catalogue of rule types. A sandboxed `expression` type covers cross-field rules. **A new rule needs no code.**
- **Matching** (`matching.yaml`): blocking passes, comparisons (m/u), deterministic rules, vetoes and thresholds.
- **Survivorship** (`survivorship.yaml`): a rule chain per attribute, attribute groups, and source priority.

**Why YAML?** It is human-readable, diffable and reviewable in pull requests, so rule changes get the same review, CI validation (`mdm validate-config`) and audit trail as code. The engines validate config at startup and fail fast on an unknown function or rule type, which avoids runtime surprises.

**Trade-off.** Configuration that is too flexible becomes an untyped programming language. We keep it declarative: rule *types* are code (tested once), and rule *instances* are config. Expressions are deliberately restricted by an AST whitelist.

## 7. Identity resolution: blocking, then Fellegi-Sunter, then constrained clustering

The method is written up in [MATCHING.md](MATCHING.md). Why this approach rather than a black-box ML model:

- **Explainable.** Every decision stores per-attribute weights. A steward or auditor can see *why* two customers were merged, which regulators expect for KYC data.
- **Works without labels on day 1.** m/u probabilities can be set by hand and later learned with EM (for example with Splink, which implements the same model on Spark) from unlabeled pairs.
- **Precision-first.** Veto rules and constrained clustering guard against false merges, which are much more expensive in MDM than missed merges.
- **ML-ready.** The evidence vectors in `match_pairs`, together with steward decisions, are labelled training data for a future gradient-boosted classifier. The design doesn't need to change for that.

Comparators (Jaro-Winkler, Soundex, nickname roots, transposition-aware dates) are implemented in pure Python so the demo has zero native dependencies. In production, `rapidfuzz` or native Spark functions take their place behind the same names.

## 8. Serving: Azure SQL / Cosmos DB + FastAPI

Operational consumers need millisecond lookups by `golden_id`, by source key ("which golden record is CRM customer C123?") and by email or phone. A Delta table is the wrong tool for point lookups at high QPS, so changed golden records are pushed via Change Data Feed to a serving store:

- **Azure SQL** when lookups are relational and joins with `xref` are common. This is the default.
- **Cosmos DB** when global distribution or very high read throughput is needed.

**FastAPI** gives typed endpoints and free OpenAPI docs. The service runs on **Azure Container Apps** behind **API Management** (Entra ID auth, throttling, versioning). Locally the same repository class is served from SQLite, and a stdlib HTTP server is used if FastAPI isn't installed. MERGE redirects mean consumers holding an old `golden_id` are transparently pointed at the survivor.

## 9. Observability: Azure Monitor + Log Analytics

- **Structured JSON logs** with `run_id`, `stage` and `source` on every line. In Log Analytics, one KQL query joins logs, metrics and reconciliation for a run.
- **Metrics** per stage: counts, DQ pass rate, pair counts, golden lifecycle events and timings. They are exported as JSON and Prometheus text locally, and as Azure Monitor custom metrics in production.
- **Alert rules** are declared in `platform.yaml` (DQ pass rate, dead-letter rate, reconciliation break, review-queue growth) and mirrored as Azure Monitor alerts that route to Teams and PagerDuty.
- **Dead-letter queue** for any record that can't be processed, with a reason code and payload so it can be replayed.
- **Dashboards**: an HTML dashboard locally, and an Azure Workbook, Power BI or Databricks SQL dashboard in production.

## 10. Security and privacy

- **Identity.** Managed identities (ADF, Databricks) and service principals for CI. There are no keys in config: the `api.auth: key_vault:partner-api-key` entry is resolved from **Key Vault** at runtime.
- **Network.** Private endpoints for ADLS, SQL and Key Vault in production, and VNet-injected Databricks.
- **Least privilege.** Unity Catalog grants per layer: consumers read gold only, engineers modify silver.
- **PII.** Column masks on email, phone and DOB (`sql/ddl.sql`); only `mdm_pii_readers` see clear values. Purview classification. GDPR erasure flows use the CDC delete path, which marks source records and triggers re-survivorship.
- **Encryption** at rest (Microsoft-managed keys, or customer-managed keys if required) and TLS 1.2+ in transit.

## 11. Cost optimisation

- Job clusters (not all-purpose), autoscaling, and **spot instances** with on-demand fallback.
- **Incremental everything**: Auto Loader and watermarks at ingest, a hash check that skips unchanged re-sends, pair-decision reuse, and survivorship only for touched golden IDs. Daily cost scales with *change volume*, not master size.
- Blocking reduces comparisons by more than 99.8% in the demo (`match.reduction_ratio`).
- Storage lifecycle tiering, plus Delta `OPTIMIZE` and liquid clustering to keep scans small.

## 12. Why the local demo has (almost) no dependencies

A reviewer should be able to clone and run `python -m mdm demo` in under a minute. The core needs only **pandas + PyYAML**. Parquet (pyarrow) and FastAPI are used when installed, with fallbacks to JSONL and the stdlib HTTP server. The point is to prove the *design* without asking anyone to provision a cloud.
