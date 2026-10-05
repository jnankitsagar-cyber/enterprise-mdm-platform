# Enterprise Master Data Platform

**Standardization · Data Quality · Identity Resolution · Golden Records · Lineage · Reconciliation**

A configuration-driven Master Data Management (MDM) platform. It ingests party (customer) data from heterogeneous source systems, standardizes it into a canonical model, validates it, resolves duplicate identities, and produces **stable, versioned, fully traceable Golden Records** with stage-by-stage reconciliation and operational monitoring.

- **Runs locally in about 5 seconds** with only `pandas` and `PyYAML`.
- **Scales on Azure** (ADF → ADLS Gen2 → Databricks/Delta → Azure SQL/API) using the *same* Python package.

```
landing ─► BRONZE (raw + metadata) ─► SILVER (canonical, standardized, DQ) ─► GOLD (xref, golden record) ─► API
              │ DLQ / schema registry      │ field-level lineage, quarantine     │ match evidence, survivorship log
              └──────────────── reconciliation R1–R6 · metrics · alerts · dashboard ────────────────┘
```

## Quick start

```bash
pip install pandas pyyaml           # minimum; or: pip install -r requirements.txt
export PYTHONPATH=src:.             # Windows PowerShell: $env:PYTHONPATH="src;."
python -m mdm demo                  # day-1 load → day-2 incremental load → accuracy evaluation
```

Then explore:

```bash
open reports/dashboard.html                 # operations dashboard
cat reports/reconciliation_*.md             # stage-by-stage reconciliation
python -m mdm review                        # data-steward review queue
python -m mdm trace <golden_id>             # full lineage of one golden record
python -m mdm serve                         # REST API on :8000 (FastAPI if installed, else stdlib)
python -m mdm demo --simulate-truncated-file   # see a reconciliation break + critical alert
python -m unittest discover -s tests -t .   # 36 tests (or: pytest)
```

### What the demo shows

```
RUN …  status=SUCCESS  reconciliation=BALANCED                                  (day 1)
 funnel: source 811 → bronze 809 (+2 dead-lettered) → silver 808 → valid 802 → golden 415
 DQ: valid=800 warning=3 quarantined=6
 matching: candidate_pairs=618 (99.8% of comparisons avoided) auto_match=541 review=4

RUN …  status=SUCCESS  reconciliation=BALANCED                                  (day 2, incremental)
 schema drift detected new_columns=['loyalty_tier'] policy=rescue schema_version=2
 transient failure, retrying … HTTP 503 (simulated)  → recovered
 96 new/changed source records, 15 unchanged re-sends skipped, 557 pair decisions reused
 golden: 452 records, create=38 update=2 merge=1 (bridging record) changed_versions=63

Identity-resolution accuracy vs ground truth: precision 1.000  recall 0.973  F1 0.986
```

| Demonstration item (from the brief) | Where |
|---|---|
| raw input | `data/landing/` (CSV, nested JSON, SQLite DB, paginated API pages) |
| standardized output | `lakehouse/silver/party/` · `trace` → `transformations` |
| validation failures | `lakehouse/silver/dq_results/`, `reports/dq_scorecard_*.csv`, dashboard |
| duplicate detection | `lakehouse/gold/match_pairs/` (with evidence), `review_queue/` |
| Golden Record creation | `lakehouse/gold/golden_record/`, `survivorship_log/`, `golden_events/` |
| reconciliation metrics | `reports/reconciliation_<run>.md / .json`, dashboard |

## Repository layout

```
config/                     ← ALL behaviour lives here (reviewed + CI-validated)
  platform.yaml               paths, storage, retries, alert rules
  sources/*.yaml              data contracts: format, schema, PK, mapping, evolution policy, trust
  standardization.yaml        function chain per canonical attribute, vocabularies
  dq_rules.yaml               18 rules across completeness, validity, integrity, uniqueness, consistency, timeliness
  matching.yaml               blocking passes, Fellegi-Sunter m/u, deterministic rules, vetoes, thresholds
  survivorship.yaml           rule chain per attribute, attribute groups, source priority
  reference/                  countries, US states, calling codes, nicknames
src/mdm/
  ingestion/                  readers (csv/json/db/api), incremental state, schema registry, bronze
  standardization/            function registry + engine (field-level lineage)
  quality/                    rule types, sandboxed expressions, engine, scorecard
  matching/                   blocking, comparators, scorer, constrained clustering, persistent golden IDs
  survivorship/               rule catalogue + engine (versioning, decision log)
  lineage/                    OpenLineage events + record-level trace
  reconciliation/             R1–R6 checks, exception playbook, reports
  observability/              JSON logging, metrics/alerts, retry, dead-letter queue, dashboard
  storage/lake.py             medallion store (Delta in prod)
  serving/                    repository + FastAPI / stdlib REST API
  pipeline.py · cli.py · evaluation.py
data_generator/generate.py   synthetic multi-source data with labelled defects + ground truth
spark/jobs/                   Databricks versions of every stage (Auto Loader, mapInPandas, MERGE, streaming)
spark/sql/ddl.sql             Unity Catalog / Delta DDL, constraints, PII column masks, grants
orchestration/                ADF pipeline + trigger, Databricks Asset Bundle (job DAG)
infra/terraform/              ADLS, Key Vault, Databricks, ADF, Log Analytics, Event Hubs
notebooks/                    reconciliation & publish task
tests/                        unit + end-to-end tests (36)
docs/                         architecture, design, tech stack, data model, matching, runbook, synthetic data
```

## Documentation

| Doc | Contents |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Azure architecture diagram, logical pipeline, components, architectural decisions, scaling path |
| [docs/TECH_STACK.md](docs/TECH_STACK.md) | **Why each technology**, alternatives considered, trade-offs |
| [docs/DESIGN.md](docs/DESIGN.md) | Each functional and non-functional requirement → design; assumptions; trade-offs; future work |
| [docs/DATA_MODEL.md](docs/DATA_MODEL.md) | ER diagram: canonical, source, golden, lineage and metadata entities |
| [docs/MATCHING.md](docs/MATCHING.md) | Matching strategy, scoring model, threshold selection, false-positive mitigation |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Alerts, reconciliation investigation playbook, steward workflow, replay |
| [docs/SYNTHETIC_DATA.md](docs/SYNTHETIC_DATA.md) | Generated defects and how each validates a capability |

## Technology stack at a glance

| Concern | Choice | Why (short version) |
|---|---|---|
| Processing | **Python** logic + **PySpark on Azure Databricks** | One implementation runs locally and at scale (`mapInPandas`); Spark handles the self-joins and shuffles that 10⁷+ records need |
| Storage | **ADLS Gen2 + Delta Lake** (Medallion) | ACID MERGE for upserts, time travel for audit, Change Data Feed for SCD2 and downstream sync, schema evolution |
| Governance | **Unity Catalog** (+ Purview) | Lineage, grants per layer, PII column masks |
| Ingestion | **ADF** + **Auto Loader** / JDBC watermark / REST; **Event Hubs** for streaming | Managed connectors and self-hosted IR; exactly-once file ingestion with rescued data; batch and streaming share bronze |
| Orchestration | **ADF** schedule → **Databricks Workflows** (Asset Bundle) | Movement vs processing separated; task retries, spot clusters, single-run guard for ID assignment |
| Rules | **YAML** config + code-defined rule types | Business changes without deployments, still reviewed and validated in CI |
| Matching | Blocking + **Fellegi-Sunter** + vetoes + constrained clustering | Explainable, works without labels, precision-first, ML-ready |
| Serving | **Azure SQL / Cosmos DB** + **FastAPI** on Container Apps + APIM | Millisecond point lookups that Delta isn't designed for; typed API with OpenAPI docs |
| Observability | JSON logs + metrics → **Azure Monitor / Log Analytics**; alert rules; dashboard | Correlated by `run_id`; declarative alerts |
| Delivery | **Terraform**, **GitHub Actions**, Docker | Reproducible environments; CI gates on config validation, tests and accuracy |

Full reasoning: **[docs/TECH_STACK.md](docs/TECH_STACK.md)**.

## Extending the platform

| I want to… | Do this |
|---|---|
| add a source system | add `config/sources/<name>.yaml` (format, location, schema, PK, mapping) |
| add a data-quality rule | add an entry to `config/dq_rules.yaml` (catalogue type or safe `expression`) |
| change how a field is cleaned | edit its chain in `config/standardization.yaml` |
| add a new cleaning function | `@register("my_fn")` in `standardization/functions.py`, reference it in YAML |
| change matching sensitivity | edit `thresholds` / `comparisons` in `config/matching.yaml`, re-run `mdm evaluate` |
| change which value wins | edit the rule chain in `config/survivorship.yaml` |
| override a match decision | add a row to `lakehouse/_state/steward_decisions.csv` |

Run `python -m mdm validate-config` after any change. CI runs it too.
