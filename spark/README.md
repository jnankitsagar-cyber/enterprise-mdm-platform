# Spark / Databricks implementation (production path)

The local engine in `src/mdm` proves the logic; these jobs show how the **same
modules** scale to tens of millions of records on Azure Databricks.

| Local (demo)                         | Databricks (prod)                                                        |
|--------------------------------------|---------------------------------------------------------------------------|
| `IngestionEngine` + readers          | `jobs/01_bronze_ingest.py` – Auto Loader (CSV/JSON), JDBC watermark, REST |
| `jobs/05_streaming_ingest.py`        | Event Hubs (Kafka API) → bronze, Structured Streaming                     |
| `StandardizationEngine`, `DataQualityEngine` | `jobs/02_silver_standardize_dq.py` – `mapInPandas` over the same Python functions, Delta MERGE |
| `IdentityResolutionEngine`           | `jobs/03_gold_identity_resolution.py` – blocking self-joins, pandas scoring UDF, connected components |
| `SurvivorshipEngine`                 | `jobs/04_gold_survivorship.py` – `applyInPandas` per golden_id, Delta MERGE + SCD2 |
| `LakeStore` (parquet/jsonl)          | Delta tables in Unity Catalog (`sql/ddl.sql`)                             |

The package is shipped to the cluster as a wheel (`pip install .` → `dist/*.whl`,
attached by the Asset Bundle in `orchestration/databricks/databricks.yml`) and
the YAML config is read from the workspace / a UC volume – so rules change
without redeploying code.

Why `mapInPandas` / `applyInPandas` and not row-wise UDFs? Arrow-batched
transfer is 10-100x faster than pickled Python UDFs, while keeping a single
implementation of every rule (no logic drift between local tests and prod).
Hot paths (blocking keys, exact comparisons) use native Spark functions
(`soundex`, `lower`, `sha2`) so they never leave the JVM.
