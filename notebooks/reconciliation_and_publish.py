# Databricks notebook source
# MAGIC %md
# MAGIC ## Reconciliation & publish (last task of the MDM job)
# MAGIC Reuses `ReconciliationEngine` from the package, writes the report to `mdm.ops`, raises on breaks
# MAGIC (fails the task -> job alerting), and syncs changed golden records to the serving store.

# COMMAND ----------
import json
from mdm.observability.metrics import Metrics
from mdm.reconciliation.engine import ReconciliationEngine

catalog = dbutils.widgets.get("catalog")  # noqa: F821
run_id = dbutils.widgets.get("run_id")    # noqa: F821

# COMMAND ----------
counts = {r.source: {} for r in spark.sql(f"SELECT DISTINCT source FROM {catalog}.ops.stage_counts WHERE run_id='{run_id}'").collect()}  # noqa: F821
for r in spark.sql(f"SELECT source, stage, cnt FROM {catalog}.ops.stage_counts WHERE run_id='{run_id}'").collect():  # noqa: F821
    counts[r.source][r.stage] = r.cnt
silver = spark.table(f"{catalog}.silver.party").select("record_key", "_dq_status").toPandas()   # noqa: F821  (keys only)
xref = spark.table(f"{catalog}.gold.xref").select("record_key", "golden_id").toPandas()         # noqa: F821
golden = spark.table(f"{catalog}.gold.golden_record").select("golden_id").toPandas()            # noqa: F821
events = spark.table(f"{catalog}.gold.golden_events").where(f"run_id='{run_id}'").toPandas()    # noqa: F821
prev = spark.sql(f"SELECT count(*) FROM {catalog}.gold.golden_record VERSION AS OF (SELECT max(version)-1 FROM (DESCRIBE HISTORY {catalog}.gold.golden_record))").first()[0]  # noqa: F821
report = ReconciliationEngine(Metrics(run_id)).run(list(counts), counts, silver, xref, golden, events, prev, run_id)
spark.createDataFrame([(run_id, json.dumps(report, default=str))], "run_id string, report string") \
     .write.mode("append").saveAsTable(f"{catalog}.ops.reconciliation_reports")  # noqa: F821
if report["status"] != "BALANCED":
    raise RuntimeError(f"Reconciliation break: {report['exceptions']}")

# COMMAND ----------
# Publish: push changed golden records (Delta Change Data Feed) to Azure SQL serving DB for the REST API.
changes = (spark.read.format("delta").option("readChangeFeed", "true")  # noqa: F821
           .option("startingVersion", 0).table(f"{catalog}.gold.golden_record")
           .where(f"updated_run_id = '{run_id}' AND _change_type IN ('insert','update_postimage')"))
(changes.drop("_change_type", "_commit_version", "_commit_timestamp").write.format("jdbc")
 .option("url", dbutils.secrets.get("kv-mdm", "serving-sql-url"))  # noqa: F821
 .option("dbtable", "mdm.golden_record_staging").mode("overwrite").save())
# a stored procedure then MERGEs staging -> mdm.golden_record (idempotent)
