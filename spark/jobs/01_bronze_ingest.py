"""BRONZE: land every source into an append-only Delta table with technical metadata.

* Files (CSV/JSON): Databricks Auto Loader -> exactly-once file tracking via the
  checkpoint, schema inference + evolution with `rescue` (unknown columns go to
  `_rescued_data`, never dropped), malformed rows captured via badRecordsPath.
* Database: JDBC extract with a high-water mark stored in mdm.ops.watermarks.
* REST API: driver-side paginated client (same RestApiReader as local) -> DataFrame.
"""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from common import args, load_config, spark_session
from mdm.ingestion.readers import RestApiReader

a = args()
spark = spark_session("mdm-bronze-ingest")
cfg = load_config(a.config_root)


def with_metadata(df, source, pk_cols):
    payload_cols = [c for c in df.columns if c not in ("_rescued_data", "_metadata")]
    return (df
            .withColumn("_source_system", F.lit(source))
            .withColumn("_source_record_id", F.concat_ws("|", *[F.col(c).cast("string") for c in pk_cols]))
            .withColumn("_ingest_run_id", F.lit(a.run_id))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path") if "_metadata" in df.columns else F.lit(None))
            .withColumn("_payload", F.to_json(F.struct(*payload_cols)))
            .withColumn("_record_hash", F.substring(F.sha2(F.col("_payload"), 256), 1, 20))
            .select("_source_system", "_source_record_id", "_ingest_run_id", "_ingest_ts", "_source_file",
                    "_record_hash", "_payload", "_rescued_data"))


for src in cfg.sources:
    target = f"{a.catalog}.bronze.{src.name}"
    if src.format in ("csv", "json"):
        stream = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", src.format)
                  .option("cloudFiles.schemaLocation", f"{a.checkpoints}/{src.name}/schema")
                  .option("cloudFiles.schemaEvolutionMode", src.schema_evolution if src.schema_evolution != "fail" else "failOnNewColumns")
                  .option("rescuedDataColumn", "_rescued_data")
                  .option("header", "true")
                  .option("badRecordsPath", f"{a.checkpoints}/{src.name}/bad_records")      # -> dead-letter
                  .load(f"{a.landing}/{src.location.rsplit('/', 1)[0]}")
                  .select("*", "_metadata"))
        (with_metadata(stream, src.name, src.primary_key)
         .filter(F.col("_source_record_id") != "")                                              # missing PK -> DLQ below
         .writeStream.format("delta")
         .option("checkpointLocation", f"{a.checkpoints}/{src.name}/bronze")
         .trigger(availableNow=True)                                                             # batch semantics, streaming bookkeeping
         .toTable(target)
         .awaitTermination())

    elif src.format == "database":
        wm = spark.sql(f"SELECT max(value) FROM {a.catalog}.ops.watermarks WHERE source = '{src.name}'").first()[0]
        col = src.incremental["column"]
        query = f"(SELECT * FROM {src.raw['table']} {'WHERE ' + col + ' > ' + repr(wm) if wm else ''}) q"
        df = (spark.read.format("jdbc")
              .option("url", dbutils.secrets.get("kv-mdm", f"{src.name}-jdbc-url"))  # noqa: F821 (Databricks global)
              .option("dbtable", query)
              .option("fetchsize", 10000)
              .option("numPartitions", 16).option("partitionColumn", src.primary_key[0])
              .load()
              .withColumn("_rescued_data", F.lit(None).cast("string")))
        with_metadata(df, src.name, src.primary_key).write.mode("append").saveAsTable(target)
        new_wm = df.agg(F.max(col)).first()[0]
        if new_wm:   # commit watermark only after the bronze write succeeded
            spark.sql(f"MERGE INTO {a.catalog}.ops.watermarks t USING (SELECT '{src.name}' source, '{new_wm}' value) s "
                      "ON t.source = s.source WHEN MATCHED THEN UPDATE SET value = s.value WHEN NOT MATCHED THEN INSERT *")

    elif src.format == "rest_api":
        # API volumes are small and rate-limited: paginate on the driver with retry/backoff, then parallel write
        class _State:      # adapter: watermarks from the ops table
            def __init__(self, wm): self._wm = wm
            def watermark(self, _): return self._wm
        wm = spark.sql(f"SELECT max(value) FROM {a.catalog}.ops.watermarks WHERE source = '{src.name}'").first()[0]
        res = RestApiReader(src, None, _State(wm), None, cfg.get("ingestion", {})).read()
        rows = [json.dumps(r.payload) for r in res.records]
        if rows:
            df = spark.read.json(spark.sparkContext.parallelize(rows)).withColumn("_rescued_data", F.lit(None).cast("string"))
            with_metadata(df, src.name, src.primary_key).write.mode("append").saveAsTable(target)

    # control totals for reconciliation R1
    spark.sql(f"""INSERT INTO {a.catalog}.ops.stage_counts
                  SELECT '{a.run_id}', '{src.name}', 'bronze', count(*) FROM {target} WHERE _ingest_run_id = '{a.run_id}'""")
