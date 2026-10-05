"""SILVER: standardize + validate only the bronze rows that arrived in this run, then MERGE.

Uses the exact same Python engines as the local pipeline inside `mapInPandas`,
so a rule tested locally behaves identically at scale.
"""
from __future__ import annotations

import json

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import args, load_config, spark_session

a = args()
spark = spark_session("mdm-silver")
cfg = load_config(a.config_root)
cfg_root = a.config_root

SILVER_SCHEMA = """record_key string, source_system string, source_record_id string, source_trust double,
 first_name string, last_name string, full_name string, name_suffix string, email string, phone string,
 date_of_birth string, address_line1 string, city string, state_code string, postal_code string,
 country_code string, status string, source_updated_at string, _record_hash string, _ingest_run_id string,
 _source_file string, _transformations string, _std_errors string, _raw string,
 _dq_status string, _dq_score double, _dq_failures string, _dq_run_id string"""


def standardize_and_validate(batches):
    # executed on executors: build engines once per task (config is small, read from UC volume)
    from mdm.config import PlatformConfig
    from mdm.quality.engine import DataQualityEngine
    from mdm.standardization.engine import CANONICAL_ATTRIBUTES, StandardizationEngine
    c = PlatformConfig.load(cfg_root)
    std, dq = StandardizationEngine(c), DataQualityEngine(c.dq, c.reference)
    for pdf in batches:
        rows = []
        for r in pdf.to_dict("records"):
            payload = json.loads(r["_payload"])
            rec, tf, errs = std.standardize_record(r["_source_system"], payload)
            src = c.source(r["_source_system"])
            rec.update(source_system=r["_source_system"], source_record_id=r["_source_record_id"],
                       record_key=f"{r['_source_system']}:{r['_source_record_id']}", source_trust=src.trust_score,
                       _record_hash=r["_record_hash"], _ingest_run_id=r["_ingest_run_id"], _source_file=r["_source_file"],
                       _transformations=json.dumps(tf, default=str), _std_errors=json.dumps(errs, default=str),
                       _raw=json.dumps({k: payload.get(v) for k, v in src.mapping.items()}, default=str))
            rows.append(rec)
        if rows:
            out, _, _ = dq.run(pd.DataFrame(rows), a.run_id)
            yield out[[c.split()[0] for c in SILVER_SCHEMA.replace("\n", "").split(",")]]


bronze_new = spark.sql(f"""
    SELECT * FROM (
      SELECT * FROM {a.catalog}.bronze.crm UNION ALL BY NAME SELECT * FROM {a.catalog}.bronze.erp_billing
      UNION ALL BY NAME SELECT * FROM {a.catalog}.bronze.ecommerce UNION ALL BY NAME SELECT * FROM {a.catalog}.bronze.partner_api)
    WHERE _ingest_run_id = '{a.run_id}'""")

# drop unchanged re-sends early (same key + same content hash already in silver)
silver_tbl = f"{a.catalog}.silver.party"
existing = spark.table(silver_tbl).select("record_key", "_record_hash")
changed = (bronze_new
           .withColumn("record_key", F.concat_ws(":", "_source_system", "_source_record_id"))
           .join(existing, ["record_key", "_record_hash"], "left_anti")
           .drop("record_key")
           .repartition(200, "_source_record_id"))

validated = changed.mapInPandas(standardize_and_validate, schema=SILVER_SCHEMA).cache()

# DQ failures exploded into their own table (steward work queue + scorecards)
(validated.select("record_key", "source_system", F.from_json("_dq_failures", "array<map<string,string>>").alias("f"))
 .select("record_key", "source_system", F.explode("f").alias("f"))
 .select(F.lit(a.run_id).alias("run_id"), "record_key", "source_system",
         F.col("f.rule_id").alias("rule_id"), F.col("f.rule").alias("rule"), F.col("f.severity").alias("severity"),
         F.col("f.dimension").alias("dimension"), F.col("f.column").alias("column"), F.col("f.value").alias("value"))
 .write.mode("append").saveAsTable(f"{a.catalog}.silver.dq_results"))

# upsert current state; a late-arriving older version never overwrites a newer one
w = Window.partitionBy("record_key").orderBy(F.col("source_updated_at").desc())
latest = validated.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
(DeltaTable.forName(spark, silver_tbl).alias("t")
 .merge(latest.alias("s"), "t.record_key = s.record_key")
 .whenMatchedUpdateAll(condition="s.source_updated_at >= t.source_updated_at")
 .whenNotMatchedInsertAll()
 .execute())
