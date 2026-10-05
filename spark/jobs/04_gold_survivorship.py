"""GOLD: build / refresh Golden Records for golden_ids touched in this run (applyInPandas)."""
from __future__ import annotations

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import args, load_config, spark_session

a = args()
spark = spark_session("mdm-survivorship")
cfg = load_config(a.config_root)
C, cfg_root = a.catalog, a.config_root

touched = spark.table(f"{C}.gold.xref").filter(F.col("run_id") == a.run_id).select("golden_id").distinct()
members = (spark.table(f"{C}.silver.party").join(spark.table(f"{C}.gold.xref").select("record_key", "golden_id"), "record_key")
           .join(touched, "golden_id"))
prev = spark.table(f"{C}.gold.golden_record")
schema = prev.schema


def survive(pdf: pd.DataFrame) -> pd.DataFrame:
    from mdm.config import PlatformConfig
    from mdm.survivorship.engine import SurvivorshipEngine
    c = PlatformConfig.load(cfg_root)
    xref = pdf[["record_key", "golden_id"]]
    golden, _log, _hist = SurvivorshipEngine(c.survivorship).run(pdf, xref, pd.DataFrame(), pd.DataFrame(), a.run_id)
    return golden


new = members.groupBy("golden_id").applyInPandas(survive, schema=schema)
# version bump only when content changed; SCD2 history via Change Data Feed on gold.golden_record
(DeltaTable.forName(spark, f"{C}.gold.golden_record").alias("t")
 .merge(new.alias("s"), "t.golden_id = s.golden_id")
 .whenMatchedUpdate(condition="t.record_hash <> s.record_hash",
                    set={**{c: f"s.{c}" for c in new.columns if c not in ("version", "created_run_id")},
                         "version": "t.version + 1"})
 .whenNotMatchedInsertAll()
 .execute())

# merged-away ids become redirects; consumers following an old id are pointed at the survivor
spark.sql(f"""
  MERGE INTO {C}.gold.golden_redirects t
  USING (SELECT related_golden_id AS old_golden_id, golden_id AS new_golden_id, run_id
         FROM {C}.gold.golden_events WHERE event = 'MERGE' AND run_id = '{a.run_id}') s
  ON t.old_golden_id = s.old_golden_id
  WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
spark.sql(f"DELETE FROM {C}.gold.golden_record WHERE golden_id IN (SELECT old_golden_id FROM {C}.gold.golden_redirects WHERE run_id = '{a.run_id}')")
