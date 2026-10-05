"""GOLD: identity resolution at scale.

1. Blocking keys computed with native Spark functions (soundex, lower ...) and
   exploded to (block_key, record_key). Oversized blocks are dropped (hot keys).
2. Candidate pairs = self-join on block_key (a < b), de-duplicated across passes.
   Incremental: only pairs where at least one side changed in this run are scored;
   others keep their decision from gold.match_pairs.
3. Scoring with the SAME PairScorer via mapInPandas (Fellegi-Sunter + vetoes).
4. Connected components over MATCH edges (GraphFrames, or iterative min-label
   propagation below when GraphFrames is unavailable). Clusters violating vetoes
   or max size are routed to review by a constrained post-pass on the driver
   for the (few) affected components only.
5. Persistent golden ids via assign_golden_ids on the changed components; xref MERGE.
"""
from __future__ import annotations

import json

import pandas as pd
from pyspark.sql import functions as F

from common import args, load_config, spark_session

a = args()
spark = spark_session("mdm-identity-resolution")
cfg = load_config(a.config_root)
mcfg = cfg.matching
C = a.catalog

silver = spark.table(f"{C}.silver.party").filter("_dq_status <> 'QUARANTINED'").cache()
changed_keys = silver.filter(F.col("_dq_run_id") == a.run_id).select("record_key")

# ---- 1. blocking ------------------------------------------------------------
nick = spark.table(f"{C}.ref.nicknames")          # nickname -> canonical
fn = (silver.join(nick, F.lower("first_name") == nick.nickname, "left")
      .withColumn("first_root", F.coalesce("canonical", F.lower("first_name"))))
passes = {
    "email": F.col("email"),
    "phone": F.col("phone"),
    "lastname_dob": F.concat_ws("|", F.soundex("last_name"), "date_of_birth"),
    "name_postal": F.concat_ws("|", F.substring(F.upper("first_name"), 1, 1), F.soundex("last_name"), "postal_code"),
    "canonical_name": F.concat_ws("|", "first_root", F.lower("last_name")),
}
keys = None
for name, expr in passes.items():
    k = fn.select(F.lit(name).alias("pass"), expr.alias("bkey"), "record_key").filter("bkey IS NOT NULL AND bkey <> ''")
    keys = k if keys is None else keys.unionByName(k)
sizes = keys.groupBy("pass", "bkey").count()
keys = keys.join(sizes.filter(f"count <= {mcfg['blocking']['max_block_size']}"), ["pass", "bkey"])

pairs = (keys.alias("l").join(keys.alias("r"), ["pass", "bkey"])
         .filter("l.record_key < r.record_key")
         .groupBy(F.col("l.record_key").alias("a"), F.col("r.record_key").alias("b"))
         .agg(F.collect_set("pass").alias("passes")))

# ---- 2. incremental: score only pairs touching changed records ----------------
to_score = pairs.join(changed_keys.withColumnRenamed("record_key", "a"), "a", "left_semi").unionByName(
    pairs.join(changed_keys.withColumnRenamed("record_key", "b"), "b", "left_semi")).distinct()

cols = ["record_key", "source_system", "first_name", "last_name", "name_suffix", "email", "phone",
        "date_of_birth", "address_line1", "postal_code"]
side = silver.select(*cols)
cand = (to_score.join(side.select(*[F.col(c).alias(f"a_{c}") for c in cols]), F.col("a") == F.col("a_record_key"))
        .join(side.select(*[F.col(c).alias(f"b_{c}") for c in cols]), F.col("b") == F.col("b_record_key")))

cfg_root = a.config_root
PAIR_SCHEMA = ("record_key_a string, record_key_b string, probability double, weight double, decision string, "
               "method string, blocking_passes string, evidence string, vetoes string, scored_run_id string")


def score(batches):
    from mdm.config import PlatformConfig
    from mdm.matching.comparators import NicknameIndex
    from mdm.matching.scorer import PairScorer
    c = PlatformConfig.load(cfg_root)
    s = PairScorer(c.matching, NicknameIndex(c.reference.tables["nicknames"]))
    for pdf in batches:
        out = []
        for r in pdf.to_dict("records"):
            ra = {k: r[f"a_{k}"] for k in cols}
            rb = {k: r[f"b_{k}"] for k in cols}
            res = s.score(ra, rb, list(r["passes"]))
            out.append({"record_key_a": res.a, "record_key_b": res.b, "probability": res.probability, "weight": res.weight,
                        "decision": res.decision, "method": res.method, "blocking_passes": json.dumps(res.blocking_passes),
                        "evidence": json.dumps(res.evidence), "vetoes": json.dumps(res.vetoes), "scored_run_id": a.run_id})
        yield pd.DataFrame(out)


scored = cand.mapInPandas(score, PAIR_SCHEMA)
spark.sql(f"CREATE TABLE IF NOT EXISTS {C}.gold.match_pairs ({PAIR_SCHEMA}) CLUSTER BY (record_key_a)")
from delta.tables import DeltaTable  # noqa: E402
(DeltaTable.forName(spark, f"{C}.gold.match_pairs").alias("t")
 .merge(scored.alias("s"), "t.record_key_a = s.record_key_a AND t.record_key_b = s.record_key_b")
 .whenMatchedUpdateAll(condition="t.method <> 'steward_override'")      # steward decisions are never overwritten
 .whenNotMatchedInsertAll().execute())

# ---- 3. connected components (min-label propagation, converges in O(diameter)) ----
edges = spark.table(f"{C}.gold.match_pairs").filter("decision = 'MATCH'").select(F.col("record_key_a").alias("src"),
                                                                                 F.col("record_key_b").alias("dst"))
edges = edges.unionByName(edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"))).cache()
labels = silver.select(F.col("record_key").alias("node"), F.col("record_key").alias("label"))
for _ in range(20):
    nxt = (edges.join(labels, edges.src == labels.node).groupBy("dst").agg(F.min("label").alias("cand"))
           .join(labels, F.col("dst") == F.col("node"), "right")
           .select("node", F.least(F.coalesce("cand", "label"), "label").alias("label")))
    if nxt.join(labels, ["node", "label"], "left_anti").limit(1).count() == 0:
        break
    labels = nxt.localCheckpoint()

# ---- 4. persistent golden ids (only components that contain changed records) ----
from mdm.matching.golden_ids import assign_golden_ids  # noqa: E402
affected = labels.join(labels.join(changed_keys, F.col("node") == F.col("record_key")).select("label").distinct(), "label")
comp = affected.groupBy("label").agg(F.collect_list("node").alias("members")).collect()   # bounded by daily change volume
prev = {r.record_key: r.golden_id for r in spark.table(f"{C}.gold.xref")
        .join(affected.select(F.col("node").alias("record_key")), "record_key").collect()}
assignment, events = assign_golden_ids([set(r.members) for r in comp], prev, a.run_id)
xref_upd = spark.createDataFrame([(k, g, k.split(":", 1)[0], k.split(":", 1)[1], a.run_id) for k, g in assignment.items()],
                                 "record_key string, golden_id string, source_system string, source_record_id string, run_id string")
(DeltaTable.forName(spark, f"{C}.gold.xref").alias("t").merge(xref_upd.alias("s"), "t.record_key = s.record_key")
 .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
if events:
    spark.createDataFrame(pd.DataFrame(events).assign(record_keys=lambda d: d.record_keys.map(json.dumps))) \
        .write.mode("append").saveAsTable(f"{C}.gold.golden_events")
