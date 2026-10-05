"""Optional: near-real-time CDC / event ingestion from Azure Event Hubs (Kafka endpoint).

Upstream systems (or Debezium on their OLTP DB) publish change events:
  {"op": "c|u|d", "source": "crm", "key": "C100123", "ts": "...", "after": {...}}
They land in the same bronze tables as batch data, so silver/gold logic is
unchanged - batch and streaming share one code path (Kappa-friendly).
Deletes ('d') are written as tombstones; silver marks the record inactive and
survivorship re-evaluates the golden record (GDPR erasure flows use the same path).
"""
from __future__ import annotations

from pyspark.sql import functions as F

from common import args, spark_session

a = args()
spark = spark_session("mdm-stream-ingest")
eh_conn = dbutils.secrets.get("kv-mdm", "eventhub-connection-string")  # noqa: F821

events = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "evh-mdm-prod.servicebus.windows.net:9093")
          .option("subscribe", "party-changes")
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.mechanism", "PLAIN")
          .option("kafka.sasl.jaas.config",
                  f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{eh_conn}";')
          .option("startingOffsets", "latest")
          .load())

parsed = (events.select(F.col("value").cast("string").alias("v"), F.col("timestamp"))
          .select(F.from_json("v", "op string, source string, key string, ts string, after string").alias("e"), "timestamp")
          .select("e.*", "timestamp"))

bronze = parsed.select(
    F.col("source").alias("_source_system"), F.col("key").alias("_source_record_id"),
    F.lit(a.run_id).alias("_ingest_run_id"), F.col("timestamp").alias("_ingest_ts"), F.lit("eventhub:party-changes").alias("_source_file"),
    F.substring(F.sha2(F.col("after"), 256), 1, 20).alias("_record_hash"), F.col("after").alias("_payload"),
    F.lit(None).cast("string").alias("_rescued_data"), F.col("op").alias("_cdc_op"))

(bronze.writeStream.format("delta")
 .option("checkpointLocation", f"{a.checkpoints}/stream/party-changes")
 .trigger(processingTime="1 minute")
 .toTable(f"{a.catalog}.bronze.party_events"))
