"""Shared helpers for Databricks jobs."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from mdm.config import PlatformConfig


def args():
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", default="mdm")
    p.add_argument("--config-root", default="/Volumes/mdm/config/current")   # UC volume holding config/*.yaml
    p.add_argument("--landing", default="abfss://landing@stmdmprod.dfs.core.windows.net")
    p.add_argument("--checkpoints", default="abfss://lakehouse@stmdmprod.dfs.core.windows.net/_checkpoints")
    p.add_argument("--run-id", required=True)                                  # {{job.run_id}} from the workflow
    return p.parse_args()


def spark_session(app: str) -> SparkSession:
    spark = SparkSession.builder.appName(app).getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "auto")      # AQE picks partition count
    return spark


def load_config(root: str) -> PlatformConfig:
    return PlatformConfig.load(root)
