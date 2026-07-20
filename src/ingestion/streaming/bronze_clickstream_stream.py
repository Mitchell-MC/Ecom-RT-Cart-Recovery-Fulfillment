"""Structured Streaming ingestion: raw clickstream JSON -> bronze.clickstream_events.

Runs as a continuous Databricks Workflow job (see orchestration/databricks/resources/
bronze_job.yml) on the `streaming` cluster policy (autotermination disabled -- this task is
meant to run 24/7 and be restarted by the Workflow on failure, not to complete and shut down).

Bronze is a landing zone, not a quality gate: rows are never dropped here. Autoloader's
rescuedDataColumn catches anything that doesn't match the expected schema (bad JSON, unexpected
new fields) into `_rescued_data` instead of failing the stream, so a malformed upstream event
degrades gracefully rather than stalling ingestion. Real validation happens at the bronze->silver
boundary (src/quality/dq_checks.py), matching the contract in docs/metric-glossary.md.

Usage (as a Databricks job task):
    spark-submit bronze_clickstream_stream.py  # env + storage_suffix come from job parameters
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../common")
)
from audit import job_run  # noqa: E402
from config import get_config  # noqa: E402

CLICKSTREAM_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField(
            "event_timestamp", StringType(), False
        ),  # cast to timestamp after landing
        StructField("session_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("cart_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField(
            "_ingested_at", StringType(), True
        ),  # producer-side timestamp, renamed below
        StructField("page_type", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price_at_event", DoubleType(), True),
    ]
)


def build_stream(
    spark: SparkSession, raw_path: str, checkpoint_path: str, schema_location: str
):
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(CLICKSTREAM_SCHEMA)
        .load(raw_path)
    )

    bronze = (
        raw.withColumnRenamed("_ingested_at", "producer_ingested_at")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("producer_ingested_at", F.to_timestamp("producer_ingested_at"))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_file_mtime", F.col("_metadata.file_modification_time"))
    )
    return bronze.writeStream.option("checkpointLocation", checkpoint_path)


def main():
    spark = SparkSession.builder.appName("bronze_clickstream_stream").getOrCreate()
    cfg = get_config()

    raw_path = f"{cfg.container_path('bronze')}/raw/clickstream"
    schema_location = (
        f"{cfg.container_path('checkpoints')}/{cfg.env}/clickstream_schema"
    )
    checkpoint_path = cfg.checkpoint_path("clickstream_bronze")
    target_table = cfg.table("bronze", "clickstream_events")

    # awaitTermination re-raises whatever killed the query, so wrapping it means a stream that
    # dies at 3am leaves a "failed" row behind. Databricks auto-restarts continuous jobs, which
    # is precisely why this matters: a query that crash-loops looks identical to a healthy one
    # from the outside, and the restart count is not somewhere anyone looks.
    with job_run(
        spark,
        cfg,
        job_name="bronze_clickstream_stream",
        layer="bronze",
        target_table=target_table,
    ):
        query = (
            build_stream(spark, raw_path, checkpoint_path, schema_location)
            .trigger(
                processingTime="1 minute"
            )  # continuous micro-batch, targets the <5min bronze SLA
            .outputMode("append")
            .toTable(target_table)
        )
        query.awaitTermination()


if __name__ == "__main__":
    main()
