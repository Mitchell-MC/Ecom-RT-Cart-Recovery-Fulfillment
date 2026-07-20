"""Batch ingestion: order-domain CSV extracts (customers, products, inventory, orders,
order_items, shipments) -> bronze.* Delta tables.

Source files are produced by data_generation/generate_orders_domain.py landing at
`{bronze_container}/raw/orders/{table}.csv`, mirroring a nightly OMS export. Each table is
idempotently MERGEd into its bronze Delta table on a natural/composite key so re-running the job
(retry, backfill) never duplicates rows -- a batch-ingestion equivalent of the streaming job's
checkpoint-based exactly-once behavior. Runs on the `job_default` cluster policy as a scheduled
Databricks Workflow task (see orchestration/databricks/resources/bronze_job.yml), targeting the
<30min bronze freshness SLA in docs/project-charter.md.

Usage (as a Databricks job task):
    spark-submit bronze_orders_domain.py  # env + storage_suffix come from job parameters
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../common")
)
from config import get_config  # noqa: E402


@dataclass(frozen=True)
class TableSpec:
    name: str
    schema: StructType
    merge_keys: tuple[str, ...]  # natural/composite key used for idempotent MERGE


TABLE_SPECS: list[TableSpec] = [
    TableSpec(
        "customers",
        StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("first_seen_date", DateType(), True),
                StructField("is_returning", BooleanType(), True),
                StructField("home_tz_offset", IntegerType(), True),
            ]
        ),
        ("customer_id",),
    ),
    TableSpec(
        "products",
        StructType(
            [
                StructField("sku", StringType(), False),
                StructField("name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("price", DoubleType(), True),
            ]
        ),
        ("sku",),
    ),
    TableSpec(
        "inventory",
        StructType(
            [
                StructField("sku", StringType(), False),
                StructField("warehouse_id", StringType(), True),
                StructField("on_hand_qty", IntegerType(), True),
                StructField("backorder_flag", BooleanType(), True),
                StructField("snapshot_date", DateType(), True),
            ]
        ),
        ("sku", "warehouse_id"),
    ),
    TableSpec(
        "orders",
        StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("cart_id", StringType(), True),
                StructField("order_created_at", TimestampType(), True),
                StructField("channel", StringType(), True),
                StructField("currency", StringType(), True),
                StructField("fx_rate_to_usd", DoubleType(), True),
                StructField("order_total_usd", DoubleType(), True),
                StructField("status", StringType(), True),
                StructField("promised_delivery_date", DateType(), True),
            ]
        ),
        ("order_id",),
    ),
    TableSpec(
        "order_items",
        StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("sku", StringType(), False),
                StructField("qty", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
            ]
        ),
        ("order_id", "sku"),
    ),
    TableSpec(
        "shipments",
        StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("carrier", StringType(), True),
                StructField("shipped_at", TimestampType(), True),
                StructField("delivered_at", TimestampType(), True),
                StructField("status", StringType(), True),
                StructField("carrier_avg_transit_days", IntegerType(), True),
                StructField("carrier_historical_late_rate_pct", DoubleType(), True),
                StructField("promised_delivery_date", DateType(), True),
            ]
        ),
        ("order_id",),
    ),
]


def read_source(spark: SparkSession, raw_path: str, spec: TableSpec) -> DataFrame:
    return (
        spark.read.format("csv")
        .option("header", "true")
        .schema(spec.schema)
        .load(f"{raw_path}/{spec.name}.csv")
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_run_id", F.lit(_current_run_id(spark)))
    )


def merge_into_bronze(
    spark: SparkSession,
    source: DataFrame,
    target_table: str,
    merge_keys: tuple[str, ...],
) -> None:
    if not spark.catalog.tableExists(target_table):
        source.write.format("delta").saveAsTable(target_table)
        return

    target = DeltaTable.forName(spark, target_table)
    condition = " AND ".join(f"target.{k} <=> source.{k}" for k in merge_keys)
    (
        target.alias("target")
        .merge(source.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _current_run_id(spark: SparkSession) -> str:
    # Databricks Workflows exposes the run id via a Spark conf set on the job cluster; falls
    # back to "manual" for interactive/local runs so the column is never null.
    return spark.conf.get("spark.databricks.job.runId", "manual")


def main():
    spark = SparkSession.builder.appName("bronze_orders_domain").getOrCreate()
    cfg = get_config()
    raw_path = f"{cfg.container_path('bronze')}/raw/orders"

    for spec in TABLE_SPECS:
        source = read_source(spark, raw_path, spec)
        target_table = cfg.table("bronze", spec.name)
        merge_into_bronze(spark, source, target_table, spec.merge_keys)
        print(
            f"bronze.{spec.name}: merged {source.count()} source rows -> {target_table}"
        )


if __name__ == "__main__":
    main()
