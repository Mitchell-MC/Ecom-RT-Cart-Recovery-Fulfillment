"""Batch ingestion: order-domain CSV extracts (customers, products, inventory, orders,
order_items, shipments) -> bronze.* Delta tables.

Source files are produced by data_generation/generate_orders_domain.py landing at
`{bronze_container}/raw/orders/{table}.csv`, mirroring a nightly OMS export. Each table is
idempotently MERGEd into its bronze Delta table on a natural/composite key so re-running the job
(retry, backfill) never duplicates rows -- a batch-ingestion equivalent of the streaming job's
checkpoint-based exactly-once behavior. Runs on the `job_default` cluster policy as a scheduled
Databricks Workflow task (see orchestration/databricks/resources/order_domain_ingest_job.yml),
targeting the <30min bronze freshness SLA in docs/project-charter.md.

Idempotency depends on the source being unique per merge key, which a MERGE cannot enforce on its
own: duplicate source rows crash a MERGE ("multiple source rows matched") but insert silently on
the first-run saveAsTable path, so the guarantee would be broken from day one in exactly the case
no one notices. Both paths therefore dedupe first -- see dedupe_on_keys. Null merge keys are
rejected outright before either path runs -- see validate.

Usage (as a Databricks job task):
    spark-submit bronze_orders_domain.py --env=dev --storage_suffix=dv01
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
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

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(_THIS_DIR, "../../common"))
sys.path.append(os.path.join(_THIS_DIR, "../../quality"))
from audit import job_run, log_dataset_metrics  # noqa: E402
from config import get_config  # noqa: E402
from reconciliation import assert_dedupe_rate_ok, assert_rows_conserved  # noqa: E402
from volume import assert_volume_plausible, trailing_baseline  # noqa: E402


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
        # Supplying a schema makes the reader bind columns by position and ignore the header,
        # so a column added or reordered upstream would land data in the wrong fields -- some
        # nulling out, adjacent strings swapping silently. enforceSchema=false validates the
        # header against the schema and fails the read when they diverge.
        .option("enforceSchema", "false")
        .schema(spec.schema)
        .load(f"{raw_path}/{spec.name}.csv")
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_run_id", F.lit(_current_run_id(spark)))
    )


def dedupe_on_keys(source: DataFrame, merge_keys: tuple[str, ...]) -> DataFrame:
    """Keep one row per merge key, preferring the last occurrence in file order.

    Every row in a batch shares the same _bronze_ingested_at (it's current_timestamp()), so
    ordering by it would tie-break arbitrarily. File order is the meaningful signal in a
    flat OMS export -- a later line for the same key supersedes an earlier one.
    """
    ordering = F.monotonically_increasing_id()
    window = Window.partitionBy(*merge_keys).orderBy(ordering.desc())
    return (
        source.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def validate(source: DataFrame, spec: TableSpec) -> int:
    """Return the source row count, failing the load if any merge key is null.

    The read schema marks merge-key columns non-nullable, but Spark's CSV reader does not
    enforce nullability on read -- a blank field parses to null and passes straight through.
    Null keys are corrosive on both write paths: the MERGE condition uses <=> (null-safe),
    so every null-keyed source row matches every null-keyed target row, and dedupe_on_keys
    collapses them all into one. A malformed export would quietly delete rows rather than
    fail, which is exactly the class of failure this job should refuse to survive.

    This is one aggregation pass, replacing the source.count() that used to run after the
    merge purely for logging -- so it costs no extra scan.
    """
    aggs = [F.count(F.lit(1)).alias("_total")]
    aggs += [
        F.sum(F.col(k).isNull().cast("long")).alias(f"_null_{k}")
        for k in spec.merge_keys
    ]
    row = source.agg(*aggs).collect()[0]

    offenders = {
        k: row[f"_null_{k}"]
        for k in spec.merge_keys
        if row[f"_null_{k}"] not in (0, None)
    }
    if offenders:
        detail = ", ".join(f"{k}={n}" for k, n in offenders.items())
        raise ValueError(
            f"{spec.name}: null values in merge key column(s) ({detail}) out of "
            f"{row['_total']} source rows; refusing to merge on a key that would "
            f"collapse distinct rows"
        )
    return row["_total"]


def describe_last_write(spark: SparkSession, target_table: str) -> str:
    """Summarize what the last commit actually wrote, from the Delta transaction log.

    Reporting the source row count implies every row landed, which is wrong for a MERGE
    where most rows are no-op updates. The log has the real numbers and costs no scan.
    """
    row = (
        DeltaTable.forName(spark, target_table)
        .history(1)
        .select("operation", "operationMetrics")
        .collect()[0]
    )
    metrics = row["operationMetrics"] or {}
    if row["operation"] == "MERGE":
        return (
            f"inserted {metrics.get('numTargetRowsInserted', '?')}, "
            f"updated {metrics.get('numTargetRowsUpdated', '?')}"
        )
    return f"wrote {metrics.get('numOutputRows', '?')} rows"


def merge_into_bronze(
    spark: SparkSession,
    source: DataFrame,
    target_table: str,
    merge_keys: tuple[str, ...],
) -> int:
    """Merge `source` into the bronze table, returning the number of rows actually written.

    The caller subtracts this from the source count to get what dedupe dropped. Rows removed
    here are removed on purpose, but only reconciling them makes the difference between "3
    restated rows" and "the extract repeated half its contents" visible.
    """
    # Applied to both paths: MERGE rejects duplicate source rows loudly, but saveAsTable would
    # accept them silently and leave run 2's MERGE to crash on the mess.
    deduped = dedupe_on_keys(source, merge_keys)
    # Cached because it is consumed twice -- once by this count, once by the write. Without it
    # the dedupe window is recomputed, which is the most expensive step in this job.
    deduped.cache()
    written_rows = deduped.count()

    try:
        if not spark.catalog.tableExists(target_table):
            deduped.write.format("delta").saveAsTable(target_table)
            return written_rows

        target = DeltaTable.forName(spark, target_table)
        condition = " AND ".join(f"target.{k} <=> source.{k}" for k in merge_keys)
        (
            target.alias("target")
            .merge(deduped.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        return written_rows
    finally:
        deduped.unpersist()


def _current_run_id(spark: SparkSession) -> str:
    # Databricks Workflows exposes the run id via a Spark conf set on the job cluster; falls
    # back to "manual" for interactive/local runs so the column is never null.
    return spark.conf.get("spark.databricks.job.runId", "manual")


def main():
    spark = SparkSession.builder.appName("bronze_orders_domain").getOrCreate()
    cfg = get_config()
    raw_path = f"{cfg.container_path('bronze')}/raw/orders"

    with job_run(
        spark,
        cfg,
        job_name="bronze_orders_domain",
        layer="bronze",
        target_table=cfg.schema("bronze") + ".*",
    ) as run:
        total_rows = 0
        for spec in TABLE_SPECS:
            dataset = f"bronze.{spec.name}"
            source = read_source(spark, raw_path, spec)
            source_rows = validate(source, spec)
            target_table = cfg.table("bronze", spec.name)

            # Read before the write, so the baseline is this dataset's history *excluding*
            # the run being judged -- otherwise a load is partly compared against itself.
            baseline = trailing_baseline(spark, cfg, dataset)

            written_rows = merge_into_bronze(
                spark, source, target_table, spec.merge_keys
            )
            deduped_rows = source_rows - written_rows

            log_dataset_metrics(
                spark,
                cfg,
                dataset=dataset,
                layer="bronze",
                source_rows=source_rows,
                written_rows=written_rows,
                deduped_rows=deduped_rows,
            )

            # Metrics are written before the assertions so a failing load still records what it
            # saw -- the row counts are the first thing anyone will want during triage, and a
            # run that fails without leaving them behind forces a manual re-read of the source.
            assert_rows_conserved(
                dataset,
                source_rows=source_rows,
                written_rows=written_rows,
                deduped_rows=deduped_rows,
            )
            assert_dedupe_rate_ok(dataset, source_rows, deduped_rows)
            assert_volume_plausible(baseline, written_rows)

            total_rows += written_rows
            # Recorded per table so a mid-loop failure still shows how far the run got.
            run.row_count = total_rows
            print(
                f"{dataset}: {source_rows} source rows, {deduped_rows} deduped -> "
                f"{target_table} ({describe_last_write(spark, target_table)})"
            )


if __name__ == "__main__":
    main()
