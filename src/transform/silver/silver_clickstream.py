"""Streaming standardization: bronze.clickstream_events -> silver.clickstream_events.

Continues the Structured Streaming pipeline from bronze (rather than dropping to batch at
silver) so the <15min bronze->silver freshness SLA in docs/project-charter.md holds end to end.
Each micro-batch is processed with foreachBatch so we can apply DQ quarantine-splitting and a
key-aware dedupe (regular DataFrame operations, including window functions) that plain streaming
dropDuplicates can't express -- see the two-key dedupe strategy below.

Partitioned by `event_date` (see docs/distributed-compute-notes.md for why date-partitioning
beats no-partitioning here, and why we deliberately do NOT also partition by cart_id/customer_id
-- cardinality is far too high and would produce a small-files problem).
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(_THIS_DIR, "../../common"))
sys.path.append(os.path.join(_THIS_DIR, "../../quality"))
from audit import job_run  # noqa: E402
from config import get_config  # noqa: E402
from dq_checks import (  # noqa: E402
    DQRule,
    apply_dq_rules,
    dedupe_last_write_wins,
    within_clock_skew,
    write_quarantine,
)

CART_EVENT_TYPES = [
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "checkout_complete",
]


def build_dq_rules() -> list[DQRule]:
    price_required = F.col("event_type").isin(["add_to_cart", "remove_from_cart"])
    cart_id_required = F.col("event_type").isin(CART_EVENT_TYPES)

    return [
        DQRule("missing_session_id", "fail", F.col("session_id").isNull()),
        DQRule(
            "missing_cart_id_for_cart_event",
            "fail",
            cart_id_required & F.col("cart_id").isNull(),
        ),
        DQRule("invalid_event_timestamp", "fail", within_clock_skew("event_timestamp")),
        DQRule(
            "bad_price_at_event",
            "fail",
            price_required
            & (F.col("price_at_event").isNull() | (F.col("price_at_event") < 0)),
        ),
    ]


def standardize(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("customer_id", F.upper(F.trim("customer_id")))
        .withColumn("cart_id", F.trim("cart_id"))
        .withColumn("device_type", F.lower(F.trim("device_type")))
    )


def dedupe(df: DataFrame) -> DataFrame:
    """cart-lifecycle events dedupe on the business key (a producer retry re-emits the same
    logical action under a new event_id); everything else dedupes on event_id."""
    cart_events = df.filter(F.col("cart_id").isNotNull())
    other_events = df.filter(F.col("cart_id").isNull())

    cart_deduped = dedupe_last_write_wins(
        cart_events,
        ["cart_id", "event_type", "event_timestamp"],
        "producer_ingested_at",
    )
    other_deduped = dedupe_last_write_wins(
        other_events, ["event_id"], "producer_ingested_at"
    )
    return cart_deduped.unionByName(other_deduped)


def process_batch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    silver_table: str,
    quarantine_table: str,
) -> None:
    if batch_df.isEmpty():
        return

    standardized = standardize(batch_df)
    clean_df, quarantine_df = apply_dq_rules(standardized, build_dq_rules())
    deduped = dedupe(clean_df).withColumn("_silver_processed_at", F.current_timestamp())

    if spark.catalog.tableExists(silver_table):
        from delta.tables import DeltaTable

        target = DeltaTable.forName(spark, silver_table)
        (
            target.alias("t")
            .merge(deduped.alias("s"), "t.event_id = s.event_id")
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            deduped.write.format("delta")
            .partitionBy("event_date")
            .saveAsTable(silver_table)
        )

    write_quarantine(quarantine_df, quarantine_table)
    print(
        f"batch {batch_id}: {deduped.count()} clean rows merged, "
        f"{quarantine_df.count()} quarantined"
    )


def main():
    spark = SparkSession.builder.appName("silver_clickstream").getOrCreate()
    cfg = get_config()

    bronze_table = cfg.table("bronze", "clickstream_events")
    silver_table = cfg.table("silver", "clickstream_events")
    quarantine_table = cfg.table("silver", "clickstream_events_quarantine")
    checkpoint_path = cfg.checkpoint_path("clickstream_silver")

    stream = (
        spark.readStream.format("delta")
        .table(bronze_table)
        .withWatermark("event_timestamp", "2 hours")
    )

    with job_run(
        spark,
        cfg,
        job_name="silver_clickstream",
        layer="silver",
        target_table=silver_table,
    ):
        query = (
            stream.writeStream.foreachBatch(
                lambda df, batch_id: process_batch(
                    df, batch_id, spark, silver_table, quarantine_table
                )
            )
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime="2 minutes")
            .start()
        )
        query.awaitTermination()


if __name__ == "__main__":
    main()
