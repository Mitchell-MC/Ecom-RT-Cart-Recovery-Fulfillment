"""gold.cart_recovery_signal -- one row per currently-abandoned cart, ranked by
docs/metric-glossary.md's priority_score. Batch job, scheduled hourly (see
orchestration/databricks/resources/gold_job.yml), reading the streaming-fed silver.clickstream_
events table plus silver.orders for customer purchase history.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../common")
)
from audit import log_run  # noqa: E402
from config import get_config  # noqa: E402

CART_EVENT_TYPES = [
    "add_to_cart",
    "remove_from_cart",
    "checkout_start",
    "checkout_complete",
]
CART_ABANDON_THRESHOLD_MINUTES = 30
CART_STALE_HORIZON_DAYS = 7


def aggregate_carts(events: DataFrame) -> DataFrame:
    cart_events = events.filter(F.col("event_type").isin(CART_EVENT_TYPES))

    return cart_events.groupBy("cart_id").agg(
        F.first("customer_id", ignorenulls=True).alias("customer_id"),
        F.max("event_timestamp").alias("last_activity_at"),
        F.max(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
            "has_add"
        ),
        F.max(F.when(F.col("event_type") == "checkout_complete", 1).otherwise(0)).alias(
            "has_checkout_complete"
        ),
        F.sum(
            F.when(
                F.col("event_type") == "add_to_cart",
                F.col("price_at_event") * F.col("quantity"),
            ).otherwise(0.0)
        ).alias("added_value"),
        F.sum(
            F.when(
                F.col("event_type") == "remove_from_cart",
                F.col("price_at_event") * F.col("quantity"),
            ).otherwise(0.0)
        ).alias("removed_value"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
            "n_adds"
        ),
        F.sum(F.when(F.col("event_type") == "remove_from_cart", 1).otherwise(0)).alias(
            "n_removes"
        ),
    )


def filter_abandoned(cart_agg: DataFrame) -> DataFrame:
    return (
        cart_agg.filter(F.col("has_add") == 1)
        .filter(F.col("has_checkout_complete") == 0)
        .filter(
            F.col("last_activity_at")
            < F.expr(
                f"current_timestamp() - INTERVAL {CART_ABANDON_THRESHOLD_MINUTES} MINUTES"
            )
        )
        .filter(
            F.col("last_activity_at")
            > F.expr(f"current_timestamp() - INTERVAL {CART_STALE_HORIZON_DAYS} DAYS")
        )
        .withColumn(
            "recoverable_cart_value",
            F.greatest(F.col("added_value") - F.col("removed_value"), F.lit(0.0)),
        )
        .withColumn(
            "item_count", F.greatest(F.col("n_adds") - F.col("n_removes"), F.lit(0))
        )
        .withColumn(
            "minutes_since_last_activity",
            (
                F.unix_timestamp(F.current_timestamp())
                - F.unix_timestamp("last_activity_at")
            )
            / 60.0,
        )
    )


def score_carts(abandoned: DataFrame, orders: DataFrame) -> DataFrame:
    # value_p90: 90th percentile of recoverable_cart_value across the current (7-day-bounded,
    # per CART_STALE_HORIZON_DAYS) abandoned-cart population -- see docs/metric-glossary.md
    # note on why this substitutes for a separately-maintained 30-day trailing window.
    value_p90_row = abandoned.select(
        F.percentile_approx("recoverable_cart_value", 0.90).alias("p90")
    ).collect()[0]
    value_p90 = value_p90_row["p90"] or 1.0

    prior_orders = (
        orders.filter(F.col("status") != "cancelled")
        .groupBy("customer_id")
        .agg(F.count("*").alias("prior_order_count"))
    )

    return (
        abandoned.join(prior_orders, on="customer_id", how="left")
        .withColumn(
            "has_prior_order", F.coalesce(F.col("prior_order_count"), F.lit(0)) > 0
        )
        .withColumn(
            "recency_score", 100 * F.exp(-F.col("minutes_since_last_activity") / 180.0)
        )
        .withColumn(
            "value_score",
            100
            * F.least(F.col("recoverable_cart_value") / F.lit(value_p90), F.lit(1.0)),
        )
        .withColumn(
            "loyalty_score", F.when(F.col("has_prior_order"), 100.0).otherwise(40.0)
        )
        .withColumn(
            "priority_score",
            0.45 * F.col("recency_score")
            + 0.40 * F.col("value_score")
            + 0.15 * F.col("loyalty_score"),
        )
        .withColumn("_gold_computed_at", F.current_timestamp())
        .select(
            "cart_id",
            "customer_id",
            "last_activity_at",
            "minutes_since_last_activity",
            "recoverable_cart_value",
            "item_count",
            "recency_score",
            "value_score",
            "loyalty_score",
            "priority_score",
            "_gold_computed_at",
        )
    )


def main():
    started_at = datetime.now(timezone.utc)
    spark = SparkSession.builder.appName("gold_cart_recovery").getOrCreate()
    cfg = get_config()

    events = spark.read.table(cfg.table("silver", "clickstream_events"))
    orders = spark.read.table(cfg.table("silver", "orders"))

    cart_agg = aggregate_carts(events)
    abandoned = filter_abandoned(cart_agg)
    scored = score_carts(abandoned, orders)

    target_table = cfg.table("gold", "cart_recovery_signal")
    scored.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target_table)

    row_count = scored.count()
    log_run(
        spark,
        cfg,
        job_name="gold_cart_recovery",
        layer="gold",
        target_table=target_table,
        row_count=row_count,
        started_at=started_at,
    )
    print(f"gold.cart_recovery_signal: {row_count} abandoned carts scored")


if __name__ == "__main__":
    main()
