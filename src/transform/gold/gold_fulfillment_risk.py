"""gold.fulfillment_risk_signal -- one row per open (in-transit, not cancelled) order, ranked by
docs/metric-glossary.md's fulfillment_risk_score. Batch job, scheduled every 4 hours (see
orchestration/databricks/resources/gold_jobs.yml).
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
from freshness import assert_upstream_fresh  # noqa: E402


def open_orders_with_shipment(orders: DataFrame, shipments: DataFrame) -> DataFrame:
    return (
        orders.filter(F.col("status") == "in_transit")
        # Rows still missing promised_delivery_date are excluded here (not scored as 0-risk)
        # rather than guessed at -- see the silver `missing_promised_delivery_date` warn rule.
        .filter(F.col("promised_delivery_date").isNotNull()).join(
            shipments.select(
                "order_id",
                "carrier",
                "carrier_avg_transit_days",
                "carrier_historical_late_rate_pct",
            ),
            on="order_id",
            how="inner",
        )
    )


def with_inventory_risk(
    orders: DataFrame, order_items: DataFrame, inventory: DataFrame
) -> DataFrame:
    backorder_by_order = (
        order_items.join(
            inventory.select("sku", "backorder_flag"), on="sku", how="left"
        )
        .groupBy("order_id")
        .agg(
            F.max(F.coalesce(F.col("backorder_flag"), F.lit(False))).alias(
                "has_backordered_item"
            )
        )
    )
    return orders.join(backorder_by_order, on="order_id", how="left").withColumn(
        "has_backordered_item", F.coalesce(F.col("has_backordered_item"), F.lit(False))
    )


def score(orders: DataFrame) -> DataFrame:
    return (
        orders.withColumn(
            "days_to_promise",
            F.datediff(F.col("promised_delivery_date"), F.current_date()),
        )
        .withColumn(
            "time_pressure_score",
            100
            * (
                1
                - F.least(
                    F.greatest(F.col("days_to_promise"), F.lit(0))
                    / F.col("carrier_avg_transit_days"),
                    F.lit(1.0),
                )
            ),
        )
        .withColumn(
            "inventory_risk_score",
            F.when(F.col("has_backordered_item"), 100.0).otherwise(0.0),
        )
        .withColumnRenamed("carrier_historical_late_rate_pct", "carrier_risk_score")
        .withColumn(
            "fulfillment_risk_score",
            0.5 * F.col("time_pressure_score")
            + 0.3 * F.col("inventory_risk_score")
            + 0.2 * F.col("carrier_risk_score"),
        )
        .withColumn("_gold_computed_at", F.current_timestamp())
        .select(
            "order_id",
            "customer_id",
            "order_total_usd",
            "carrier",
            "promised_delivery_date",
            "days_to_promise",
            "time_pressure_score",
            "inventory_risk_score",
            "carrier_risk_score",
            "fulfillment_risk_score",
            "_gold_computed_at",
        )
    )


def main():
    spark = SparkSession.builder.appName("gold_fulfillment_risk").getOrCreate()
    cfg = get_config()
    target_table = cfg.table("gold", "fulfillment_risk_signal")

    with job_run(
        spark,
        cfg,
        job_name="gold_fulfillment_risk",
        layer="gold",
        target_table=target_table,
    ) as run:
        assert_upstream_fresh(spark, cfg, "gold_fulfillment_risk")

        orders = spark.read.table(cfg.table("silver", "orders"))
        shipments = spark.read.table(cfg.table("silver", "shipments"))
        order_items = spark.read.table(cfg.table("silver", "order_items"))
        inventory = spark.read.table(cfg.table("silver", "inventory"))

        open_orders = open_orders_with_shipment(orders, shipments)
        with_inventory = with_inventory_risk(open_orders, order_items, inventory)
        scored = score(with_inventory)

        scored.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target_table)

        run.row_count = scored.count()
        print(f"gold.fulfillment_risk_signal: {run.row_count} open orders scored")


if __name__ == "__main__":
    main()
