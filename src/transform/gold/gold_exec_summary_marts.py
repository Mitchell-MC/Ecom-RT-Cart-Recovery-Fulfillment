"""gold.exec_summary_daily -- one row appended/upserted per calendar day, the mart Power BI's
exec summary page reads from (see bi/powerbi/data-model.md). Scheduled daily at 06:00 UTC per
docs/project-charter.md.

`recoverable_revenue` and `delayed_order_revenue_risk` are captures of the *current* gold signal
snapshots (gold.cart_recovery_signal / gold.fulfillment_risk_signal are overwritten in place each
run, not historical) -- this job is what turns those snapshots into a trend line by recording one
point per day. `on_time_delivery_rate` and `conversion_lag_median_minutes` are computed directly
from silver over a trailing 30-day window, so they're accurate for the day regardless of when
this job runs relative to the snapshot jobs.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(_THIS_DIR, "../../common"))
sys.path.append(os.path.join(_THIS_DIR, "../../quality"))
from audit import job_run  # noqa: E402
from config import get_config  # noqa: E402
from freshness import assert_upstream_fresh  # noqa: E402

PRIORITY_ACTION_THRESHOLD = 60
RISK_ACTION_THRESHOLD = 60
TRAILING_WINDOW_DAYS = 30


def recoverable_revenue(cart_recovery_signal: DataFrame) -> float:
    row = (
        cart_recovery_signal.filter(
            F.col("priority_score") >= PRIORITY_ACTION_THRESHOLD
        )
        .agg(F.sum("recoverable_cart_value").alias("total"))
        .collect()[0]
    )
    return row["total"] or 0.0


def delayed_order_revenue_risk(fulfillment_risk_signal: DataFrame) -> float:
    row = (
        fulfillment_risk_signal.filter(
            F.col("fulfillment_risk_score") >= RISK_ACTION_THRESHOLD
        )
        .agg(F.sum("order_total_usd").alias("total"))
        .collect()[0]
    )
    return row["total"] or 0.0


def on_time_delivery_rate(shipments: DataFrame) -> float:
    recent = shipments.filter(
        F.col("delivered_at").isNotNull()
        & (
            F.col("delivered_at")
            >= F.expr(f"current_timestamp() - INTERVAL {TRAILING_WINDOW_DAYS} DAYS")
        )
    ).withColumn(
        "is_on_time", F.to_date("delivered_at") <= F.col("promised_delivery_date")
    )

    row = recent.agg(F.avg(F.col("is_on_time").cast("int")).alias("rate")).collect()[0]
    return row["rate"] or 0.0


def conversion_lag_median_minutes(clickstream_events: DataFrame) -> float | None:
    per_cart = (
        clickstream_events.filter(
            F.col("event_type").isin("add_to_cart", "checkout_complete")
        )
        .filter(F.col("cart_id").isNotNull())
        .groupBy("cart_id")
        .agg(
            F.min(
                F.when(F.col("event_type") == "add_to_cart", F.col("event_timestamp"))
            ).alias("first_add_at"),
            F.max(
                F.when(
                    F.col("event_type") == "checkout_complete", F.col("event_timestamp")
                )
            ).alias("checkout_complete_at"),
        )
        .filter(F.col("checkout_complete_at").isNotNull())
        .filter(
            F.col("checkout_complete_at")
            >= F.expr(f"current_timestamp() - INTERVAL {TRAILING_WINDOW_DAYS} DAYS")
        )
        .withColumn(
            "lag_minutes",
            (
                F.unix_timestamp("checkout_complete_at")
                - F.unix_timestamp("first_add_at")
            )
            / 60.0,
        )
    )
    row = per_cart.agg(
        F.percentile_approx("lag_minutes", 0.5).alias("median")
    ).collect()[0]
    return row["median"]


def upsert_daily_row(spark: SparkSession, target_table: str, metrics: dict) -> None:
    row_df = spark.createDataFrame([metrics])
    if not spark.catalog.tableExists(target_table):
        row_df.write.format("delta").saveAsTable(target_table)
        return
    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(row_df.alias("s"), "t.metric_date = s.metric_date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    spark = SparkSession.builder.appName("gold_exec_summary_marts").getOrCreate()
    cfg = get_config()
    target_table = cfg.table("gold", "exec_summary_daily")

    with job_run(
        spark,
        cfg,
        job_name="gold_exec_summary_marts",
        layer="gold",
        target_table=target_table,
    ) as run:
        # The 06:00 slot assumes the hourly/4-hourly gold jobs already landed today's snapshot
        # (see gold_jobs.yml). That assumption is scheduling, not a dependency, so it has to be
        # checked rather than trusted -- otherwise a day of missed runs is rolled up as today's.
        assert_upstream_fresh(spark, cfg, "gold_exec_summary_marts")

        cart_recovery_signal = spark.read.table(
            cfg.table("gold", "cart_recovery_signal")
        )
        fulfillment_risk_signal = spark.read.table(
            cfg.table("gold", "fulfillment_risk_signal")
        )
        shipments = spark.read.table(cfg.table("silver", "shipments"))
        clickstream_events = spark.read.table(cfg.table("silver", "clickstream_events"))

        metrics = {
            "metric_date": datetime.now(timezone.utc).date().isoformat(),
            "recoverable_revenue": recoverable_revenue(cart_recovery_signal),
            "delayed_order_revenue_risk": delayed_order_revenue_risk(
                fulfillment_risk_signal
            ),
            "on_time_delivery_rate": on_time_delivery_rate(shipments),
            "conversion_lag_median_minutes": conversion_lag_median_minutes(
                clickstream_events
            ),
        }

        upsert_daily_row(spark, target_table, metrics)
        run.row_count = 1
        print(
            f"gold.exec_summary_daily upserted for {metrics['metric_date']}: {metrics}"
        )


if __name__ == "__main__":
    main()
