"""gold.exec_summary_daily -- one row appended/upserted per calendar day, the mart Power BI's
exec summary page reads from (see bi/powerbi/data-model.md). Scheduled daily at 06:00 UTC per
docs/project-charter.md.

`recoverable_revenue` and `delayed_order_revenue_risk` are read from
gold.cart_recovery_signal_history / gold.fulfillment_risk_signal_history -- the insert-only,
one-row-per-run history tables written by gold_cart_recovery.py / gold_fulfillment_risk.py --
using each metric's latest snapshot as of this job's run time. That makes both metrics correct
regardless of when this job runs relative to the (differently-cadenced, hourly/4h) signal jobs,
without a hard cross-job dependency: no risk of reading a stale-until-overwritten current-snapshot
table, and a real historical record survives even if this daily job itself never ran on a given
day. `on_time_delivery_rate` and `conversion_lag_median_minutes` are computed directly from
silver over a trailing 30-day window, so they were already immune to this issue.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../common")
)
from audit import log_run  # noqa: E402
from config import get_config  # noqa: E402

PRIORITY_ACTION_THRESHOLD = 60
RISK_ACTION_THRESHOLD = 60
TRAILING_WINDOW_DAYS = 30


def _latest_snapshot(history: DataFrame) -> DataFrame:
    latest_run = history.agg(F.max("_snapshot_at").alias("t")).collect()[0]["t"]
    return history.filter(F.col("_snapshot_at") == F.lit(latest_run))


def recoverable_revenue(cart_recovery_signal_history: DataFrame) -> float:
    row = (
        _latest_snapshot(cart_recovery_signal_history)
        .filter(F.col("priority_score") >= PRIORITY_ACTION_THRESHOLD)
        .agg(F.sum("recoverable_cart_value").alias("total"))
        .collect()[0]
    )
    return row["total"] or 0.0


def delayed_order_revenue_risk(fulfillment_risk_signal_history: DataFrame) -> float:
    row = (
        _latest_snapshot(fulfillment_risk_signal_history)
        .filter(F.col("fulfillment_risk_score") >= RISK_ACTION_THRESHOLD)
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
    started_at = datetime.now(timezone.utc)
    spark = SparkSession.builder.appName("gold_exec_summary_marts").getOrCreate()
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
    cfg = get_config(dbutils)

    cart_recovery_signal_history = spark.read.table(
        cfg.table("gold", "cart_recovery_signal_history")
    )
    fulfillment_risk_signal_history = spark.read.table(
        cfg.table("gold", "fulfillment_risk_signal_history")
    )
    shipments = spark.read.table(cfg.table("silver", "shipments"))
    clickstream_events = spark.read.table(cfg.table("silver", "clickstream_events"))

    metrics = {
        "metric_date": datetime.now(timezone.utc).date().isoformat(),
        "recoverable_revenue": recoverable_revenue(cart_recovery_signal_history),
        "delayed_order_revenue_risk": delayed_order_revenue_risk(
            fulfillment_risk_signal_history
        ),
        "on_time_delivery_rate": on_time_delivery_rate(shipments),
        "conversion_lag_median_minutes": conversion_lag_median_minutes(
            clickstream_events
        ),
    }

    target_table = cfg.table("gold", "exec_summary_daily")
    upsert_daily_row(spark, target_table, metrics)

    log_run(
        spark,
        cfg,
        job_name="gold_exec_summary_marts",
        layer="gold",
        target_table=target_table,
        row_count=1,
        started_at=started_at,
    )
    print(f"gold.exec_summary_daily upserted for {metrics['metric_date']}: {metrics}")


if __name__ == "__main__":
    main()
