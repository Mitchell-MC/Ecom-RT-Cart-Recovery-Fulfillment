"""Physical layout benchmark: partition-only vs. partition + Z-ORDER for the cart-recovery
point-lookup query pattern. Methodology and how to interpret the output are documented in
docs/distributed-compute-notes.md#5-benchmark-partition-only-vs-partition--z-order -- this
script produces the numbers that doc's Result section should be filled in with.

Not part of the scheduled pipeline: run manually/ad hoc against a populated `silver` schema.

Usage (as a one-off Databricks job or notebook):
    spark-submit benchmark_layout.py
"""

from __future__ import annotations

import os
import sys
import time

from pyspark.sql import SparkSession

sys.path.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../common")
)
from config import get_config  # noqa: E402

N_RUNS = 5


def build_zorder_copy(
    spark: SparkSession, source_table: str, target_table: str
) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")
    spark.sql(
        f"""
        CREATE TABLE {target_table}
        USING DELTA
        PARTITIONED BY (event_date)
        AS SELECT * FROM {source_table}
    """
    )
    spark.sql(f"OPTIMIZE {target_table} ZORDER BY (cart_id)")


def sample_lookup_predicate(
    spark: SparkSession, source_table: str
) -> tuple[str, str, str]:
    """Pick a real cart_id + date window from the data so the benchmark reflects an actual
    query shape rather than a cart_id guaranteed to hit zero rows."""
    row = (
        spark.table(source_table)
        .where("cart_id is not null")
        .selectExpr("cart_id", "event_date")
        .limit(1)
        .collect()[0]
    )
    return row["cart_id"], str(row["event_date"]), str(row["event_date"])


def time_query(spark: SparkSession, table: str, cart_id: str, start_date: str) -> float:
    spark.sql("CLEAR CACHE")
    query = f"""
        SELECT * FROM {table}
        WHERE cart_id = '{cart_id}'
          AND event_date >= date_sub('{start_date}', 7)
    """
    started = time.perf_counter()
    spark.sql(query).count()  # force execution
    return time.perf_counter() - started


def file_count(spark: SparkSession, table: str) -> int:
    detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
    return detail["numFiles"]


def main():
    spark = SparkSession.builder.appName("benchmark_layout").getOrCreate()
    cfg = get_config()

    layout_a = cfg.table("silver", "clickstream_events")
    layout_b = cfg.table("silver", "_bench_clickstream_events_zorder")

    build_zorder_copy(spark, layout_a, layout_b)
    cart_id, start_date, _ = sample_lookup_predicate(spark, layout_a)

    results = {}
    for label, table in [
        ("A: partition-only", layout_a),
        ("B: partition + ZORDER", layout_b),
    ]:
        durations = [
            time_query(spark, table, cart_id, start_date) for _ in range(N_RUNS)
        ]
        results[label] = {
            "avg_seconds": sum(durations) / len(durations),
            "min_seconds": min(durations),
            "num_files": file_count(spark, table),
        }

    print(f"\nQuery: cart_id = {cart_id!r}, event_date >= {start_date} - 7 days")
    print(f"{'layout':<24} {'avg_sec':>10} {'min_sec':>10} {'num_files':>10}")
    for label, r in results.items():
        print(
            f"{label:<24} {r['avg_seconds']:>10.3f} {r['min_seconds']:>10.3f} {r['num_files']:>10}"
        )
    print("\nPaste this table into docs/distributed-compute-notes.md section 5.")


if __name__ == "__main__":
    main()
