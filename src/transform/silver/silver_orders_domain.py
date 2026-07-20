"""Batch standardization: bronze.{customers,products,inventory,orders,order_items,shipments}
-> silver.*.

Silver stays normalized (one silver table per bronze table) -- denormalizing into
order-with-items-with-shipment views is a gold-layer concern (src/transform/gold), so a schema
change to one entity doesn't ripple through every downstream consumer. Each table gets
type/format standardization, the DQ contract from docs/metric-glossary.md where it applies, and
an idempotent MERGE into silver on the same key used at bronze.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from typing import Callable, Optional

from delta.tables import DeltaTable
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
    assert_quarantine_rate_ok,
    write_quarantine,
)


def _customers_standardize(df: DataFrame) -> DataFrame:
    return df.withColumn("customer_id", F.upper(F.trim("customer_id")))


def _products_standardize(df: DataFrame) -> DataFrame:
    return df.withColumn("sku", F.upper(F.trim("sku"))).withColumn(
        "category", F.lower("category")
    )


def _orders_standardize(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.upper(F.trim("customer_id")))
        .withColumn("order_date", F.to_date("order_created_at"))
    )


def _orders_dq_rules() -> list[DQRule]:
    return [
        DQRule("missing_order_total", "fail", F.col("order_total_usd").isNull()),
        DQRule("negative_order_total", "fail", F.col("order_total_usd") < 0),
        DQRule("missing_fx_rate", "fail", F.col("fx_rate_to_usd").isNull()),
        # Per docs/metric-glossary.md this is a warn, not a fail: gold.fulfillment_risk_signal
        # filters `promised_delivery_date IS NOT NULL` directly rather than reading this tag, so
        # the row still reaches silver for revenue/KPI purposes while fulfillment-risk scoring
        # waits for the field to be backfilled.
        DQRule(
            "missing_promised_delivery_date",
            "warn",
            F.col("promised_delivery_date").isNull() & (F.col("status") != "cancelled"),
        ),
    ]


def _order_items_dq_rules() -> list[DQRule]:
    return [
        DQRule("non_positive_qty", "fail", F.col("qty").isNull() | (F.col("qty") <= 0)),
        DQRule(
            "negative_unit_price",
            "fail",
            F.col("unit_price").isNull() | (F.col("unit_price") < 0),
        ),
    ]


def _products_dq_rules() -> list[DQRule]:
    return [
        DQRule("negative_price", "fail", F.col("price").isNull() | (F.col("price") < 0))
    ]


def _inventory_dq_rules() -> list[DQRule]:
    return [DQRule("negative_on_hand_qty", "fail", F.col("on_hand_qty") < 0)]


@dataclass(frozen=True)
class SilverTableSpec:
    name: str
    merge_keys: tuple[str, ...]
    standardize: Callable[[DataFrame], DataFrame] = field(default=lambda df: df)
    dq_rules: Optional[Callable[[], list[DQRule]]] = None


TABLE_SPECS: list[SilverTableSpec] = [
    SilverTableSpec("customers", ("customer_id",), _customers_standardize),
    SilverTableSpec("products", ("sku",), _products_standardize, _products_dq_rules),
    SilverTableSpec("inventory", ("sku", "warehouse_id"), dq_rules=_inventory_dq_rules),
    SilverTableSpec("orders", ("order_id",), _orders_standardize, _orders_dq_rules),
    SilverTableSpec("order_items", ("order_id", "sku"), dq_rules=_order_items_dq_rules),
    SilverTableSpec("shipments", ("order_id",)),
]


def merge_into_silver(
    spark: SparkSession, df: DataFrame, target_table: str, merge_keys: tuple[str, ...]
) -> None:
    if not spark.catalog.tableExists(target_table):
        df.write.format("delta").saveAsTable(target_table)
        return
    target = DeltaTable.forName(spark, target_table)
    condition = " AND ".join(f"target.{k} <=> source.{k}" for k in merge_keys)
    (
        target.alias("target")
        .merge(df.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def process_table(
    spark: SparkSession, cfg, spec: SilverTableSpec
) -> tuple[int, int]:
    """Returns (rows_merged, rows_quarantined) so main can total them for the audit row."""
    bronze_table = cfg.table("bronze", spec.name)
    silver_table = cfg.table("silver", spec.name)
    quarantine_table = cfg.table("silver", f"{spec.name}_quarantine")

    df = spark.read.table(bronze_table)
    standardized = spec.standardize(df)

    if spec.dq_rules:
        clean_df, quarantine_df = apply_dq_rules(standardized, spec.dq_rules())
        write_quarantine(quarantine_df, quarantine_table)
        quarantined_count = quarantine_df.count()
        # Checked after the quarantine write so the rows that triggered it are inspectable --
        # raising first would leave nothing to diagnose from.
        assert_quarantine_rate_ok(
            f"silver.{spec.name}", clean_df.count(), quarantined_count
        )
    else:
        clean_df, quarantined_count = standardized, 0

    # Uniform silver watermark: without it these tables carry only _bronze_ingested_at, so a
    # freshness check on silver would actually be measuring bronze and would read as fresh even
    # if this job had not run in days. src/quality/freshness.py depends on this column.
    clean_df = clean_df.withColumn("_silver_processed_at", F.current_timestamp())

    merge_into_silver(spark, clean_df, silver_table, spec.merge_keys)
    row_count = clean_df.count()
    print(
        f"silver.{spec.name}: {row_count} rows merged, {quarantined_count} quarantined"
    )
    return row_count, quarantined_count


def main():
    spark = SparkSession.builder.appName("silver_orders_domain").getOrCreate()
    cfg = get_config()

    with job_run(
        spark,
        cfg,
        job_name="silver_orders_domain",
        layer="silver",
        target_table=cfg.schema("silver") + ".*",
    ) as run:
        total_rows = 0
        total_quarantined = 0
        for spec in TABLE_SPECS:
            rows, quarantined = process_table(spark, cfg, spec)
            total_rows += rows
            total_quarantined += quarantined
            # Update as we go: a crash on table 4 of 6 still records what the first three did,
            # which is the difference between "nothing ran" and "partially applied" during triage.
            run.row_count = total_rows
            run.quarantined_count = total_quarantined


if __name__ == "__main__":
    main()
