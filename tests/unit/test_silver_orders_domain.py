from dq_checks import apply_dq_rules
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from silver_orders_domain import (
    _orders_dq_rules,
    _products_dq_rules,
    _shipments_dq_rules,
)


def test_products_dq_rules_warns_on_unexpected_category(spark):
    schema = StructType(
        [
            StructField("sku", StringType()),
            StructField("price", DoubleType()),
            StructField("category", StringType()),
        ]
    )
    rows = [
        {"sku": "SKU1", "price": 10.0, "category": "apparel"},
        {"sku": "SKU2", "price": 10.0, "category": "crypto"},  # unexpected
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = apply_dq_rules(df, _products_dq_rules())

    assert quarantine_df.count() == 0
    warnings = {row["sku"]: row["_dq_warnings"] for row in clean_df.collect()}
    assert warnings["SKU1"] == []
    assert warnings["SKU2"] == ["unexpected_category"]


def test_orders_dq_rules_warns_on_unexpected_channel_currency_status(spark):
    schema = StructType(
        [
            StructField("order_id", StringType()),
            StructField("order_total_usd", DoubleType()),
            StructField("fx_rate_to_usd", DoubleType()),
            StructField("promised_delivery_date", StringType()),
            StructField("channel", StringType()),
            StructField("currency", StringType()),
            StructField("status", StringType()),
        ]
    )
    rows = [
        {
            "order_id": "1",
            "order_total_usd": 10.0,
            "fx_rate_to_usd": 1.0,
            "promised_delivery_date": "2024-01-01",
            "channel": "web",
            "currency": "USD",
            "status": "delivered",
        },
        {
            "order_id": "2",
            "order_total_usd": 10.0,
            "fx_rate_to_usd": 1.0,
            "promised_delivery_date": "2024-01-01",
            "channel": "carrier_pigeon",  # unexpected
            "currency": "BTC",  # unexpected
            "status": "lost",  # unexpected
        },
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = apply_dq_rules(df, _orders_dq_rules())

    assert quarantine_df.count() == 0
    warnings = {row["order_id"]: set(row["_dq_warnings"]) for row in clean_df.collect()}
    assert warnings["1"] == set()
    assert warnings["2"] == {
        "unexpected_channel",
        "unexpected_currency",
        "unexpected_status",
    }


def test_shipments_dq_rules_warns_on_unexpected_status(spark):
    schema = StructType(
        [StructField("order_id", StringType()), StructField("status", StringType())]
    )
    rows = [
        {"order_id": "1", "status": "delivered"},
        {"order_id": "2", "status": "lost_in_space"},  # unexpected
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = apply_dq_rules(df, _shipments_dq_rules())

    assert quarantine_df.count() == 0
    warnings = {row["order_id"]: row["_dq_warnings"] for row in clean_df.collect()}
    assert warnings["1"] == []
    assert warnings["2"] == ["unexpected_status"]
