"""Dedupe tests for the bronze order-domain load.

The guarantee under test is the one the module docstring claims: one row per merge key reaching
either write path. Duplicates crash a MERGE loudly but insert silently via saveAsTable on first
run, so the first-run case is the one worth pinning down.
"""

from bronze_orders_domain import dedupe_on_keys


def test_keeps_last_occurrence_for_duplicate_key(spark):
    # A flat OMS export can restate a row later in the file; the later line wins.
    df = spark.createDataFrame(
        [
            {"order_id": "O1", "status": "shipped"},
            {"order_id": "O1", "status": "delivered"},
            {"order_id": "O2", "status": "pending"},
        ]
    )
    result = {
        r["order_id"]: r["status"] for r in dedupe_on_keys(df, ("order_id",)).collect()
    }

    assert result == {"O1": "delivered", "O2": "pending"}


def test_composite_key_dedupes_on_the_full_key(spark):
    # order_items is keyed on (order_id, sku): same order, different sku is not a duplicate.
    df = spark.createDataFrame(
        [
            {"order_id": "O1", "sku": "A", "qty": 1},
            {"order_id": "O1", "sku": "B", "qty": 2},
            {"order_id": "O1", "sku": "A", "qty": 3},
        ]
    )
    result = dedupe_on_keys(df, ("order_id", "sku"))

    assert result.count() == 2
    assert {(r["sku"], r["qty"]) for r in result.collect()} == {("A", 3), ("B", 2)}


def test_unique_input_is_unchanged(spark):
    df = spark.createDataFrame(
        [
            {"order_id": "O1", "status": "shipped"},
            {"order_id": "O2", "status": "pending"},
        ]
    )
    result = dedupe_on_keys(df, ("order_id",))

    assert result.count() == 2
    assert set(result.columns) == set(df.columns)  # no _rn leakage
