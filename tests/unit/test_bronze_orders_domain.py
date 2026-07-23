"""Dedupe and validation tests for the bronze order-domain load.

The guarantee under test is the one the module docstring claims: one row per merge key reaching
either write path. Duplicates crash a MERGE loudly but insert silently via saveAsTable on first
run, so the first-run case is the one worth pinning down.
"""

import pytest

from bronze_orders_domain import TableSpec, dedupe_on_keys, validate


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


def _spec(name, merge_keys):
    # validate() reads only name and merge_keys; the schema is applied at read time.
    return TableSpec(name, None, merge_keys)


def test_validate_returns_source_row_count(spark):
    df = spark.createDataFrame(
        [
            {"order_id": "O1", "status": "shipped"},
            {"order_id": "O2", "status": "pending"},
        ]
    )

    assert validate(df, _spec("orders", ("order_id",))) == 2


def test_validate_rejects_null_merge_key(spark):
    # A blank key field parses to null: <=> would match it against every other null-keyed
    # row, so the merge would collapse distinct rows instead of failing.
    df = spark.createDataFrame(
        [
            {"order_id": "O1", "status": "shipped"},
            {"order_id": None, "status": "pending"},
        ]
    )

    with pytest.raises(ValueError, match=r"order_id=1"):
        validate(df, _spec("orders", ("order_id",)))


def test_validate_reports_every_offending_key_column(spark):
    df = spark.createDataFrame(
        [
            {"order_id": "O1", "sku": "A"},
            {"order_id": None, "sku": "B"},
            {"order_id": "O2", "sku": None},
        ]
    )

    with pytest.raises(ValueError) as excinfo:
        validate(df, _spec("order_items", ("order_id", "sku")))

    message = str(excinfo.value)
    assert "order_id=1" in message and "sku=1" in message


def test_validate_accepts_nulls_outside_the_merge_key(spark):
    # cart_id is null for standalone orders -- routine, not a failure.
    df = spark.createDataFrame(
        [
            {"order_id": "O1", "cart_id": None},
            {"order_id": "O2", "cart_id": "C2"},
        ]
    )

    assert validate(df, _spec("orders", ("order_id",))) == 2
