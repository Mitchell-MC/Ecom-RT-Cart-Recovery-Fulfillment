from datetime import datetime, timedelta, timezone

import pytest
from dq_checks import (
    DQRule,
    apply_dq_rules,
    assert_unique,
    dedupe_last_write_wins,
    non_negative,
    not_in,
    not_null,
    within_clock_skew,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def test_apply_dq_rules_splits_clean_and_quarantine(spark):
    now = datetime.now(timezone.utc)
    rows = [
        {"id": "1", "price": 10.0, "ts": now},
        {"id": "2", "price": None, "ts": now},  # fails not_null
        {"id": "3", "price": -5.0, "ts": now},  # fails non_negative
        {"id": "4", "price": 10.0, "ts": now + timedelta(days=2)},  # fails clock skew
    ]
    df = spark.createDataFrame(rows)

    rules = [
        DQRule("missing_price", "fail", not_null("price")),
        DQRule("negative_price", "fail", non_negative("price")),
        DQRule("bad_timestamp", "fail", within_clock_skew("ts")),
    ]
    clean_df, quarantine_df = apply_dq_rules(df, rules)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 3
    assert clean_df.collect()[0]["id"] == "1"

    reasons = {
        row["id"]: set(row["_dq_fail_reasons"]) for row in quarantine_df.collect()
    }
    assert reasons["2"] == {"missing_price"}
    assert reasons["3"] == {"negative_price"}
    assert reasons["4"] == {"bad_timestamp"}


def test_apply_dq_rules_warn_does_not_quarantine(spark):
    # `value` is null in the only row, so Spark cannot infer its type -- state it explicitly.
    schema = StructType(
        [StructField("id", StringType()), StructField("value", DoubleType())]
    )
    df = spark.createDataFrame([{"id": "1", "value": None}], schema=schema)
    rules = [DQRule("missing_value", "warn", not_null("value"))]
    clean_df, quarantine_df = apply_dq_rules(df, rules)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 0
    assert clean_df.collect()[0]["_dq_warnings"] == ["missing_value"]


def test_not_in_flags_values_outside_allowed_list(spark):
    schema = StructType(
        [StructField("id", StringType()), StructField("category", StringType())]
    )
    rows = [
        {"id": "1", "category": "apparel"},  # allowed
        {"id": "2", "category": "crypto"},  # not allowed
        {"id": "3", "category": None},  # null never triggers
    ]
    df = spark.createDataFrame(rows, schema=schema)
    rules = [
        DQRule(
            "unexpected_category", "warn", not_in("category", ["apparel", "footwear"])
        )
    ]
    clean_df, quarantine_df = apply_dq_rules(df, rules)

    assert quarantine_df.count() == 0  # warn severity never quarantines
    warnings = {row["id"]: row["_dq_warnings"] for row in clean_df.collect()}
    assert warnings["1"] == []
    assert warnings["2"] == ["unexpected_category"]
    assert warnings["3"] == []


def test_dedupe_last_write_wins(spark):
    rows = [
        {"key": "a", "order_col": 1, "payload": "old"},
        {"key": "a", "order_col": 2, "payload": "new"},
        {"key": "b", "order_col": 1, "payload": "only"},
    ]
    df = spark.createDataFrame(rows)
    deduped = dedupe_last_write_wins(df, ["key"], "order_col")

    result = {row["key"]: row["payload"] for row in deduped.collect()}
    assert result == {"a": "new", "b": "only"}


def test_assert_unique_passes_when_grain_has_no_duplicates(spark):
    df = spark.createDataFrame([{"id": "1"}, {"id": "2"}])
    assert_unique(df, ["id"], context="test_table")  # should not raise


def test_assert_unique_raises_on_duplicate_keys(spark):
    df = spark.createDataFrame([{"id": "1"}, {"id": "1"}, {"id": "2"}])
    with pytest.raises(ValueError, match="test_table"):
        assert_unique(df, ["id"], context="test_table")
