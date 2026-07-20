from datetime import datetime, timedelta, timezone

from dq_checks import (
    DQRule,
    apply_dq_rules,
    dedupe_last_write_wins,
    non_negative,
    not_null,
    within_clock_skew,
)


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
    df = spark.createDataFrame([{"id": "1", "value": None}])
    rules = [DQRule("missing_value", "warn", not_null("value"))]
    clean_df, quarantine_df = apply_dq_rules(df, rules)

    assert clean_df.count() == 1
    assert quarantine_df.count() == 0
    assert clean_df.collect()[0]["_dq_warnings"] == ["missing_value"]


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
