"""Tests for the upstream freshness gate.

table_watermark is patched so these stay Spark-free: the logic under test is the budget
comparison and what it does with a missing table, not Delta reads. The gate exists to stop a
gold job publishing a freshly-timestamped table built on stale upstream data, so the cases that
matter are (a) stale but present, and (b) absent entirely -- (b) being the one a naive
`max(watermark) > cutoff` check silently passes.
"""

from datetime import datetime, timedelta, timezone

import pytest

import freshness
from freshness import (
    StaleUpstreamError,
    UpstreamRequirement,
    assert_upstream_fresh,
    staleness_report,
)

NOW = datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc)


class _Cfg:
    def table(self, layer, name):
        return f"ecom_dev.{layer}.{name}"


@pytest.fixture
def watermarks(monkeypatch):
    """Map of table name -> watermark datetime (or None for missing/empty)."""
    values = {}
    monkeypatch.setattr(
        freshness, "table_watermark", lambda spark, table, column: values.get(table)
    )
    return values


REQ = (UpstreamRequirement("silver", "orders", "_silver_processed_at", 90),)


def test_fresh_upstream_reports_nothing(watermarks):
    watermarks["ecom_dev.silver.orders"] = NOW - timedelta(minutes=10)

    assert staleness_report(None, _Cfg(), REQ, now=NOW) == []


def test_stale_upstream_is_reported_with_age_and_budget(watermarks):
    watermarks["ecom_dev.silver.orders"] = NOW - timedelta(days=3)

    report = staleness_report(None, _Cfg(), REQ, now=NOW)

    assert len(report) == 1
    assert "3d00h old" in report[0]
    assert "budget 90m" in report[0]


def test_missing_table_is_a_breach_not_a_pass(watermarks):
    # The table is absent entirely -- no watermark to compare. Treating "no rows" as "not stale"
    # is how a never-populated table sails through a freshness check.
    report = staleness_report(None, _Cfg(), REQ, now=NOW)

    assert len(report) == 1
    assert "missing or empty" in report[0]


def test_naive_watermark_is_treated_as_utc(watermarks):
    # Spark hands back naive timestamps; the pipeline is UTC end to end. Without the explicit
    # attach this comparison raises TypeError instead of measuring anything.
    watermarks["ecom_dev.silver.orders"] = datetime(2026, 7, 20, 11, 50)

    assert staleness_report(None, _Cfg(), REQ, now=NOW) == []


def test_boundary_exactly_at_budget_is_not_stale(watermarks):
    watermarks["ecom_dev.silver.orders"] = NOW - timedelta(minutes=90)

    assert staleness_report(None, _Cfg(), REQ, now=NOW) == []


def test_assert_raises_naming_every_stale_upstream(watermarks, monkeypatch):
    monkeypatch.setitem(
        freshness.UPSTREAM_REQUIREMENTS,
        "gold_test_job",
        (
            UpstreamRequirement("silver", "orders", "_silver_processed_at", 90),
            UpstreamRequirement("silver", "shipments", "_silver_processed_at", 90),
        ),
    )
    watermarks["ecom_dev.silver.orders"] = NOW - timedelta(days=3)
    watermarks["ecom_dev.silver.shipments"] = NOW - timedelta(minutes=5)

    with pytest.raises(StaleUpstreamError) as excinfo:
        assert_upstream_fresh(None, _Cfg(), "gold_test_job", now=NOW)

    message = str(excinfo.value)
    assert "silver.orders" in message
    assert "silver.shipments" not in message  # fresh one isn't blamed


def test_assert_passes_when_all_fresh(watermarks, monkeypatch):
    monkeypatch.setitem(freshness.UPSTREAM_REQUIREMENTS, "gold_test_job", REQ)
    watermarks["ecom_dev.silver.orders"] = NOW - timedelta(minutes=1)

    assert_upstream_fresh(None, _Cfg(), "gold_test_job", now=NOW)


def test_unknown_job_is_a_no_op():
    # A job with no declared upstreams shouldn't fail closed just for not being in the map.
    assert_upstream_fresh(None, _Cfg(), "some_job_with_no_upstreams", now=NOW)


def test_every_gold_job_declares_its_upstreams():
    # The gate is only as good as its coverage: a gold job missing from this map silently
    # reverts to the old publish-on-stale-data behaviour.
    assert set(freshness.UPSTREAM_REQUIREMENTS) == {
        "gold_cart_recovery",
        "gold_fulfillment_risk",
        "gold_exec_summary_marts",
    }
