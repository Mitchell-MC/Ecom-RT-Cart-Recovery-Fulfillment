"""Tests for the batch-level quarantine rate guard.

Row-level DQ already works: bad rows are quarantined and good rows proceed. What it cannot see
is that *most* of a batch failed, which is not a row problem but a contract problem -- an
upstream rename or unit change. Before this guard, that case published the surviving slice and
reported success.
"""

import pytest

from dq_checks import (
    MAX_QUARANTINE_RATE,
    QuarantineRateExceeded,
    assert_quarantine_rate_ok,
)


def test_normal_bad_row_rate_passes():
    rate = assert_quarantine_rate_ok(
        "silver.orders", clean_count=980, quarantined_count=20
    )
    assert rate == pytest.approx(0.02)


def test_mass_quarantine_raises():
    with pytest.raises(QuarantineRateExceeded, match="900/1000"):
        assert_quarantine_rate_ok(
            "silver.orders", clean_count=100, quarantined_count=900
        )


def test_small_batch_is_exempt():
    # 3 of 5 is 60%, but on five rows that is noise. A guard that fires here gets muted.
    rate = assert_quarantine_rate_ok(
        "silver.orders", clean_count=2, quarantined_count=3
    )
    assert rate == pytest.approx(0.6)


def test_empty_batch_is_not_a_division_by_zero():
    assert assert_quarantine_rate_ok("silver.orders", 0, 0) == 0.0


def test_everything_quarantined_on_a_large_batch_raises():
    # The worst case and the one most likely to be mistaken for success: zero clean rows
    # written, no error raised, job green.
    with pytest.raises(QuarantineRateExceeded):
        assert_quarantine_rate_ok("silver.orders", clean_count=0, quarantined_count=500)


def test_rate_exactly_at_threshold_passes():
    at_threshold = int(1000 * MAX_QUARANTINE_RATE)
    rate = assert_quarantine_rate_ok(
        "silver.orders",
        clean_count=1000 - at_threshold,
        quarantined_count=at_threshold,
    )
    assert rate == pytest.approx(MAX_QUARANTINE_RATE)


def test_message_names_the_dataset_and_counts():
    with pytest.raises(QuarantineRateExceeded) as excinfo:
        assert_quarantine_rate_ok(
            "silver.clickstream_events", clean_count=10, quarantined_count=490
        )

    message = str(excinfo.value)
    assert "silver.clickstream_events" in message
    assert "98.0%" in message
