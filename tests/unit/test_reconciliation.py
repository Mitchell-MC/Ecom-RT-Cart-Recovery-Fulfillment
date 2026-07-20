"""Tests for row-conservation reconciliation.

The invariant -- source == written + deduped + quarantined -- is the generalisation of the two
bugs at the base of this branch: duplicate source rows collapsing into one, and null merge keys
collapsing distinct rows together. Both were row-count failures that produced a green job.
"""

import pytest

from reconciliation import (
    RowConservationError,
    assert_dedupe_rate_ok,
    assert_rows_conserved,
)


def test_balanced_load_passes():
    assert_rows_conserved("bronze.orders", 1000, written_rows=980, deduped_rows=20)


def test_all_three_terms_balance():
    assert_rows_conserved(
        "silver.orders",
        1000,
        written_rows=900,
        deduped_rows=40,
        quarantined_rows=60,
    )


def test_missing_rows_raise():
    # 50 rows went nowhere and nobody chose that -- the silent-loss case.
    with pytest.raises(RowConservationError, match="50 rows"):
        assert_rows_conserved("bronze.orders", 1000, written_rows=950)


def test_extra_rows_raise():
    # More rows out than in means a join fanned out; also a corruption, in the other direction.
    with pytest.raises(RowConservationError):
        assert_rows_conserved("bronze.orders", 1000, written_rows=1200)


def test_message_names_every_term():
    with pytest.raises(RowConservationError) as excinfo:
        assert_rows_conserved(
            "bronze.orders", 1000, written_rows=900, deduped_rows=10, quarantined_rows=5
        )

    message = str(excinfo.value)
    assert "written=900" in message
    assert "deduped=10" in message
    assert "quarantined=5" in message


def test_empty_source_is_balanced():
    assert_rows_conserved("bronze.orders", 0, written_rows=0)


def test_normal_duplicate_rate_passes():
    rate = assert_dedupe_rate_ok("bronze.orders", source_rows=1000, deduped_rows=3)
    assert rate == pytest.approx(0.003)


def test_mostly_duplicate_source_raises():
    # A restated row here and there is normal; half the file repeating is a broken extract.
    with pytest.raises(RowConservationError, match="500/1000"):
        assert_dedupe_rate_ok("bronze.orders", source_rows=1000, deduped_rows=500)


def test_small_source_is_exempt_from_dedupe_rate():
    rate = assert_dedupe_rate_ok("bronze.orders", source_rows=10, deduped_rows=9)
    assert rate == pytest.approx(0.9)


def test_empty_source_is_not_a_division_by_zero():
    assert assert_dedupe_rate_ok("bronze.orders", source_rows=0, deduped_rows=0) == 0.0
