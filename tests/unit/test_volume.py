"""Tests for the volume plausibility band.

The failure this guards is a truncated upstream export: valid rows, matching header, fresh
timestamps, nothing quarantined, and 1% of the expected volume. Every other check on this
pipeline passes it.

trailing_baseline is not exercised here (it is a Delta read); the Baseline it returns is
constructed directly so the band logic and the not-enough-history behaviour can be pinned down.
"""

import pytest

from volume import (
    MIN_BASELINE_ROWS,
    MIN_RUNS_FOR_BASELINE,
    Baseline,
    VolumeAnomaly,
    assert_volume_plausible,
)

USABLE = Baseline("bronze.orders", median_rows=10_000, runs=20)


def test_normal_variation_passes():
    assert_volume_plausible(USABLE, 11_500)
    assert_volume_plausible(USABLE, 8_000)


def test_truncated_export_raises():
    # The motivating case: 800 rows where 90k were expected.
    with pytest.raises(VolumeAnomaly, match="far below"):
        assert_volume_plausible(USABLE, 800)


def test_duplicated_export_raises():
    with pytest.raises(VolumeAnomaly, match="far above"):
        assert_volume_plausible(USABLE, 90_000)


def test_empty_load_raises():
    with pytest.raises(VolumeAnomaly):
        assert_volume_plausible(USABLE, 0)


def test_message_carries_the_numbers_needed_to_judge_it():
    with pytest.raises(VolumeAnomaly) as excinfo:
        assert_volume_plausible(USABLE, 500)

    message = str(excinfo.value)
    assert "500 rows" in message
    assert "10,000" in message  # the baseline it was judged against
    assert "20 runs" in message  # how much history that baseline rests on


def test_too_few_runs_is_a_no_op():
    # A new dataset must not fail closed on its first loads, or the check gets deleted.
    thin = Baseline("bronze.orders", median_rows=10_000, runs=MIN_RUNS_FOR_BASELINE - 1)
    assert_volume_plausible(thin, 5)


def test_tiny_dataset_is_a_no_op():
    # One row either way swamps the ratio on a dataset this small.
    tiny = Baseline("bronze.warehouses", median_rows=MIN_BASELINE_ROWS - 1, runs=50)
    assert_volume_plausible(tiny, 1)


def test_no_history_is_a_no_op():
    assert_volume_plausible(Baseline("bronze.new_table", 0.0, 0), 1_000_000)


def test_boundary_ratios_pass():
    # Exactly at the band edges is allowed; the check is for order-of-magnitude misses.
    assert_volume_plausible(USABLE, 2_500)  # ratio 0.25
    assert_volume_plausible(USABLE, 40_000)  # ratio 4.0


@pytest.mark.parametrize("written", [2_499, 40_001])
def test_just_outside_the_band_raises(written):
    with pytest.raises(VolumeAnomaly):
        assert_volume_plausible(USABLE, written)
