"""Volume plausibility: is this the right order of magnitude of data?

Every other guard on this pipeline checks the *shape* of the data -- null keys, CSV headers,
quarantine rates, freshness. None of them looks at how much of it there is, and that leaves one
silent-corruption path fully open: a truncated upstream export. 800 order rows arrive instead of
90,000. Every row is valid, the header matches, nothing quarantines, the data is fresh, every
job is green -- and gold recomputes revenue off 1% of the orders. Finance acts on the number.

Baselines come from gold.dataset_metrics, which every load writes to. That makes each run both
a check against history and a contribution to the baseline the next run is checked against.

Deliberately a wide band, not a tight one. This is a smoke alarm for order-of-magnitude
mistakes, not a forecasting model: real volume swings with weekday, promotions and season, and a
band tight enough to catch a 20% dip would fire every Monday and be muted within a fortnight.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# A truncated or duplicated export misses these by orders of magnitude; ordinary business
# variation does not come close to them.
MIN_RATIO = 0.25
MAX_RATIO = 4.0

# Below this many prior runs the average is not a baseline, it is a coincidence. New datasets
# and freshly-restored environments pass unchecked until there is enough history to judge.
MIN_RUNS_FOR_BASELINE = 5

# Datasets legitimately this small are dominated by noise -- one row either way moves the ratio
# more than the thresholds allow -- so the check would only ever produce false alarms.
MIN_BASELINE_ROWS = 50


class VolumeAnomaly(RuntimeError):
    """Raised when a load's row count is implausible against its own recent history."""


@dataclass(frozen=True)
class Baseline:
    dataset: str
    median_rows: float
    runs: int

    @property
    def usable(self) -> bool:
        return (
            self.runs >= MIN_RUNS_FOR_BASELINE and self.median_rows >= MIN_BASELINE_ROWS
        )


def trailing_baseline(
    spark: SparkSession, cfg, dataset: str, days: int = 14
) -> Baseline:
    """Median written_rows for `dataset` over the last `days`, from gold.dataset_metrics.

    Median rather than mean: the failure this guard exists to catch is exactly the kind of
    outlier that drags a mean toward itself. One truncated load in the window would lower a
    mean enough to help the next truncated load look normal.
    """
    metrics_table = cfg.table("gold", "dataset_metrics")
    if not spark.catalog.tableExists(metrics_table):
        return Baseline(dataset, 0.0, 0)

    history = spark.read.table(metrics_table).filter(
        (F.col("dataset") == dataset)
        & (F.col("measured_at") > F.expr(f"current_timestamp() - INTERVAL {days} DAYS"))
    )
    row = history.agg(
        F.percentile_approx("written_rows", 0.5).alias("median"),
        F.count(F.lit(1)).alias("runs"),
    ).collect()[0]

    return Baseline(dataset, float(row["median"] or 0.0), int(row["runs"] or 0))


def assert_volume_plausible(
    baseline: Baseline,
    written_rows: int,
    min_ratio: float = MIN_RATIO,
    max_ratio: float = MAX_RATIO,
) -> None:
    """Fail when `written_rows` is implausible against `baseline`.

    A no-op until there is enough history to judge against -- see Baseline.usable. Failing
    closed on an unknown baseline would block every new dataset and every restored environment
    on its first runs, which is how a check gets deleted rather than fixed.
    """
    if not baseline.usable:
        return

    ratio = written_rows / baseline.median_rows
    if ratio < min_ratio or ratio > max_ratio:
        direction = "far below" if ratio < min_ratio else "far above"
        raise VolumeAnomaly(
            f"{baseline.dataset}: {written_rows} rows is {direction} the recent median of "
            f"{baseline.median_rows:,.0f} over {baseline.runs} runs "
            f"(ratio {ratio:.2f}, "
            f"allowed {min_ratio}-{max_ratio}) -- treating an order-of-magnitude change as a "
            f"broken extract rather than a real business swing"
        )
