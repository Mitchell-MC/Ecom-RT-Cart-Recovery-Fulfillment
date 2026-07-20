"""Executable version of the data-quality contract table in docs/metric-glossary.md.

Every silver job builds a list of DQRule and calls apply_dq_rules() at the bronze->silver
boundary. `fail` rules quarantine the row (excluded from silver, written to a `_quarantine`
Delta table for later inspection/backfill); `warn` rules keep the row in silver but tag it with
`_dq_warnings` so downstream gold logic can choose to exclude it (e.g. fulfillment_risk excludes
orders warned for missing promised_delivery_date rather than guessing a promise date).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class DQRule:
    name: str
    severity: str  # "fail" | "warn"
    condition: Column  # evaluates True when the row VIOLATES the rule

    def __post_init__(self):
        assert self.severity in ("fail", "warn"), f"unknown severity: {self.severity}"


def not_null(*cols: str) -> Column:
    return reduce(lambda a, b: a | b, [F.col(c).isNull() for c in cols])


def non_negative(col: str) -> Column:
    return F.col(col).isNotNull() & (F.col(col) < 0)


def within_clock_skew(col: str, past_days: int = 1, future_minutes: int = 5) -> Column:
    too_old = F.col(col) < F.expr(f"current_timestamp() - INTERVAL {past_days} DAYS")
    too_new = F.col(col) > F.expr(
        f"current_timestamp() + INTERVAL {future_minutes} MINUTES"
    )
    return F.col(col).isNull() | too_old | too_new


def apply_dq_rules(df: DataFrame, rules: list[DQRule]) -> tuple[DataFrame, DataFrame]:
    """Returns (clean_df, quarantine_df). `clean_df` carries a `_dq_warnings` array column
    (empty array if no warn-level rule fired); `quarantine_df` carries `_dq_fail_reasons`.
    """
    fail_rules = [r for r in rules if r.severity == "fail"]
    warn_rules = [r for r in rules if r.severity == "warn"]

    fail_condition = reduce(
        lambda a, b: a | b, [r.condition for r in fail_rules], F.lit(False)
    )
    fail_reasons = (
        F.array_compact(
            F.array(*[F.when(r.condition, F.lit(r.name)) for r in fail_rules])
        )
        if fail_rules
        else F.array().cast("array<string>")
    )

    warn_reasons = (
        F.array_compact(
            F.array(*[F.when(r.condition, F.lit(r.name)) for r in warn_rules])
        )
        if warn_rules
        else F.array().cast("array<string>")
    )

    tagged = df.withColumn("_dq_fail_reasons", fail_reasons).withColumn(
        "_dq_warnings", warn_reasons
    )

    quarantine_df = tagged.filter(fail_condition).drop("_dq_warnings")
    clean_df = tagged.filter(~fail_condition).drop("_dq_fail_reasons")
    return clean_df, quarantine_df


def dedupe_last_write_wins(
    df: DataFrame, key_cols: list[str], order_col: str
) -> DataFrame:
    """Collapses duplicate (key_cols) rows, keeping the one with the max order_col --
    used for producer-retry duplicates (see docs/metric-glossary.md dedup rule)."""
    from pyspark.sql.window import Window

    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_dq_row_num", F.row_number().over(w))
        .filter(F.col("_dq_row_num") == 1)
        .drop("_dq_row_num")
    )


class QuarantineRateExceeded(RuntimeError):
    """Raised when so much of a batch failed DQ that the batch itself is suspect."""


# A few bad rows are normal operation. A large fraction failing means something structural
# changed -- an upstream rename, a unit change, a timezone shift -- and quarantining most of a
# batch row-by-row is the pipeline reporting success while publishing a fraction of the data.
MAX_QUARANTINE_RATE = 0.25
MIN_ROWS_FOR_RATE_CHECK = 100


def assert_quarantine_rate_ok(
    dataset: str,
    clean_count: int,
    quarantined_count: int,
    max_rate: float = MAX_QUARANTINE_RATE,
    min_rows: int = MIN_ROWS_FOR_RATE_CHECK,
) -> float:
    """Return the quarantine rate, raising if it exceeds `max_rate` on a large enough batch.

    Small batches are exempt: 2 bad rows out of 5 is a 40% rate and means nothing, and a check
    that fires on those gets muted, taking the useful signal with it. Below `min_rows` the rate
    is computed and returned for logging but never raises.
    """
    total = clean_count + quarantined_count
    if total == 0:
        return 0.0

    rate = quarantined_count / total
    if total >= min_rows and rate > max_rate:
        raise QuarantineRateExceeded(
            f"{dataset}: {quarantined_count}/{total} rows ({rate:.1%}) failed DQ, "
            f"threshold {max_rate:.0%} -- treating this as a bad batch rather than "
            f"publishing the {clean_count} rows that happened to pass"
        )
    return rate


def write_quarantine(quarantine_df: DataFrame, target_table: str) -> None:
    if quarantine_df.head(1):
        (
            quarantine_df.withColumn("_quarantined_at", F.current_timestamp())
            .write.format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )
