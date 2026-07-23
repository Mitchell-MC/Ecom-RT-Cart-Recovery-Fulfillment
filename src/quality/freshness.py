"""Freshness gates derived from the SLA table in docs/project-charter.md.

Two related problems, one mechanism:

1. Gold jobs run on independent cron schedules rather than upstream dependencies -- deliberate,
   since three signals with three cadences shouldn't be welded into one DAG (see gold_jobs.yml).
   But an independent schedule means a gold job fires happily on top of stale silver, recomputes
   from three-day-old state, and writes a gold table stamped with a *current* timestamp. The
   dashboard then shows a recent refresh time over stale business state, which is worse than a
   visibly broken dashboard: it converts a contained upstream failure into confidently-wrong
   numbers that nobody has any reason to distrust. `assert_upstream_fresh` refuses that.

2. Nothing detects a job that never ran at all. A crashed run can log its own failure, but a run
   that was never scheduled, whose cluster failed to launch, or whose job was deleted has no
   process alive to report anything. Absence has to be detected from outside, on a timer, by
   something that knows what *should* be there -- see freshness_check.py, which is that timer.

Watermarks are read from the data, not from pipeline_audit_log. A job can succeed while writing
nothing (an empty source, a filter that silently matched no rows), so "the job ran" and "the data
is fresh" are different claims, and only the second one matters to a consumer.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Every silver table carries _silver_processed_at; bronze carries _bronze_ingested_at.
SILVER_WATERMARK = "_silver_processed_at"
BRONZE_WATERMARK = "_bronze_ingested_at"


class StaleUpstreamError(RuntimeError):
    """Raised when an upstream dataset is older than the consumer's SLA allows."""


@dataclass(frozen=True)
class UpstreamRequirement:
    layer: str
    table: str
    watermark_column: str
    max_age_minutes: int


# Budgets are the consumer's tolerance, not the producer's cadence: a gold job reading a table
# refreshed every 15 minutes needs slack for a late run plus its own runtime, otherwise the gate
# fires on healthy pipelines and gets muted -- which costs more than not having it.
UPSTREAM_REQUIREMENTS: dict[str, tuple[UpstreamRequirement, ...]] = {
    "gold_cart_recovery": (
        UpstreamRequirement("silver", "clickstream_events", SILVER_WATERMARK, 60),
        UpstreamRequirement("silver", "orders", SILVER_WATERMARK, 90),
    ),
    "gold_fulfillment_risk": (
        UpstreamRequirement("silver", "orders", SILVER_WATERMARK, 90),
        UpstreamRequirement("silver", "shipments", SILVER_WATERMARK, 90),
        UpstreamRequirement("silver", "order_items", SILVER_WATERMARK, 90),
        UpstreamRequirement("silver", "inventory", SILVER_WATERMARK, 90),
    ),
    # Daily rollup: tolerates a missed hourly/4-hourly run, but not a missed day -- reading
    # yesterday's gold into "today's" exec summary is the exact laundering this gate exists for.
    "gold_exec_summary_marts": (
        UpstreamRequirement("gold", "cart_recovery_signal", "_gold_computed_at", 1440),
        UpstreamRequirement(
            "gold", "fulfillment_risk_signal", "_gold_computed_at", 1440
        ),
        UpstreamRequirement("silver", "shipments", SILVER_WATERMARK, 1440),
        UpstreamRequirement("silver", "clickstream_events", SILVER_WATERMARK, 1440),
    ),
}


def table_watermark(
    spark: SparkSession, table: str, watermark_column: str
) -> datetime | None:
    """Newest watermark value in `table`, or None if the table is missing or empty."""
    if not spark.catalog.tableExists(table):
        return None
    row = spark.read.table(table).agg(F.max(watermark_column).alias("wm")).collect()[0]
    return row["wm"]


def staleness_report(
    spark: SparkSession, cfg, requirements: tuple[UpstreamRequirement, ...], now=None
) -> list[str]:
    """Return one human-readable line per breach; empty list means everything is inside SLA."""
    now = now or datetime.now(timezone.utc)
    breaches = []

    for req in requirements:
        table = cfg.table(req.layer, req.table)
        watermark = table_watermark(spark, table, req.watermark_column)

        if watermark is None:
            breaches.append(f"{table}: missing or empty (no {req.watermark_column})")
            continue

        # Spark returns naive timestamps in session-local time; the pipeline runs UTC end to end
        # (every cluster policy sets it), so attach UTC rather than silently comparing across
        # an unknown offset and getting an hours-wrong age.
        if watermark.tzinfo is None:
            watermark = watermark.replace(tzinfo=timezone.utc)

        age = now - watermark
        if age > timedelta(minutes=req.max_age_minutes):
            breaches.append(
                f"{table}: {_format_age(age)} old, budget {req.max_age_minutes}m "
                f"(watermark {watermark.isoformat()})"
            )

    return breaches


def assert_upstream_fresh(spark: SparkSession, cfg, job_name: str, now=None) -> None:
    """Fail the job if any declared upstream is outside its freshness budget.

    Failing here is the point: the alternative is publishing a gold table whose refresh
    timestamp says "seconds ago" over data that hasn't moved in days.
    """
    requirements = UPSTREAM_REQUIREMENTS.get(job_name)
    if not requirements:
        return

    breaches = staleness_report(spark, cfg, requirements, now=now)
    if breaches:
        raise StaleUpstreamError(
            f"{job_name}: refusing to publish on stale upstream data -- "
            + "; ".join(breaches)
        )


def _format_age(age: timedelta) -> str:
    minutes = int(age.total_seconds() // 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"
