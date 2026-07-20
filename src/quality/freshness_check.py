"""Standalone freshness monitor -- the task docs/project-charter.md's SLA table has always
claimed enforces the bronze SLA, which until now did not exist.

Everything else in this repo detects failure from *inside* a run: a job crashes, catches it,
logs it, exits non-zero, and Workflows notifies. That covers every failure mode except the one
that matters most here -- a run that never happens. A paused schedule, a deleted job, a cluster
that cannot launch, a quota denial: nothing is executing, so nothing reports, and the pipeline
is silently dead while every dashboard still renders yesterday's numbers. Absence can only be
detected by something that runs on its own schedule and knows what *should* be there.

Runs on the shortest cadence of anything it watches, so a breach is caught within roughly one
SLA period rather than whenever someone notices. Exits non-zero on breach, which is what turns
it into an alert: the on_failure notification on this job is the actual delivery mechanism.
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(_THIS_DIR, "../common"))
from alerting import post_alert, run_url  # noqa: E402
from config import get_config  # noqa: E402
from freshness import (  # noqa: E402
    BRONZE_WATERMARK,
    SILVER_WATERMARK,
    UpstreamRequirement,
    staleness_report,
)

# Budgets are the SLA from docs/project-charter.md plus headroom for one missed run, so a single
# late batch doesn't page anyone but a stopped pipeline does.
MONITORED: tuple[UpstreamRequirement, ...] = (
    # < 5 min streaming SLA; 20m tolerates a restart and its backlog drain.
    UpstreamRequirement("bronze", "clickstream_events", BRONZE_WATERMARK, 20),
    # < 30 min batch SLA on a 15-minute cadence: 60m is two missed runs.
    UpstreamRequirement("bronze", "orders", BRONZE_WATERMARK, 60),
    UpstreamRequirement("bronze", "shipments", BRONZE_WATERMARK, 60),
    UpstreamRequirement("silver", "clickstream_events", SILVER_WATERMARK, 30),
    UpstreamRequirement("silver", "orders", SILVER_WATERMARK, 60),
    UpstreamRequirement("silver", "shipments", SILVER_WATERMARK, 60),
    # Gold cadences from the same table: hourly, 4-hourly, daily.
    UpstreamRequirement("gold", "cart_recovery_signal", "_gold_computed_at", 150),
    UpstreamRequirement("gold", "fulfillment_risk_signal", "_gold_computed_at", 480),
)


def main():
    spark = SparkSession.builder.appName("freshness_check").getOrCreate()
    cfg = get_config()

    breaches = staleness_report(spark, cfg, MONITORED)

    if breaches:
        # Printed before raising so the driver log lists every breach, not just the first --
        # during an incident the shape of the breach set is the diagnosis. All of bronze stale
        # means ingestion stopped; one silver table stale means one job is wedged.
        print(f"FRESHNESS BREACH ({len(breaches)} dataset(s) outside SLA):")
        for breach in breaches:
            print(f"  - {breach}")

        # This is the alert that catches a pipeline nobody is running, so it is the one most
        # likely to be the first anyone hears of an outage. It names the datasets rather than
        # the task, because "freshness_check failed" tells the reader nothing they can act on.
        post_alert(
            title=f"Freshness SLA breach: {len(breaches)} dataset(s) stale",
            job_name="freshness_check",
            env=cfg.env,
            details=[("Stale datasets", "\n" + "\n".join(f"• {b}" for b in breaches))],
            run_url=run_url(spark),
        )
        raise SystemExit(1)

    print(f"freshness OK: {len(MONITORED)} datasets inside SLA")


if __name__ == "__main__":
    main()
