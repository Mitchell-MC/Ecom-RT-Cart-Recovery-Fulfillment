"""Writes one row per job run to gold.pipeline_audit_log -- the freshness/observability record
the Data Platform Lead persona in docs/project-charter.md relies on instead of digging through
Databricks Workflow run history by hand. Called from the end of every bronze/silver/gold job.

Runs are recorded through the `job_run` context manager, which logs a row on the way out
whether the body succeeded or raised. Logging only on success (the original shape of this
module) makes the table structurally unable to express failure: a crashed run wrote no row at
all, so "broken" and "never scheduled" and "still running" were all just an absent row, and
nothing queries for absence. A status column nobody can ever set to "failed" is decoration.

Note the residual gap this does NOT close: a run that never starts -- cluster launch failure,
a paused schedule, a deleted job -- still writes nothing, because nothing is executing to write
it. Absence is caught from the outside by the freshness check task
(src/quality/freshness_check.py), not from in here.
"""

from __future__ import annotations

import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from alerting import post_alert, run_url

AUDIT_SCHEMA = StructType(
    [
        StructField("job_name", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("row_count", LongType(), True),
        StructField("quarantined_count", LongType(), True),
        StructField("run_id", StringType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("finished_at", TimestampType(), False),
        StructField("status", StringType(), False),
        # Populated on failure only: the exception type and message, so triage can start from
        # this table instead of hunting the run's driver logs in the Workflows UI.
        StructField("error_message", StringType(), True),
    ]
)


@dataclass
class RunResult:
    """Mutable handle the job body fills in; read by `job_run` when the body exits.

    Deliberately mutable and default-None: a job that fails partway never sets row_count, and
    a null row_count on a failed row is honest about that. Zero would not be.
    """

    row_count: int | None = None
    quarantined_count: int = 0


@contextmanager
def job_run(
    spark: SparkSession,
    cfg,
    *,
    job_name: str,
    layer: str,
    target_table: str,
):
    """Log exactly one audit row for the wrapped body, on both the success and failure paths.

    The exception is re-raised after logging, so the task still goes red in Workflows and the
    on_failure notification still fires -- this records the failure, it does not swallow it.
    """
    started_at = datetime.now(timezone.utc)
    result = RunResult()
    try:
        yield result
    except BaseException as exc:  # noqa: BLE001 -- logged and immediately re-raised
        # Best-effort: if the audit write itself fails (the usual cause being the same outage
        # that killed the job), the original exception is the one worth propagating.
        try:
            log_run(
                spark,
                cfg,
                job_name=job_name,
                layer=layer,
                target_table=target_table,
                row_count=result.row_count,
                quarantined_count=result.quarantined_count,
                started_at=started_at,
                status="failed",
                error_message=_format_error(exc),
            )
        except Exception:  # pragma: no cover -- diagnostic only
            traceback.print_exc()

        _alert_failure(
            spark,
            cfg,
            job_name=job_name,
            target_table=target_table,
            started_at=started_at,
            exc=exc,
        )
        raise
    else:
        log_run(
            spark,
            cfg,
            job_name=job_name,
            layer=layer,
            target_table=target_table,
            row_count=result.row_count,
            quarantined_count=result.quarantined_count,
            started_at=started_at,
            status="success",
        )


def _format_error(exc: BaseException, limit: int = 2000) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text[:limit]


def current_run_id(spark) -> str:
    """The Databricks job run id, or "manual" when it can't be read.

    On serverless / Spark Connect, spark.conf.get raises CONFIG_NOT_AVAILABLE for a
    non-allowlisted key even when a default is passed -- the default argument is not honoured
    the way it is on a classic cluster -- so this is guarded rather than relying on it.
    """
    try:
        return spark.conf.get("spark.databricks.job.runId", "manual")
    except Exception:
        return "manual"


def _alert_failure(
    spark: SparkSession,
    cfg,
    *,
    job_name: str,
    target_table: str,
    started_at,
    exc: BaseException,
) -> None:
    """Send the actionable Slack alert for a failed run. Never raises."""
    try:
        details = [
            ("Target", f"`{target_table}`"),
            ("Failed at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")),
            ("Error", f"`{_format_error(exc, limit=400)}`"),
        ]

        last_good = _last_successful_run(spark, cfg, job_name)
        # "Last good run" is what turns an alert into a scoped incident: it is the difference
        # between "this just broke" and "this has been broken for three days", and it decides
        # who needs to be told before anything is fixed.
        last_good_text = (
            last_good.strftime("%Y-%m-%d %H:%M UTC") if last_good else "none on record"
        )
        details.append(("Last good run", last_good_text))

        post_alert(
            title=f"Pipeline failure: {job_name}",
            job_name=job_name,
            env=cfg.env,
            details=details,
            run_url=run_url(spark),
        )
    except Exception:  # pragma: no cover -- alerting must never mask the real failure
        traceback.print_exc()


def _last_successful_run(spark: SparkSession, cfg, job_name: str):
    """Timestamp of this job's most recent success, or None if there isn't one on record."""
    audit_table = cfg.table("gold", "pipeline_audit_log")
    if not spark.catalog.tableExists(audit_table):
        return None

    row = (
        spark.read.table(audit_table)
        .filter((F.col("job_name") == job_name) & (F.col("status") == "success"))
        .agg(F.max("finished_at").alias("last_good"))
        .collect()[0]
    )
    return row["last_good"]


def log_run(
    spark: SparkSession,
    cfg,
    *,
    job_name: str,
    layer: str,
    target_table: str,
    row_count: int | None,
    started_at,
    quarantined_count: int = 0,
    status: str = "success",
    error_message: str | None = None,
) -> None:
    audit_table = cfg.table("gold", "pipeline_audit_log")
    run_id = current_run_id(spark)

    row = spark.createDataFrame(
        [
            {
                "job_name": job_name,
                "layer": layer,
                "target_table": target_table,
                "row_count": row_count,
                "quarantined_count": quarantined_count,
                "run_id": run_id,
                "started_at": started_at,
                "status": status,
                "error_message": error_message,
            }
        ],
        schema=_row_schema(),
    ).withColumn("finished_at", F.current_timestamp())

    if spark.catalog.tableExists(audit_table):
        # mergeSchema so the error_message column can land on a table written by an earlier
        # version of this module without a manual ALTER.
        row.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(audit_table)
    else:
        row.write.format("delta").saveAsTable(audit_table)


def _row_schema() -> StructType:
    return StructType([f for f in AUDIT_SCHEMA.fields if f.name != "finished_at"])


# One row per dataset per run, as opposed to pipeline_audit_log's one row per *run*. Kept
# separate rather than folded into that table because the two answer different questions and
# mixing grains in one table makes both awkward to query: pipeline_audit_log answers "did this
# job run and did it work", this answers "what happened to this dataset's rows".
#
# The row counts are recorded as a chain -- source -> written, with what was dropped in between
# -- because the interesting failures live in the gaps. A source that suddenly has 40% duplicate
# keys is a broken upstream export, but after dedupe it produces a perfectly clean bronze table
# and looks identical to a healthy load.
DATASET_METRICS_SCHEMA = StructType(
    [
        StructField("dataset", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("run_id", StringType(), True),
        StructField("source_rows", LongType(), True),
        StructField("written_rows", LongType(), True),
        StructField("deduped_rows", LongType(), True),
        StructField("quarantined_rows", LongType(), True),
        StructField("measured_at", TimestampType(), False),
    ]
)


def log_dataset_metrics(
    spark: SparkSession,
    cfg,
    *,
    dataset: str,
    layer: str,
    source_rows: int,
    written_rows: int,
    deduped_rows: int = 0,
    quarantined_rows: int = 0,
) -> None:
    """Record the row-count chain for one dataset in one run.

    Consumed by src/quality/volume.py, which compares written_rows against this table's own
    trailing history -- so every run both checks itself against the past and contributes the
    baseline the next run will be checked against.
    """
    metrics_table = cfg.table("gold", "dataset_metrics")
    run_id = current_run_id(spark)

    row = spark.createDataFrame(
        [
            {
                "dataset": dataset,
                "layer": layer,
                "run_id": run_id,
                "source_rows": source_rows,
                "written_rows": written_rows,
                "deduped_rows": deduped_rows,
                "quarantined_rows": quarantined_rows,
            }
        ],
        schema=_metrics_row_schema(),
    ).withColumn("measured_at", F.current_timestamp())

    if spark.catalog.tableExists(metrics_table):
        row.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(metrics_table)
    else:
        row.write.format("delta").saveAsTable(metrics_table)


def _metrics_row_schema() -> StructType:
    return StructType(
        [f for f in DATASET_METRICS_SCHEMA.fields if f.name != "measured_at"]
    )
