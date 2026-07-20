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
    run_id = spark.conf.get("spark.databricks.job.runId", "manual")

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
