"""Writes one row per job run to gold.pipeline_audit_log -- the freshness/observability record
the Data Platform Lead persona in docs/project-charter.md relies on instead of digging through
Databricks Workflow run history by hand. Called from the end of every bronze/silver/gold job.
"""

from __future__ import annotations

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
    ]
)


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
            }
        ],
        schema=_row_schema(),
    ).withColumn("finished_at", F.current_timestamp())

    if spark.catalog.tableExists(audit_table):
        row.write.format("delta").mode("append").saveAsTable(audit_table)
    else:
        row.write.format("delta").saveAsTable(audit_table)


def _row_schema() -> StructType:
    return StructType([f for f in AUDIT_SCHEMA.fields if f.name != "finished_at"])
