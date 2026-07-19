"""Stretch/alternate orchestrator: mirrors the same batch dependency graph as
orchestration/databricks/resources/main_pipeline_job.yml, but from Airflow instead of Databricks
Workflows.

Deliberately does NOT reimplement any Spark logic here -- Airflow just triggers the exact same
Databricks Jobs (via DatabricksRunNowOperator against the Databricks Jobs API) that the Asset
Bundle deploys, and waits on them. That's the realistic shape of "Airflow as orchestrator for
Databricks workloads": Airflow owns scheduling/dependencies/alerting, Databricks still owns the
compute. Requires an Airflow connection `databricks_default` (host + PAT or SP OAuth) pointing at
the same workspace the Asset Bundle deployed to.

Scope note: this DAG mirrors `main_pipeline` (the manual/demo/backfill graph), not the
independently-scheduled production cadences in gold_jobs.yml (hourly / every-4h / daily) --
matching those cadences in Airflow would mean three separate DAGs with different
`schedule_interval`s, one per gold job, all depending on the same upstream `order_domain_ingest`
DAG via a sensor. Left as the natural next step if Airflow became the primary orchestrator
rather than the stretch target; see docs/architecture.md.

The two streaming jobs (bronze_clickstream_stream, silver_clickstream) are continuous Databricks
Jobs managed outside any scheduler's run loop -- `check_streaming_jobs_healthy` below confirms
both are in a RUNNING state via the Jobs API before the batch DAG proceeds, rather than Airflow
trying to "start" them (they're already always-on).
"""
from __future__ import annotations

import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

ENV = Variable.get("ecom_env", default_var="dev")
DATABRICKS_CONN_ID = "databricks_default"

STREAMING_JOB_NAMES = [f"[{ENV}] bronze_clickstream_stream", f"[{ENV}] silver_clickstream"]


def check_streaming_jobs_healthy(**_):
    hook = DatabricksHook(databricks_conn_id=DATABRICKS_CONN_ID)
    unhealthy = []
    for job_name in STREAMING_JOB_NAMES:
        jobs = hook.list_jobs(job_name=job_name) or []
        if not jobs:
            unhealthy.append(f"{job_name}: not found")
            continue
        job_id = jobs[0]["job_id"]
        runs = hook.list_runs(job_id=job_id, active_only=True) or []
        if not runs or runs[0].get("state", {}).get("life_cycle_state") not in (
                "RUNNING", "PENDING"):
            unhealthy.append(f"{job_name}: no active run")

    if unhealthy:
        raise AirflowFailException(
            "Streaming prerequisite check failed -- gold signals would be built on stale silver "
            f"data: {unhealthy}")


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": datetime.timedelta(minutes=2),
}

with DAG(
    dag_id="ecom_signal_platform",
    description="Alternate orchestrator for the ecommerce signal platform batch DAG (stretch target; see module docstring)",
    schedule_interval="@hourly",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ecom-signal-platform", "stretch-orchestrator"],
) as dag:

    check_streaming = PythonOperator(
        task_id="check_streaming_jobs_healthy",
        python_callable=check_streaming_jobs_healthy,
    )

    order_domain_ingest = DatabricksRunNowOperator(
        task_id="order_domain_ingest",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name=f"[{ENV}] order_domain_ingest",
    )

    gold_cart_recovery = DatabricksRunNowOperator(
        task_id="gold_cart_recovery",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name=f"[{ENV}] gold_cart_recovery",
    )

    gold_fulfillment_risk = DatabricksRunNowOperator(
        task_id="gold_fulfillment_risk",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name=f"[{ENV}] gold_fulfillment_risk",
    )

    gold_exec_summary_marts = DatabricksRunNowOperator(
        task_id="gold_exec_summary_marts",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_name=f"[{ENV}] gold_exec_summary_marts",
    )

    check_streaming >> order_domain_ingest >> [gold_cart_recovery, gold_fulfillment_risk] \
        >> gold_exec_summary_marts
