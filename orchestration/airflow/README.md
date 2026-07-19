# Airflow (stretch orchestrator)

`dags/ecom_signal_platform_dag.py` is an alternate orchestrator for the batch half of the
pipeline, included to demonstrate the same DAG expressed outside Databricks Workflows — not
because this project runs two orchestrators in production simultaneously (see
`docs/project-charter.md` and `docs/architecture.md` for why Databricks Workflows is primary).

## Wiring it up

1. Drop `dags/ecom_signal_platform_dag.py` into your Airflow deployment's `dags/` folder (or
   point `AIRFLOW__CORE__DAGS_FOLDER` at this directory).
2. `pip install -r requirements.txt` into the Airflow environment.
3. Create an Airflow connection `databricks_default` (Connections UI or
   `airflow connections add`) of type `Databricks`, pointing at the same workspace the Asset
   Bundle in `orchestration/databricks/` deployed jobs into. Use a PAT or OAuth service
   principal credential — the same `pipeline` service principal Terraform provisions in
   `infra/terraform/modules/access` works here too.
4. Set the Airflow Variable `ecom_env` to `dev` or `staging` (defaults to `dev`) — this is how
   the DAG resolves which Databricks Jobs to call by name (`[dev] order_domain_ingest`, etc.),
   since the same DAG file targets whichever environment's jobs it's pointed at.
5. Deploy the Databricks jobs first (`databricks bundle deploy -t <target>`) — this DAG triggers
   jobs by name via the Jobs API, it doesn't define them.

## What it does and doesn't cover

Covers the same task graph as `orchestration/databricks/resources/main_pipeline_job.yml`:
`order_domain_ingest -> {gold_cart_recovery, gold_fulfillment_risk} -> gold_exec_summary_marts`,
gated on a health check confirming both continuous streaming jobs are actively running. It does
not attempt to replicate the independently-scheduled cadences in `gold_jobs.yml` (hourly /
every-4h / daily) — see the module docstring for why that would need three DAGs, not one.
