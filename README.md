# Ecommerce Signal Platform

[![CI](https://github.com/Mitchell-MC/Ecom-RT-Cart-Recovery-Fulfillment/actions/workflows/ci.yml/badge.svg)](https://github.com/Mitchell-MC/Ecom-RT-Cart-Recovery-Fulfillment/actions/workflows/ci.yml)

A portfolio-grade lakehouse platform that turns raw ecommerce clickstream and order data into two
operational signals a retail ops team would actually act on:

- **Cart recovery** — which abandoned carts are worth a recovery email/SMS today, ranked by
  recoverable revenue and urgency.
- **Fulfillment risk** — which open orders are at risk of missing their delivery promise, ranked
  by delayed-revenue exposure.

The project is built to demonstrate senior/staff-level platform engineering, not just a notebook:
medallion architecture on Databricks, Structured Streaming for behavioral data, Terraform-managed
infrastructure, Databricks Workflows as the primary orchestrator (with an Airflow-equivalent DAG
as a stretch target), and a full CI/CD promotion path from `dev` to `staging`.

## Why this exists

Most portfolio data projects stop at "I built a notebook that computes a metric." This one is
scoped to answer the question a hiring manager actually asks: *could this person own a production
data platform?* That means IaC-provisioned infrastructure, contract-driven ingestion, tested
transforms, observable orchestration, and a promotion pipeline — not just a clever query.

See [docs/project-charter.md](docs/project-charter.md) for the full problem statement, personas,
and scope boundaries.

## Architecture at a glance

```
                 ┌───────────────────────────────────────────────────────────┐
                 │                        Azure                              │
                 │                                                           │
  synthetic      │   ADLS Gen2 (raw landing)                                 │
  clickstream ───┼──▶  bronze/clickstream/*.json  ──▶ Structured Streaming   │
  events (JSON)  │                                          │                │
                 │                                          ▼                │
  order-domain   │   ADLS Gen2 (raw landing)          Unity Catalog          │
  CSV extracts ──┼──▶  bronze/orders/*.csv  ──▶ Batch  bronze.*  tables      │
  (orders,       │                                          │                │
   customers,    │                                          ▼                │
   inventory,    │                                    silver.*  tables       │
   shipments)    │                              (deduped, typed, keyed)      │
                 │                                          │                │
                 │                                          ▼                │
                 │                                     gold.*  marts         │
                 │                    (cart_recovery_signal, fulfillment_risk_signal,
                 │                     exec_summary marts)                   │
                 │                                          │                │
                 └──────────────────────────────────────────┼────────────────┘
                                                              ▼
                                                    Power BI ROI dashboard

  Orchestration: Databricks Workflows (primary) — src jobs run as tasks with
  retries/timeouts/SLAs, failure + health notifications, and a standalone
  freshness monitor that catches jobs which never ran. Airflow DAG
  (orchestration/airflow) mirrors the same dependency graph as a
  stretch/alternate orchestrator.

  IaC: Terraform provisions the workspace-level objects (Unity Catalog
  catalog/schemas, cluster policies, secret scopes, job/pipeline permissions)
  for dev and staging as separate state.

  CI/CD: GitHub Actions lints + unit-tests every PR, deploys to dev on merge
  to main, and promotes dev → staging behind a manual approval gate.
```

Full write-up: [docs/architecture.md](docs/architecture.md). Distributed-compute tradeoffs
(partitioning, shuffle, streaming trigger intervals, layout benchmarks):
[docs/distributed-compute-notes.md](docs/distributed-compute-notes.md).

## Failing loudly

The design goal for the pipeline's failure behaviour is that no bad outcome is silent. A pipeline
that fails visibly gets fixed in an hour; one that fails silently gets discovered by whoever is
reading the dashboard, days later, usually at month-end. Every control below exists because
something specific could otherwise go wrong while every job stayed green.

| Failure mode | What catches it |
|---|---|
| Job crashes | `on_failure` notification + a `status='failed'` row in `gold.pipeline_audit_log` |
| Job hangs | `RUN_DURATION_SECONDS` health rule (a hang never fires `on_failure`) |
| Stream alive but falling behind | `STREAMING_BACKLOG_SECONDS` health rule |
| Job never runs at all | `freshness_check` job, every 15 min — nothing inside a run can detect this |
| Consumer builds on stale upstream | `assert_upstream_fresh` before each gold publish |
| Truncated or duplicated extract | volume band vs. the dataset's own trailing median |
| Rows silently disappearing | row-conservation reconciliation: `source == written + deduped + quarantined` |
| Upstream contract change | `enforceSchema` on read, quarantine-rate ceiling, strict gold schemas |
| Duplicate source rows | dedupe before both write paths + a dedupe-rate ceiling |

Alerts carry business impact, the last known-good run, the owning team, and the upstream system
most likely responsible — `Job X failed` is a notification, not an alert. Two observability
tables back this: `gold.pipeline_audit_log` (one row per run) and `gold.dataset_metrics` (one row
per dataset per run, which also supplies the volume baselines).

When something does break: [docs/runbook.md](docs/runbook.md).

## Repo layout

```
docs/                   Charter, metric glossary, architecture, tradeoffs, demo scripts,
                        incident runbook, schema migrations
infra/terraform/        Modules + dev/staging environments (Azure Databricks + Unity Catalog)
data_generation/        Synthetic clickstream + order-domain data generators
src/common/             Config resolution, run audit log, Slack alerting
src/ingestion/          Bronze layer: Structured Streaming (clickstream) + batch (order domain)
src/transform/silver/   Standardization, dedup, keying
src/transform/gold/     Cart-recovery signal, fulfillment-risk signal, exec summary marts
src/quality/            DQ contracts, freshness gates, volume + row-count reconciliation
orchestration/databricks/  Databricks Asset Bundle + Workflow job definitions
orchestration/airflow/     Stretch: Airflow DAG mirroring the same pipeline
.github/workflows/      CI (lint/test), deploy-dev, promote-staging
tests/                  Unit tests for transforms, DQ, and the reliability controls
bi/powerbi/             Data model, DAX measures, and report layout for the ROI dashboard
```

## Status

This repo is a from-scratch build-out following the phased plan in
[docs/project-charter.md](docs/project-charter.md). Code targets Databricks Runtime (Spark
Structured Streaming + Delta Lake) and is designed to be deployed via the Terraform +
Databricks Asset Bundle path in `infra/` and `orchestration/databricks/` — it is not runnable
against a local Spark session in this environment (no JVM/Spark installed here). Unit tests run
against a local PySpark session in CI (GitHub Actions installs a JVM for that job); see
[.github/workflows/ci.yml](.github/workflows/ci.yml).

## Quickstart (once you have an Azure subscription + Databricks workspace)

```bash
# 1. Generate synthetic source data
python -m venv .venv && . .venv/Scripts/activate   # or source .venv/bin/activate on macOS/Linux
pip install -r data_generation/requirements.txt
python data_generation/generate_clickstream.py --out data_generation/output/clickstream --days 14
python data_generation/generate_orders_domain.py --out data_generation/output/orders --days 90

# 2. Provision dev infrastructure
cd infra/terraform/env/dev
terraform init
terraform apply -var-file=terraform.tfvars   # copy terraform.tfvars.example first

# 3. (Optional) Slack alerting. Without this, jobs run normally and alerts
#    degrade to a driver-log line -- nothing breaks, so dev can skip it.
databricks secrets create-scope ecom
databricks secrets put-secret ecom slack_webhook_url

# 4. Deploy jobs via Databricks Asset Bundle
cd ../../../../orchestration/databricks
# alert_email has no default on purpose: a job whose failure notifies nobody is
# indistinguishable from one that succeeded.
databricks bundle deploy -t dev --var alert_email=you@example.com
databricks bundle run -t dev main_pipeline

# 5. Point Power BI at the gold schema per bi/powerbi/data-model.md
```

Deploying to an environment that **already holds data** needs the schema migrations in
[docs/migrations.md](docs/migrations.md) applied first — gold tables are written without
`overwriteSchema` and silver is MERGEd without `autoMerge`, so a schema change is deliberate
rather than something a job does to itself at 3am. A brand-new environment needs nothing.

## Talking points

If you're using this repo for interviews, start with
[docs/star-talking-points.md](docs/star-talking-points.md) and the two demo scripts in `docs/`
(10-minute and 30-minute variants).

For the "walk me through a pipeline that failed silently" question, the material is the failure
table above plus [docs/runbook.md](docs/runbook.md) — specifically the ordering it argues for:
establish blast radius before touching anything, contain before reconstructing, communicate
before you have the full picture, and judge the monitoring rather than the bug in the
post-mortem. The commit history on the reliability work is deliberately written to explain *what
would have gone wrong*, not just what changed.
