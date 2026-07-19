# Ecommerce Signal Platform

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
  retries/timeouts/SLAs. Airflow DAG (orchestration/airflow) mirrors the same
  dependency graph as a stretch/alternate orchestrator.

  IaC: Terraform provisions the workspace-level objects (Unity Catalog
  catalog/schemas, cluster policies, secret scopes, job/pipeline permissions)
  for dev and staging as separate state.

  CI/CD: GitHub Actions lints + unit-tests every PR, deploys to dev on merge
  to main, and promotes dev → staging behind a manual approval gate.
```

Full write-up: [docs/architecture.md](docs/architecture.md). Distributed-compute tradeoffs
(partitioning, shuffle, streaming trigger intervals, layout benchmarks):
[docs/distributed-compute-notes.md](docs/distributed-compute-notes.md).

## Repo layout

```
docs/                   Charter, metric glossary, architecture, tradeoffs, demo scripts
infra/terraform/        Modules + dev/staging environments (Azure Databricks + Unity Catalog)
data_generation/        Synthetic clickstream + order-domain data generators
src/ingestion/          Bronze layer: Structured Streaming (clickstream) + batch (order domain)
src/transform/silver/   Standardization, dedup, keying
src/transform/gold/     Cart-recovery signal, fulfillment-risk signal, exec summary marts
src/quality/            Data quality contracts and checks
orchestration/databricks/  Databricks Asset Bundle + Workflow job definitions
orchestration/airflow/     Stretch: Airflow DAG mirroring the same pipeline
.github/workflows/      CI (lint/test), deploy-dev, promote-staging
tests/                  Unit tests for silver/gold logic and DQ checks
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

# 3. Deploy jobs via Databricks Asset Bundle
cd ../../../../orchestration/databricks
databricks bundle deploy -t dev
databricks bundle run -t dev main_pipeline

# 4. Point Power BI at the gold schema per bi/powerbi/data-model.md
```

## Talking points

If you're using this repo for interviews, start with
[docs/star-talking-points.md](docs/star-talking-points.md) and the two demo scripts in `docs/`
(10-minute and 30-minute variants).
