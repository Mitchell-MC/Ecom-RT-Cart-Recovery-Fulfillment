# Project Charter — Ecommerce Signal Platform

## Problem statement

A mid-size DTC ecommerce retailer has clickstream and order data spread across systems but no
platform that turns it into daily operational signal. Two revenue-relevant questions go
unanswered every day:

1. **Which abandoned carts should we try to recover today, and how much revenue is at stake?**
2. **Which open orders are at risk of missing their delivery promise, and how much revenue/CS
   cost does that expose us to?**

This project builds the lakehouse platform that answers both, continuously, with the operational
maturity (IaC, orchestration, CI/CD, data quality gates) a real platform team would require before
trusting it with revenue decisions.

## Stakeholder personas

| Persona | Role | What they need | How they consume it |
|---|---|---|---|
| VP of Growth / CRM Lead | Owns retention & recovery campaigns | Ranked list of recoverable-revenue carts, refreshed intraday | `gold.cart_recovery_signal` + Power BI card "Recoverable Revenue (7d)" |
| Fulfillment Ops Manager | Owns on-time delivery SLA | Ranked list of at-risk orders before the promise date slips | `gold.fulfillment_risk_signal` + Power BI card "Delayed-Order Revenue Risk" |
| Data Platform Lead (you) | Owns the pipeline | Freshness, DQ pass rate, job success/retry history | Databricks Workflow run history + `gold.pipeline_audit_log` |
| CFO / Exec sponsor | Wants ROI narrative | Weekly rollup: recovered revenue attributable to the signal, delivery risk trend | Power BI exec summary page |

## Scope

**In scope (v1):**
- Medallion lakehouse (bronze/silver/gold) on Databricks + Unity Catalog
- Structured Streaming ingestion for clickstream behavioral events
- Batch ingestion for order-domain data (orders, order items, customers, inventory, shipments)
- Data quality contracts enforced at bronze→silver boundary
- Two gold signal models: abandoned-cart recovery, fulfillment risk
- Exec summary marts for BI
- Terraform-provisioned dev/staging environments
- Databricks Workflows as primary orchestrator; Airflow DAG as a stretch alternate
- GitHub Actions CI/CD with dev→staging promotion behind manual approval
- Power BI ROI dashboard

**Explicitly out of scope (v1):**
- Real-time alerting/paging integrations (Slack/PagerDuty on signal thresholds)
- ML propensity scoring (v1 signals are rule/heuristic-based and documented as such — see
  [metric-glossary.md](metric-glossary.md) for exact logic; a propensity model is a natural v2)
- Multi-region disaster recovery
- Advanced cost showback/chargeback tooling
- Production-scale synthetic data volume (data generators target realistic *shape*, not
  production *scale* — see [distributed-compute-notes.md](distributed-compute-notes.md) for how
  the benchmark still produces a meaningful skew/shuffle comparison at smaller scale)

## Environments

| Environment | Purpose | Promotion path |
|---|---|---|
| `dev` | Feature development, manual testing | Every merge to `main` auto-deploys here |
| `staging` | Pre-production validation, demo environment | Manual approval gate in GitHub Actions promotes `dev` → `staging` |

No `prod` environment in v1 — this is a portfolio project, not a live revenue system. The
promotion pipeline is built to the same standard a `staging → prod` gate would use, so the story
generalizes in an interview ("this is identical to how I'd gate a prod promotion, we just don't
have a production ecommerce system behind it").

## Freshness SLAs

| Dataset | Freshness target | Enforced by |
|---|---|---|
| `bronze.clickstream_events` | < 5 min from event to bronze (streaming) | Structured Streaming trigger interval + `pipeline_audit_log` watermark check |
| `bronze.orders*` | < 30 min from batch drop to bronze | Databricks Workflow schedule + freshness check task |
| `silver.*` | < 15 min after bronze (streaming), < 30 min after bronze (batch) | Downstream task timeout in the Workflow job |
| `gold.cart_recovery_signal` | Refreshed hourly | Databricks Workflow schedule (cron, hourly) |
| `gold.fulfillment_risk_signal` | Refreshed every 4 hours | Databricks Workflow schedule |
| `gold.exec_summary_*` | Refreshed daily (06:00 UTC) | Databricks Workflow schedule |

## Repository working agreements

- **Branching:** trunk-based. Short-lived feature branches (`feat/…`, `fix/…`, `infra/…`) off
  `main`, merged via PR. No long-lived environment branches — environment is a deploy target
  (Terraform workspace / Asset Bundle target), not a git branch.
- **PR standard:** every PR must (a) pass CI (lint + unit tests), (b) include a one-line "why"
  in the description, (c) for `infra/**` changes, include the `terraform plan` output as a PR
  comment (posted automatically by the `terraform-plan.yml` workflow).
- **Commit style:** imperative mood, scoped prefix where useful (`silver:`, `infra:`, `ci:`).
- **Naming conventions:**
  - Unity Catalog: catalog per environment (`ecom_dev`, `ecom_staging`), schemas `bronze`,
    `silver`, `gold` within each catalog.
  - Tables: `snake_case`, singular domain + plural entity (`bronze_clickstream_events`,
    `silver_orders`, `gold_cart_recovery_signal`).
  - ADLS containers: `bronze`, `silver`, `gold`, `checkpoints`, mirrored per environment via
    separate storage accounts (`st<project><env><suffix>`).
- **Release strategy:** no versioned releases in v1 (continuous deploy to `dev`/`staging`); the
  git tag `v1-portfolio` marks the state used for interview demos.

## Success criteria

See the verification checklist in the top-level plan / [docs/architecture.md](docs/architecture.md#verification):
KPI reconciliation, DQ pass rates, streaming checkpoint-recovery test, layout benchmark,
orchestration reliability (retries/backfill), CI/CD gate + promotion, BI/gold parity, and a
rehearsed end-to-end demo.
