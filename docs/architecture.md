# Architecture

## System overview

See the ASCII diagram in [README.md](../README.md) for the high-level flow. This doc walks
through *why* each piece is shaped the way it is, not just what it does.

## Medallion layers

**Bronze** is a landing zone, not a quality gate. `src/ingestion/streaming/bronze_clickstream_stream.py`
and `src/ingestion/batch/bronze_orders_domain.py` land data with minimal transformation — type
casting and lineage metadata (`_bronze_ingested_at`, `_source_file`, `_batch_run_id`) only. Rows
are never dropped at bronze; Autoloader's rescued-data column and a permissive schema absorb
malformed upstream data rather than stalling ingestion. This matters because bronze is the
system's audit trail — if silver's DQ gate rejects something, you need the original row still
sitting in bronze to debug why.

**Silver** is where `src/quality/dq_checks.py` enforces the contract in
[metric-glossary.md](metric-glossary.md): null/referential/bounds checks, clock-skew guards, and
dedup. Silver stays normalized (one table per bronze source), so a schema change to `orders`
doesn't ripple into every consumer of `customers`.

**Gold** (`src/transform/gold/`) is where business logic and denormalization happen —
`gold_cart_recovery.py` and `gold_fulfillment_risk.py` implement the scoring formulas from the
metric glossary. Each writes two tables: an overwrite-mode current-snapshot table for the
DirectQuery ops consumers, and an insert-only `*_history` table (periodic snapshot fact — see
[docs/metric-glossary.md](metric-glossary.md#history-tables-periodic-snapshot-fact-pattern)) so
history survives being overwritten. `gold_exec_summary_marts.py` reads those history tables to
build the daily trend table for BI, rather than depending on its own schedule landing after a
fresh snapshot. Immediately before each write, both jobs call `assert_unique()`
(`src/quality/dq_checks.py`) against their documented grain (`cart_id` / `order_id`) — a fast-fail
guard against a source-side duplicate key silently fanning out a join (e.g. a duplicated
`shipments.order_id`) into a wrong-grain gold table, which a plain row-count check in
`gold.pipeline_audit_log` wouldn't catch. `src/common/catalog_docs.py` then pushes short
table/column descriptions, hand-paraphrased from the metric glossary, into Unity Catalog via
`COMMENT ON TABLE`/`ALTER COLUMN ... COMMENT`, so the contract is visible directly in Catalog
Explorer and not only in this markdown.

## Streaming vs. batch, and why both

Clickstream (behavioral, high-volume, naturally event-shaped) is Structured Streaming end to
end — bronze and silver are both continuous jobs (see `orchestration/databricks/resources/
streaming_jobs.yml`), targeting the <5min bronze / <15min silver freshness SLAs. Order-domain
data (transactional, naturally batch-shaped — an OMS export, not an event stream) is batch,
on a 15-minute schedule (`order_domain_ingest_job.yml`). This isn't a compromise; it reflects
what the two source systems actually are. Forcing the order domain into Structured Streaming
would mean either fabricating a CDC stream that doesn't exist in this project's scope, or
polling a batch export every few seconds for no freshness benefit.

## Why Databricks Workflows is primary and Airflow is the stretch target

Workflows keeps orchestration, compute, and Unity Catalog permissions in one control plane —
a task's cluster spins up already scoped to the right catalog/schema grants via the job's
service principal, with no separate credential to manage for "can Airflow reach Databricks."
For a platform whose only compute is Databricks, that's less moving parts, not fewer
capabilities. Airflow earns its place when you're orchestrating *across* systems Databricks
doesn't own (a dbt run elsewhere, a Fivetran sync, a Snowflake load) — this project doesn't have
that yet, so `orchestration/airflow/` exists to prove the pattern (see its README for exactly
what it does and doesn't cover) rather than to replace Workflows.

## Why Terraform stops at the workspace-object layer

Terraform in `infra/terraform/` provisions storage, Unity Catalog structure, compute policies,
and identity — slow-changing platform primitives. Job/pipeline *definitions* deploy via
Databricks Asset Bundles (`orchestration/databricks/`). A code-only PR that changes
`gold_cart_recovery.py`'s scoring formula should trigger `databricks bundle deploy`, not a
`terraform apply` — bundling job logic into Terraform state would mean every gold-layer PR waits
on a Terraform plan/apply cycle for no reason.

### Why we don't Terraform the metastore

A Unity Catalog metastore is an account-level, one-per-region resource, typically created once
by whoever bootstraps the Databricks account and assigned to every workspace in that region —
`infra/terraform/modules/unity-catalog` assumes one already exists (see
`infra/terraform/README.md` for the exact assumption). Re-provisioning a metastore per project
repo isn't how any real Databricks account is organized; modeling it as if it were would make
the Terraform harder to reason about, not easier.

## Security model

- **No stored secrets for CI**: GitHub Actions authenticates to Azure via OIDC federated
  credentials (`infra/terraform/modules/access`), then to Databricks via `azure-cli` auth,
  which rides the same federated session. See `.github/workflows/deploy-dev.yml`.
- **Application secrets** (anything a job needs at runtime, not CI) live in a Key
  Vault-backed Databricks secret scope, not a Databricks-managed one — Azure stays the source of
  truth, auditable and rotatable outside the Databricks control plane.
- **Least privilege via Unity Catalog grants**: `reader_groups` get `SELECT` on gold only; the
  pipeline service principal gets `CREATE_TABLE`/`MODIFY`/`SELECT` at the catalog level (it owns
  writing every layer), documented in `infra/terraform/modules/unity-catalog/main.tf`.

## Rule-based v1 signals, not ML

Both `priority_score` and `fulfillment_risk_score` are documented, auditable heuristics (see
`docs/metric-glossary.md`), not trained models. That's a scope decision, not a limitation
nobody noticed: a rule-based v1 ships without a labeled training set, is trivially explainable
to the CRM/Ops personas who have to act on it ("why is this cart scored 82?" has a one-line
answer), and gives a v2 propensity model a clean baseline to beat. Explicitly out of scope for
v1 — see `docs/project-charter.md`.

## No production environment

`dev` and `staging` are the only environments (see `docs/project-charter.md`). This is a
portfolio project, not a live revenue system, so a `prod` environment would be theater. The
`staging` promotion gate (manual approval in `promote-staging.yml`) is built to the same
standard a `staging -> prod` gate would use, so the pattern generalizes without needing a fake
`prod` to prove it.

## Verification

How each phase's output was checked, and what "done" means for each:

- **KPI reconciliation** — every gold-layer formula traces to a numbered section in
  `docs/metric-glossary.md`; a reviewer can diff the code against the doc directly.
- **Data quality** — `src/quality/dq_checks.py` enforces completeness, uniqueness (via dedup),
  timestamp validity (clock-skew guard), and referential checks at the bronze→silver boundary,
  plus a grain-level `assert_unique()` guard at the silver→gold boundary (one row per `cart_id` /
  `order_id` before each gold write); `tests/unit/test_dq_checks.py` covers the rule logic in
  isolation.
- **Streaming stability** — checkpointed Structured Streaming (`checkpointLocation` on every
  streaming job) gives deterministic reprocessing and crash recovery by construction; the
  continuous-job `max_retries`/auto-restart behavior in `streaming_jobs.yml` is the operational
  half of that story.
- **Performance benchmark** — `src/transform/silver/benchmark_layout.py`, methodology and
  hypothesis in `docs/distributed-compute-notes.md#5-benchmark-partition-only-vs-partition--z-order`.
- **Orchestration reliability** — every scheduled task has `max_retries`/`timeout_seconds`
  (`order_domain_ingest_job.yml`, `gold_jobs.yml`); `gold.pipeline_audit_log`
  (`src/common/audit.py`) is the backfill/audit record; `main_pipeline_job.yml` is the
  dependency-graph backfill path.
- **CI/CD gate + promotion** — `.github/workflows/ci.yml` gates every PR;
  `terraform-plan.yml` posts plan output for `infra/**` PRs; `deploy-dev.yml` auto-deploys
  `main`; `promote-staging.yml` requires a GitHub Environment approval before touching staging.
- **BI/gold parity** — `bi/powerbi/data-model.md` documents storage mode per table specifically
  so DirectQuery tables (`cart_recovery_signal`, `fulfillment_risk_signal`) never silently
  diverge from gold; there's nothing to reconcile because Power BI reads gold directly for those.
