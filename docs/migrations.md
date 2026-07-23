# Migrations

Schema changes that existing environments need applied before or during a deploy. Gold tables
are written without `overwriteSchema` and silver tables are MERGEd without `autoMerge`, so a
schema change is a deliberate step here rather than something a job does to itself at 3am. That
is the intended trade: the cost is this file, the benefit is that a transform bug can no longer
silently republish a different schema to Power BI.

Order matters — apply these **before** deploying the bundle, or the first run of each affected
job will fail on schema mismatch.

## Observability branch (`fix/job-params-argv`)

Three changes, two of which need action on environments that already have data.

### 1. `_silver_processed_at` on the order-domain silver tables — **action required**

`silver_orders_domain` now stamps its own watermark. `merge_into_silver` does a MERGE with
`whenNotMatchedInsertAll()`, which fails when the source carries a column the target lacks.

```sql
ALTER TABLE ecom_<env>.silver.customers    ADD COLUMN _silver_processed_at TIMESTAMP;
ALTER TABLE ecom_<env>.silver.products     ADD COLUMN _silver_processed_at TIMESTAMP;
ALTER TABLE ecom_<env>.silver.inventory    ADD COLUMN _silver_processed_at TIMESTAMP;
ALTER TABLE ecom_<env>.silver.orders       ADD COLUMN _silver_processed_at TIMESTAMP;
ALTER TABLE ecom_<env>.silver.order_items  ADD COLUMN _silver_processed_at TIMESTAMP;
ALTER TABLE ecom_<env>.silver.shipments    ADD COLUMN _silver_processed_at TIMESTAMP;
```

Existing rows keep NULL until their next merge. That matters for one deploy cycle: the
`freshness_check` job reads `max(_silver_processed_at)`, and `max()` over all-NULL returns NULL,
which the check reports as "missing or empty". Either run `order_domain_ingest` once immediately
after deploying, or expect one freshness alert per silver table until it next runs.

### 2. `_gold_computed_at` on the gold signal tables — **action required**

Both gold signal jobs stamp this, and they no longer pass `overwriteSchema`.

```sql
ALTER TABLE ecom_<env>.gold.cart_recovery_signal    ADD COLUMN _gold_computed_at TIMESTAMP;
ALTER TABLE ecom_<env>.gold.fulfillment_risk_signal ADD COLUMN _gold_computed_at TIMESTAMP;
```

These tables are full overwrites, so the column populates on the next run — no backfill needed.

Note the ordering trap: `gold_exec_summary_marts` gates on `_gold_computed_at` being no more
than 24h old on **both** signal tables. Until each has run once post-migration, the daily rollup
will fail its freshness gate. That is the gate working correctly, but it will look like a broken
deploy if you hit it at 06:00 without expecting it. Run both signal jobs once after migrating.

### 3. `error_message` on `gold.pipeline_audit_log` — no action

Written with `mergeSchema`, so it lands on the existing table by itself. Deliberately the only
place in the pipeline that evolves its own schema: it is an internal operational log with no
external consumers, and an audit write that fails because of its own schema would suppress
exactly the record needed to diagnose the failure that triggered it.

`gold.dataset_metrics` is new and is created on first write.

## Adding a column to a gold table, generally

1. Add the column to this file with the `ALTER TABLE` statement.
2. Apply it to each environment (dev first).
3. Deploy the code that writes it.
4. Tell whoever owns the Power BI dataset — a new column is additive and safe, but a *renamed*
   or retyped one is not, and gold is a published contract.

For a rename or a type change, prefer adding the new column, backfilling, moving consumers, then
dropping the old one. A single destructive `ALTER` is what `overwriteSchema` used to do
implicitly, and the reason it was removed.
