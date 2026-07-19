# Power BI Data Model

> **No Databricks workspace yet?** See [`bi/local_preview/`](../local_preview/README.md) for a
> Spark-free way to generate CSVs shaped exactly like the tables below, so you can build this
> report today and swap the source later without changing the model.

## Connection

Power BI connects to the Databricks SQL warehouse Terraform provisions
(`infra/terraform/modules/compute` → `databricks_sql_endpoint.bi`, output as `sql_warehouse_id`)
via the native **Databricks connector** (Get Data → Databricks), authenticating with
Azure AD SSO (the analyst's own AAD identity, scoped by the `reader_groups` grants in
`infra/terraform/modules/unity-catalog`) rather than a service principal — so row-level
permissions and audit trail match the person actually looking at the report.

Server hostname / HTTP path come from the SQL warehouse's connection details in the Databricks
UI; catalog is `ecom_staging` for the shared/demo report (`ecom_dev` for local iteration while
building the report).

## Storage mode per table

| Source table | Storage mode | Why |
|---|---|---|
| `gold.cart_recovery_signal` | **DirectQuery** | Ops-facing, refreshed hourly at the source; DirectQuery means the CRM lead always sees the current snapshot without waiting on a Power BI refresh schedule. Table is small (one row per currently-abandoned cart), so DirectQuery latency is acceptable. |
| `gold.fulfillment_risk_signal` | **DirectQuery** | Same reasoning; refreshed every 4h at the source. |
| `gold.exec_summary_daily` | **Import** | Historical trend table, append-only, small (one row/day). Import gives instant-feeling trend charts and lets DAX time-intelligence functions (which need a proper date table) work without hitting the warehouse per interaction. |
| `silver.products`, `silver.customers` (category/segment attributes only) | **Import**, scheduled refresh every 6h | Small dimension tables used purely for slicers/drilldowns on the two DirectQuery fact tables — imported so slicer interactions don't add warehouse round-trips on every click. |

Mixing DirectQuery and Import in one model requires **Composite Models** (enabled by default in
current Power BI Desktop); tables map to Dual/DirectQuery/Import storage per the table above via
each table's Properties pane.

## Relationships

```
dim_date (Import, generated via DAX CALENDAR())
    │ 1:*
    ▼
gold.exec_summary_daily (Import)   -- metric_date

silver.customers (Import) ──1:*──▶ gold.cart_recovery_signal (DirectQuery)   -- customer_id
silver.products  (Import)          -- not directly joined to cart_recovery_signal (no sku
                                       column at that grain); used for category-level BI in a
                                       future v2 line-item-grain cart signal, noted but not wired.

silver.customers (Import) ──1:*──▶ gold.fulfillment_risk_signal (DirectQuery)  -- customer_id
```

`dim_date` is a standard DAX-generated calendar table (`CALENDAR(DATE(2024,1,1), TODAY())`,
marked as the model's official Date Table) — required for the trailing-N-day and
period-over-period measures in `measures.dax`.

## Why two DirectQuery tables aren't joined to each other

`cart_recovery_signal` and `fulfillment_risk_signal` are independent signals about different
entities (a cart vs. an order) that happen to share `customer_id`. Power BI doesn't allow a
direct relationship between two DirectQuery tables from different "DirectQuery groups" unless
they're on the same source with query folding support for the join — both are on the same
Databricks warehouse here, so a relationship on `customer_id` *is* technically supported, but
it's deliberately not added: it would let a filter on one ops page silently affect the other,
which isn't how the CRM lead and Fulfillment Ops manager personas in
`docs/project-charter.md` actually use the report (they work these signals independently). Both
instead relate only to the shared `silver.customers` dimension.
