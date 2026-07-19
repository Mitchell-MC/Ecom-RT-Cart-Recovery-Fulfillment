# Local Power BI Preview (no Databricks workspace required)

Lets you build and iterate on the Power BI report today, against real-shaped numbers, before
`infra/terraform` has been applied to a real Azure subscription. Not the source of truth — see
the module docstring in `build_local_gold_preview.py` for exactly how it differs from the real
PySpark gold jobs in `src/transform/gold/`, including one real asymmetry in how faithfully the
`delayed_order_revenue_risk` trend reconstructs history vs. `recoverable_revenue`.

## Generate the data

```bash
# from repo root, using the project's venv
.venv/Scripts/python.exe data_generation/generate_clickstream.py \
    --out data_generation/output/clickstream --days 21 --customers 800 --products 150
.venv/Scripts/python.exe data_generation/generate_orders_domain.py \
    --out data_generation/output/orders --days 21 --customers 800 --products 150 \
    --extra-orders-per-day 20 --clickstream-manifest-dir data_generation/output/clickstream

.venv/Scripts/python.exe -m pip install -r bi/local_preview/requirements.txt
.venv/Scripts/python.exe bi/local_preview/build_local_gold_preview.py \
    --clickstream-dir data_generation/output/clickstream \
    --orders-dir data_generation/output/orders \
    --out bi/local_preview/output
```

This writes five CSVs to `bi/local_preview/output/`:

| File | Maps to | Grain |
|---|---|---|
| `gold_cart_recovery_signal.csv` | `gold.cart_recovery_signal` | one row per currently-abandoned cart |
| `gold_fulfillment_risk_signal.csv` | `gold.fulfillment_risk_signal` | one row per open at-risk order |
| `gold_exec_summary_daily.csv` | `gold.exec_summary_daily` | one row per day (trend) |
| `silver_customers.csv` | `silver.customers` | dimension, for slicers |
| `silver_products.csv` | `silver.products` | dimension, for slicers |

## Point Power BI at it

1. Power BI Desktop → **Get Data** → **Text/CSV** → select each of the five files above (Import
   mode — there's no warehouse to DirectQuery yet).
2. In Power Query, set types explicitly rather than trusting auto-detect: `last_activity_at` /
   `promised_delivery_date` / `metric_date` as Date/DateTime, the `*_score` and revenue columns
   as Decimal Number.
3. Model tab → mark `gold_exec_summary_daily[metric_date]` as a Date table, or add a proper
   `dim_date` via `CALENDAR()` per `bi/powerbi/data-model.md` — needed for any time-intelligence
   DAX.
4. Relationships: `silver_customers[customer_id]` (1) → `gold_cart_recovery_signal[customer_id]`
   (*) and → `gold_fulfillment_risk_signal[customer_id]` (*). Column names match
   `bi/powerbi/data-model.md` exactly so this is a drop-in for the DirectQuery version later.
5. Paste the measures from `bi/powerbi/measures.dax` into Modeling → New Measure one at a time
   (Power BI Desktop has no bulk-DAX-file import). Table names in those measures
   (`gold_cart_recovery_signal`, `gold_exec_summary_daily`, etc.) already match this CSV import's
   default table names, so they should resolve without editing.
6. Build the three pages per `bi/powerbi/report-layout.md`.

## Swapping to the real Databricks warehouse later

Once `infra/terraform` is applied and the Databricks jobs have run against real data:
Get Data → Databricks → point at the `sql_warehouse_id` Terraform output, set storage mode per
`bi/powerbi/data-model.md`, and re-point each visual's table to the new Databricks-backed table
of the same name. The DAX measures don't need to change — they reference column/table names, not
the data source.

## Regenerating with fresh data

Re-run the three commands above any time — `build_local_gold_preview.py` always re-derives "now"
from the latest event in the clickstream data, so the output stays internally consistent no
matter when you run it relative to when the CSVs were generated.
