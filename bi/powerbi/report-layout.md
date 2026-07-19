# Power BI Report Layout

Three pages, one per persona in `docs/project-charter.md`. Built against the model in
`data-model.md` using the measures in `measures.dax`.

## Page 1 — Exec Summary (CFO / exec sponsor)

**Top row — 4 KPI cards** (each with a small trend sparkline underneath, `Days` slicer top-right
controlling every trailing-N measure on the page):
1. **Recoverable Revenue (Trailing N)** — card + sparkline over `dim_date`, WoW % as a
   secondary label from `Cart Recovery Rate WoW %`.
2. **Delayed-Order Revenue Risk (Trailing N)**.
3. **On-Time Delivery Rate Trend** — formatted as a percentage, colored (green ≥95%, amber
   90–95%, red <90%; thresholds are placeholders — replace with the retailer's actual delivery
   SLA once this connects to a real fulfillment contract).
4. **Median Conversion Lag (Minutes)**.

**Bottom half:** two line charts (`gold.exec_summary_daily` over `dim_date`) —
Recoverable Revenue trend and Delayed-Order Revenue Risk trend, so the exec sponsor can see
whether the platform's signals are trending the business toward or away from the ROI story
described in `docs/star-talking-points.md`.

## Page 2 — Cart Recovery Ops (CRM / Growth lead)

- **Slicers:** `Cart Priority Tier` (multi-select), customer `is_returning` (new vs. returning).
- **Live card:** `Recoverable Revenue (Today, Live)` — DirectQuery, no trailing-window slicer
  applies (this page is about *acting right now*, not trend).
- **Main table**, sorted by `priority_score` descending: `cart_id`, `customer_id`,
  `minutes_since_last_activity`, `recoverable_cart_value`, `item_count`, `Cart Priority Tier`
  (conditional-formatted background), `priority_score`. Row-level detail expands (drillthrough)
  to a **Cart Detail** page showing the underlying `silver.clickstream_events` for that
  `cart_id` — useful for the "why is this cart scored this way" question a CRM analyst will
  actually ask.
- **Distribution chart:** histogram of `priority_score` across all currently-abandoned carts, so
  the CRM lead can judge whether today's 60+ cutoff is capturing the right volume for their
  outreach capacity (a full-width campaign vs. a hand-picked top-20 list).

## Page 3 — Fulfillment Risk Ops (Fulfillment Ops manager)

- **Slicers:** `Fulfillment Risk Tier`, `carrier`.
- **Live card:** `Delayed-Order Revenue Risk (Live)` and `At-Risk Order Count`.
- **Main table**, sorted by `fulfillment_risk_score` descending: `order_id`, `customer_id`,
  `carrier`, `promised_delivery_date`, `days_to_promise`, `order_total_usd`,
  `Fulfillment Risk Tier`, `fulfillment_risk_score` (with `time_pressure_score` /
  `inventory_risk_score` / `carrier_risk_score` available as tooltip-page detail — the ops
  manager needs to know *why* an order is flagged, not just that it is, to decide whether the
  fix is "call the carrier" or "expedite from another warehouse").
- **Carrier scorecard** (small multiples or a bar chart): count of at-risk orders and average
  `carrier_risk_score` per carrier — surfaces a systemic carrier problem vs. one-off orders.

## Build notes

- Every page uses the same `dim_date` and `Days` parameter table for slicer consistency, even
  though pages 2/3 mostly use the *live* DirectQuery cards rather than the trailing-window
  measures — kept in the model so a future "how did this list look 7 days ago" ask doesn't
  require a model change, just a new visual.
- Row-level security is out of scope for v1 (see `docs/project-charter.md` scope boundaries) —
  every `reader_groups` member sees every customer/order. A real deployment would add RLS roles
  scoped by territory/account-owner before shipping this to a CRM team.
