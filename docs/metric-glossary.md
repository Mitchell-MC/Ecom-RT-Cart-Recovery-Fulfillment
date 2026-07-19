# Metric Glossary / KPI Contracts

Every metric below is a contract: grain, definition, source columns, and freshness are fixed
here and the gold-layer code must match this document. If code and this doc disagree, this doc
wins and the code has a bug.

## Conventions

- All monetary figures are in the order's transaction currency, converted to USD at the
  `fx_rate_to_usd` captured on the source row (no rate lookups at query time).
- All timestamps are stored as UTC in bronze/silver; presentation-layer (Power BI) localizes.
- "Session" = clickstream events from the same `session_id` with no gap > 30 minutes between
  consecutive events (standard web-analytics session cutoff).

---

### 1. Abandoned Cart

**Grain:** one row per `cart_id` per evaluation run.

**Definition:** A cart is *abandoned* if it has at least one `add_to_cart` event, no
`checkout_complete` event, and the most recent cart event is older than **30 minutes**
(`cart_abandon_threshold_minutes`) but younger than **7 days** (`cart_stale_horizon_days` — after
7 days we stop surfacing it as recoverable and it ages out of the signal).

**Source:** `silver.clickstream_events` filtered to `event_type in ('add_to_cart',
'remove_from_cart', 'checkout_start', 'checkout_complete')`, grouped by `cart_id`.

### 2. Recoverable Cart Value

**Grain:** one row per `cart_id`.

**Definition:** Sum of `line_item_price * quantity` for items currently in the cart (net of any
`remove_from_cart` events), using the last-known price at time of `add_to_cart`.

**Formula:**
```
recoverable_cart_value = SUM(price_at_add * qty_at_add) - SUM(price_at_remove * qty_at_remove)
                          over active (not-removed) line items in the cart
```

**Source:** `silver.clickstream_events` joined to `silver.products` for current price fallback
when `price_at_add` is null (bot/malformed event).

### 3. Cart Recovery Priority Score

**Grain:** one row per `cart_id`, computed in `gold.cart_recovery_signal`.

**Definition:** A 0–100 heuristic score combining recency, value, and customer history —
documented as a rule-based v1 score, explicitly *not* a trained propensity model (see
[project-charter.md](project-charter.md) scope boundaries).

**Formula:**
```
recency_score   = 100 * exp(-minutes_since_last_activity / 180)      # decays over ~3h half-life-ish
value_score     = 100 * min(recoverable_cart_value / value_p90, 1.0)  # capped at 90th pct cart value
loyalty_score   = 100 if customer has >=1 prior completed order else 40

priority_score  = 0.45 * recency_score + 0.40 * value_score + 0.15 * loyalty_score
```
`value_p90` is recomputed per run as the trailing-30-day 90th percentile of
`recoverable_cart_value` across all abandoned carts (keeps the score scale-stable as catalog
price mix shifts).

### 4. Recoverable Revenue (headline KPI)

**Grain:** aggregate, trailing N days (BI-selectable: 1/7/30).

**Definition:** `SUM(recoverable_cart_value)` across all currently-abandoned carts with
`priority_score >= 60` (the "worth acting on today" cutoff).

### 5. On-Time Delivery Rate

**Grain:** aggregate, trailing N days, computed over orders with a `delivered_at` timestamp.

**Definition:**
```
on_time_delivery_rate = COUNT(orders WHERE delivered_at <= promised_delivery_date)
                         / COUNT(orders WHERE delivered_at IS NOT NULL)
```

### 6. Fulfillment Risk Score

**Grain:** one row per `order_id`, computed in `gold.fulfillment_risk_signal`, for orders that
are not yet `delivered_at IS NOT NULL` and not `cancelled`.

**Definition:** A 0–100 heuristic combining time pressure and known risk factors.

**Formula:**
```
days_to_promise      = DATEDIFF(promised_delivery_date, current_date)
time_pressure_score  = 100 * (1 - min(max(days_to_promise, 0) / carrier_avg_transit_days, 1.0))
inventory_risk_score = 100 if any line item had a backorder/partial-pick event else 0
carrier_risk_score   = carrier_historical_late_rate_pct   # trailing-90-day rate, precomputed in silver

fulfillment_risk_score = 0.5 * time_pressure_score
                        + 0.3 * inventory_risk_score
                        + 0.2 * carrier_risk_score
```

### 7. Delayed-Order Revenue Risk (headline KPI)

**Grain:** aggregate, current snapshot.

**Definition:** `SUM(order_total_usd)` across open orders with `fulfillment_risk_score >= 60`.

### 8. Conversion Lag

**Grain:** aggregate, trailing N days.

**Definition:** For sessions that eventually convert (place an order within 7 days of first
session event), the median time between first `add_to_cart` and `checkout_complete`.

**Source:** `silver.clickstream_events` joined to `silver.orders` on `customer_id` +
`cart_id → order_id` linkage captured at checkout.

---

## Data quality gate tied to these definitions

Every metric above depends on fields validated by `src/quality/dq_checks.py` at the
bronze→silver boundary:

| Field | Check | Severity |
|---|---|---|
| `cart_id`, `session_id`, `order_id` | not null, referential match to a known session/customer | fail (row quarantined) |
| `event_timestamp`, `order_created_at` | not null, within [-1 day, +5 min] of ingestion time (clock-skew guard) | fail |
| `price_at_add`, `order_total_usd` | not null, >= 0 | fail |
| `promised_delivery_date` | not null for orders past `checkout_complete` | warn (excluded from fulfillment_risk until backfilled) |
| duplicate `(cart_id, event_type, event_timestamp)` | deduped, last-write-wins on `_ingested_at` | dedup, not a failure |

See [Data Quality Gates](../src/quality/dq_checks.py) for the executable version of this table.
