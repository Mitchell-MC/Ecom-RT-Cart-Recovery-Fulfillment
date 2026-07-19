"""Spark-free, pandas port of the gold layer for iterating on the Power BI report before a real
Databricks workspace exists.

This is a PREVIEW tool, not the source of truth: `src/transform/gold/*.py` (PySpark, deployed to
Databricks) is the real gold logic and this script must be kept in sync with it by hand -- if
they diverge, the PySpark version wins (see docs/metric-glossary.md). This exists purely because
the dev machine has no JVM, so there's no way to run the real Spark jobs locally; this lets you
open Power BI Desktop today against real-shaped numbers instead of waiting on cloud provisioning.

"Now" is pinned to the latest event timestamp in the generated clickstream data (not wall-clock
time), so the output stays meaningful no matter how long after generation you run this -- real
wall-clock "now" would drift past the synthetic data's date range and make every cart look stale.

Usage:
    python build_local_gold_preview.py \
        --clickstream-dir ../../data_generation/output/clickstream \
        --orders-dir ../../data_generation/output/orders \
        --out output
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd

CART_EVENT_TYPES = ["add_to_cart", "remove_from_cart", "checkout_start", "checkout_complete"]
CART_ABANDON_THRESHOLD_MINUTES = 30
CART_STALE_HORIZON_DAYS = 7
TRAILING_WINDOW_DAYS = 30
PRIORITY_ACTION_THRESHOLD = 60
RISK_ACTION_THRESHOLD = 60


def load_clickstream(clickstream_dir: Path) -> pd.DataFrame:
    files = glob.glob(str(clickstream_dir / "dt=*" / "*.json"))
    records = []
    for f in files:
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    df = pd.DataFrame.from_records(records)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], utc=True, format="ISO8601")
    return df


def load_orders_domain(orders_dir: Path) -> dict[str, pd.DataFrame]:
    tables = {}
    for name in ["customers", "products", "inventory", "orders", "order_items", "shipments"]:
        path = orders_dir / f"{name}.csv"
        tables[name] = pd.read_csv(path) if path.exists() else pd.DataFrame()

    for col in ["order_created_at"]:
        if col in tables["orders"].columns:
            tables["orders"][col] = pd.to_datetime(tables["orders"][col], utc=True, format="ISO8601")
    if "promised_delivery_date" in tables["orders"].columns:
        tables["orders"]["promised_delivery_date"] = pd.to_datetime(
            tables["orders"]["promised_delivery_date"]).dt.date
    for col in ["shipped_at", "delivered_at"]:
        if col in tables["shipments"].columns:
            tables["shipments"][col] = pd.to_datetime(tables["shipments"][col], utc=True, format="ISO8601")
    if "promised_delivery_date" in tables["shipments"].columns:
        tables["shipments"]["promised_delivery_date"] = pd.to_datetime(
            tables["shipments"]["promised_delivery_date"]).dt.date
    return tables


# ---------------------------------------------------------------------------
# Cart recovery signal -- pandas port of src/transform/gold/gold_cart_recovery.py
# ---------------------------------------------------------------------------

def aggregate_carts(events: pd.DataFrame) -> pd.DataFrame:
    cart_events = events[events["event_type"].isin(CART_EVENT_TYPES)].copy()
    cart_events["add_value"] = cart_events["price_at_event"].fillna(0) * cart_events["quantity"].fillna(0)
    cart_events["add_value"] = cart_events["add_value"].where(cart_events["event_type"] == "add_to_cart", 0.0)
    cart_events["remove_value"] = (
        cart_events["price_at_event"].fillna(0) * cart_events["quantity"].fillna(0)
    ).where(cart_events["event_type"] == "remove_from_cart", 0.0)

    grouped = cart_events.groupby("cart_id").agg(
        customer_id=("customer_id", "first"),
        last_activity_at=("event_timestamp", "max"),
        has_add=("event_type", lambda s: int((s == "add_to_cart").any())),
        has_checkout_complete=("event_type", lambda s: int((s == "checkout_complete").any())),
        added_value=("add_value", "sum"),
        removed_value=("remove_value", "sum"),
        n_adds=("event_type", lambda s: int((s == "add_to_cart").sum())),
        n_removes=("event_type", lambda s: int((s == "remove_from_cart").sum())),
    ).reset_index()
    return grouped


def filter_abandoned(cart_agg: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    abandoned = cart_agg[
        (cart_agg["has_add"] == 1)
        & (cart_agg["has_checkout_complete"] == 0)
        & (cart_agg["last_activity_at"] < as_of - pd.Timedelta(minutes=CART_ABANDON_THRESHOLD_MINUTES))
        & (cart_agg["last_activity_at"] > as_of - pd.Timedelta(days=CART_STALE_HORIZON_DAYS))
    ].copy()
    abandoned["recoverable_cart_value"] = (abandoned["added_value"] - abandoned["removed_value"]).clip(lower=0)
    abandoned["item_count"] = (abandoned["n_adds"] - abandoned["n_removes"]).clip(lower=0)
    abandoned["minutes_since_last_activity"] = (
        (as_of - abandoned["last_activity_at"]).dt.total_seconds() / 60.0
    )
    return abandoned


def score_carts(abandoned: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    if abandoned.empty:
        return abandoned.assign(recency_score=[], value_score=[], loyalty_score=[], priority_score=[])

    value_p90 = abandoned["recoverable_cart_value"].quantile(0.90) or 1.0
    value_p90 = value_p90 if value_p90 > 0 else 1.0

    prior_orders = (
        orders[orders["status"] != "cancelled"].groupby("customer_id").size().rename("prior_order_count")
    )
    scored = abandoned.merge(prior_orders, on="customer_id", how="left")
    scored["prior_order_count"] = scored["prior_order_count"].fillna(0)
    scored["has_prior_order"] = scored["prior_order_count"] > 0

    scored["recency_score"] = 100 * np.exp(-scored["minutes_since_last_activity"] / 180.0)
    scored["value_score"] = 100 * (scored["recoverable_cart_value"] / value_p90).clip(upper=1.0)
    scored["loyalty_score"] = scored["has_prior_order"].map({True: 100.0, False: 40.0})
    scored["priority_score"] = (
        0.45 * scored["recency_score"] + 0.40 * scored["value_score"] + 0.15 * scored["loyalty_score"]
    )
    return scored[["cart_id", "customer_id", "last_activity_at", "minutes_since_last_activity",
                    "recoverable_cart_value", "item_count", "recency_score", "value_score",
                    "loyalty_score", "priority_score"]]


def build_cart_recovery_signal(events: pd.DataFrame, orders: pd.DataFrame, as_of: pd.Timestamp) -> pd.DataFrame:
    return score_carts(filter_abandoned(aggregate_carts(events), as_of), orders)


# ---------------------------------------------------------------------------
# Fulfillment risk signal -- pandas port of src/transform/gold/gold_fulfillment_risk.py
# ---------------------------------------------------------------------------

def build_fulfillment_risk_signal(orders: pd.DataFrame, shipments: pd.DataFrame,
                                   order_items: pd.DataFrame, inventory: pd.DataFrame,
                                   as_of: pd.Timestamp) -> pd.DataFrame:
    open_orders = orders[
        (orders["status"] == "in_transit") & orders["promised_delivery_date"].notna()
    ].merge(
        shipments[["order_id", "carrier", "carrier_avg_transit_days", "carrier_historical_late_rate_pct"]],
        on="order_id", how="inner",
    )
    if open_orders.empty:
        return open_orders

    backorder_by_order = (
        order_items.merge(inventory[["sku", "backorder_flag"]], on="sku", how="left")
        .groupby("order_id")["backorder_flag"].apply(lambda s: bool(s.fillna(False).any()))
        .rename("has_backordered_item")
    )
    open_orders = open_orders.merge(backorder_by_order, on="order_id", how="left")
    open_orders["has_backordered_item"] = open_orders["has_backordered_item"].fillna(False)

    as_of_date = as_of.date()
    open_orders["days_to_promise"] = open_orders["promised_delivery_date"].apply(
        lambda d: (d - as_of_date).days)
    open_orders["time_pressure_score"] = 100 * (
        1 - (open_orders["days_to_promise"].clip(lower=0) / open_orders["carrier_avg_transit_days"]).clip(upper=1.0)
    )
    open_orders["inventory_risk_score"] = open_orders["has_backordered_item"].map({True: 100.0, False: 0.0})
    open_orders["carrier_risk_score"] = open_orders["carrier_historical_late_rate_pct"]
    open_orders["fulfillment_risk_score"] = (
        0.5 * open_orders["time_pressure_score"]
        + 0.3 * open_orders["inventory_risk_score"]
        + 0.2 * open_orders["carrier_risk_score"]
    )
    return open_orders[["order_id", "customer_id", "order_total_usd", "carrier",
                         "promised_delivery_date", "days_to_promise", "time_pressure_score",
                         "inventory_risk_score", "carrier_risk_score", "fulfillment_risk_score"]]


# ---------------------------------------------------------------------------
# Exec summary trend -- one row per day in the generated date range, each computed "as of" that
# day's end so the report has an actual multi-day trend to chart, not a single snapshot point.
#
# Asymmetric fidelity, worth knowing before reading too much into the shape of the trend:
# - recoverable_revenue is a *faithful* historical reconstruction -- it's derived purely from
#   raw clickstream event timestamps filtered to <= as_of, so "abandoned as of day D" is exactly
#   what filter_abandoned() would have said on day D.
# - delayed_order_revenue_risk is NOT: `orders.status` / `shipments.status` in the generated CSVs
#   reflect the order's *final* outcome as of true wall-clock generation time (delivered vs.
#   still in_transit), not what its status would have been as of the historical `as_of` cutoff.
#   An order that was in transit on day D but has since delivered (by real "now") won't show up
#   as at-risk on day D here, even though it genuinely was. Early days in the trend will
#   under-count in-transit orders for this reason -- expect delayed_order_revenue_risk to look
#   artificially low/zero for older days and only become meaningful near the most recent days.
#   The real gold_fulfillment_risk.py job doesn't have this issue: it only ever evaluates the
#   true current state, never a simulated historical cutoff.
# ---------------------------------------------------------------------------

def build_exec_summary_daily(events: pd.DataFrame, orders: pd.DataFrame, shipments: pd.DataFrame,
                              order_items: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    all_days = sorted(events["event_timestamp"].dt.date.unique())
    rows = []
    for day in all_days:
        as_of = pd.Timestamp(day, tz="UTC") + pd.Timedelta(hours=23, minutes=59)

        cart_signal = build_cart_recovery_signal(events, orders, as_of)
        recoverable_revenue = (
            cart_signal.loc[cart_signal["priority_score"] >= PRIORITY_ACTION_THRESHOLD, "recoverable_cart_value"].sum()
            if not cart_signal.empty else 0.0
        )

        risk_signal = build_fulfillment_risk_signal(orders, shipments, order_items, inventory, as_of)
        delayed_revenue = (
            risk_signal.loc[risk_signal["fulfillment_risk_score"] >= RISK_ACTION_THRESHOLD, "order_total_usd"].sum()
            if not risk_signal.empty else 0.0
        )

        window_start = as_of - pd.Timedelta(days=TRAILING_WINDOW_DAYS)
        recent_shipments = shipments[
            shipments["delivered_at"].notna()
            & (shipments["delivered_at"] >= window_start)
            & (shipments["delivered_at"] <= as_of)
        ]
        if len(recent_shipments):
            on_time = (
                recent_shipments["delivered_at"].dt.date <= recent_shipments["promised_delivery_date"]
            ).mean()
        else:
            on_time = None

        conv = events[events["event_type"].isin(["add_to_cart", "checkout_complete"])
                       & events["cart_id"].notna()]
        per_cart = conv.groupby("cart_id").agg(
            first_add_at=("event_timestamp", lambda s: s[conv.loc[s.index, "event_type"] == "add_to_cart"].min()),
            checkout_complete_at=("event_timestamp",
                                    lambda s: s[conv.loc[s.index, "event_type"] == "checkout_complete"].max()),
        )
        per_cart = per_cart.dropna()
        per_cart = per_cart[(per_cart["checkout_complete_at"] >= window_start)
                             & (per_cart["checkout_complete_at"] <= as_of)]
        if len(per_cart):
            lag_minutes = (per_cart["checkout_complete_at"] - per_cart["first_add_at"]).dt.total_seconds() / 60.0
            conversion_lag_median = lag_minutes.median()
        else:
            conversion_lag_median = None

        rows.append({
            "metric_date": day.isoformat(),
            "recoverable_revenue": round(float(recoverable_revenue), 2),
            "delayed_order_revenue_risk": round(float(delayed_revenue), 2),
            "on_time_delivery_rate": round(float(on_time), 4) if on_time is not None else None,
            "conversion_lag_median_minutes": round(float(conversion_lag_median), 1)
                if conversion_lag_median is not None else None,
        })
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clickstream-dir", required=True)
    parser.add_argument("--orders-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    events = load_clickstream(Path(args.clickstream_dir))
    tables = load_orders_domain(Path(args.orders_dir))
    as_of_now = events["event_timestamp"].max()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    cart_recovery = build_cart_recovery_signal(events, tables["orders"], as_of_now)
    cart_recovery.to_csv(out_dir / "gold_cart_recovery_signal.csv", index=False)
    print(f"gold_cart_recovery_signal.csv: {len(cart_recovery)} abandoned carts (as of {as_of_now})")

    fulfillment_risk = build_fulfillment_risk_signal(
        tables["orders"], tables["shipments"], tables["order_items"], tables["inventory"], as_of_now)
    fulfillment_risk.to_csv(out_dir / "gold_fulfillment_risk_signal.csv", index=False)
    print(f"gold_fulfillment_risk_signal.csv: {len(fulfillment_risk)} open at-risk orders")

    exec_summary = build_exec_summary_daily(
        events, tables["orders"], tables["shipments"], tables["order_items"], tables["inventory"])
    exec_summary.to_csv(out_dir / "gold_exec_summary_daily.csv", index=False)
    print(f"gold_exec_summary_daily.csv: {len(exec_summary)} daily rows")

    tables["customers"].to_csv(out_dir / "silver_customers.csv", index=False)
    tables["products"].to_csv(out_dir / "silver_products.csv", index=False)
    print("silver_customers.csv / silver_products.csv copied through for dimension slicers")


if __name__ == "__main__":
    main()
