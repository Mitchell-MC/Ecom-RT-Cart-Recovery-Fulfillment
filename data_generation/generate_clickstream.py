"""Generate synthetic ecommerce clickstream events into a date-partitioned raw landing zone.

Mimics what a real event pipeline (Kafka/Event Hubs -> Autoloader) would drop into
`bronze/clickstream/dt=YYYY-MM-DD/*.json` -- one JSON object per line, one file set per day.

Also writes `_converted_sessions.csv` / `_converted_session_items.csv` manifests describing
which sessions reached checkout_complete. `generate_orders_domain.py` optionally consumes these
manifests to build order records that are consistent with a subset of the clickstream (the rest
of order volume is generated independently, simulating phone/CS orders with no clickstream
trail -- see docs/metric-glossary.md for why silver resolves this join probabilistically rather
than assuming 1:1 clickstream-to-order coverage).

Usage:
    python generate_clickstream.py --out data_generation/output/clickstream --days 14
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from seed_catalog import build_customers, build_products

DEVICE_TYPES = ["desktop", "mobile", "mobile", "tablet"]  # mobile weighted heavier
PAGE_TYPES = ["home", "category", "product", "cart", "checkout"]

CART_ABANDON_AFTER_MIN = 30  # must match docs/metric-glossary.md cart_abandon_threshold_minutes


def hour_weight() -> int:
    """Weighted-random hour of day, skewed toward evening browsing."""
    weights = [1, 1, 1, 1, 1, 1, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 6, 7, 9, 9, 8, 6, 4, 2]
    return random.choices(range(24), weights=weights, k=1)[0]


def make_event(event_type: str, ts: datetime, session_id: str, customer_id: str,
                cart_id: str | None, device_type: str, extra: dict | None = None) -> dict:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": ts.isoformat(),
        "session_id": session_id,
        "customer_id": customer_id,
        "cart_id": cart_id,
        "device_type": device_type,
        # simulated ingestion latency: 1-45s after the event fires
        "_ingested_at": (ts + timedelta(seconds=random.randint(1, 45))).isoformat(),
    }
    if extra:
        event.update(extra)
    return event


def inject_bad_row(event: dict, rng: random.Random) -> dict:
    """Deliberately corrupt ~this event to exercise src/quality/dq_checks.py."""
    corruption = rng.choice(["null_price", "negative_price", "future_timestamp", "none"])
    if corruption == "null_price" and "price_at_event" in event:
        event["price_at_event"] = None
    elif corruption == "negative_price" and "price_at_event" in event:
        event["price_at_event"] = -abs(event["price_at_event"] or 1.0)
    elif corruption == "future_timestamp":
        future = datetime.fromisoformat(event["event_timestamp"]) + timedelta(days=2)
        event["event_timestamp"] = future.isoformat()
    return event


def generate_session(day: datetime, customer, products, rng: random.Random,
                      bad_row_rate: float):
    session_id = f"SESS-{uuid.uuid4()}"
    device_type = rng.choice(DEVICE_TYPES)
    start = day.replace(hour=hour_weight(), minute=rng.randint(0, 59), second=rng.randint(0, 59))
    ts = start
    events = []
    cart_id = None
    cart_items: dict[str, dict] = {}  # sku -> {qty, price}

    n_pageviews = rng.randint(1, 5)
    for _ in range(n_pageviews):
        page_type = rng.choice(PAGE_TYPES[:3])  # home/category/product browsing
        product = rng.choice(products)
        events.append(make_event("page_view", ts, session_id, customer.customer_id, cart_id,
                                  device_type, {"page_type": page_type, "sku": product.sku}))
        ts += timedelta(seconds=rng.randint(15, 240))

    converted = False
    checkout_complete_ts = None

    if rng.random() < 0.40:  # adds something to cart
        cart_id = f"CART-{uuid.uuid4()}"
        n_items = rng.randint(1, 3)
        for _ in range(n_items):
            product = rng.choice(products)
            qty = rng.randint(1, 3)
            evt = make_event("add_to_cart", ts, session_id, customer.customer_id, cart_id,
                              device_type,
                              {"sku": product.sku, "quantity": qty, "price_at_event": product.price})
            if rng.random() < bad_row_rate:
                evt = inject_bad_row(evt, rng)
            events.append(evt)
            cart_items[product.sku] = {"qty": qty, "price": product.price}
            ts += timedelta(seconds=rng.randint(10, 90))

        if rng.random() < 0.15 and cart_items:  # removes one item
            sku = rng.choice(list(cart_items.keys()))
            removed = cart_items.pop(sku)
            events.append(make_event("remove_from_cart", ts, session_id, customer.customer_id,
                                      cart_id, device_type,
                                      {"sku": sku, "quantity": removed["qty"],
                                       "price_at_event": removed["price"]}))
            ts += timedelta(seconds=rng.randint(10, 60))

        if cart_items and rng.random() < 0.55:  # proceeds to checkout
            events.append(make_event("checkout_start", ts, session_id, customer.customer_id,
                                      cart_id, device_type))
            ts += timedelta(seconds=rng.randint(30, 180))

            if rng.random() < 0.65:  # completes checkout -> becomes an order candidate
                events.append(make_event("checkout_complete", ts, session_id, customer.customer_id,
                                          cart_id, device_type))
                converted = True
                checkout_complete_ts = ts

    return events, cart_id, cart_items, converted, checkout_complete_ts, session_id


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="Output directory root")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD, defaults to today-days")
    parser.add_argument("--customers", type=int, default=2000)
    parser.add_argument("--products", type=int, default=400)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--daily-active-rate", type=float, default=0.06,
                         help="Fraction of customers who have >=1 session on a given day")
    parser.add_argument("--bad-row-rate", type=float, default=0.015,
                         help="Fraction of add_to_cart events deliberately corrupted for DQ testing")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    customers = build_customers(args.customers, args.seed)
    products = build_products(args.products, args.seed)

    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    if args.start_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start = (datetime.now(timezone.utc) - timedelta(days=args.days)).replace(
            hour=0, minute=0, second=0, microsecond=0)

    converted_sessions = []
    converted_items = []
    total_events = 0

    for day_offset in range(args.days):
        day = start + timedelta(days=day_offset)
        day_dir = out_root / f"dt={day.date().isoformat()}"
        day_dir.mkdir(parents=True, exist_ok=True)
        day_events = []

        active_today = [c for c in customers
                         if rng.random() < (args.daily_active_rate * (1.6 if c.is_returning else 1.0))]

        for customer in active_today:
            n_sessions = 1 if rng.random() < 0.85 else 2
            for _ in range(n_sessions):
                (events, cart_id, cart_items, converted,
                 checkout_ts, session_id) = generate_session(day, customer, products, rng,
                                                               args.bad_row_rate)
                day_events.extend(events)
                if converted and cart_id:
                    converted_sessions.append({
                        "cart_id": cart_id,
                        "customer_id": customer.customer_id,
                        "session_id": session_id,
                        "converted_at": checkout_ts.isoformat(),
                    })
                    for sku, item in cart_items.items():
                        converted_items.append({
                            "cart_id": cart_id, "sku": sku,
                            "qty": item["qty"], "price_at_add": item["price"],
                        })

        # simulate multiple producer partitions landing as separate files
        chunk_size = max(1, len(day_events) // 4 or 1)
        for i in range(0, len(day_events), chunk_size) or [0]:
            chunk = day_events[i:i + chunk_size]
            if not chunk:
                continue
            part_path = day_dir / f"part-{i // chunk_size:04d}.json"
            with part_path.open("w", encoding="utf-8") as f:
                for evt in chunk:
                    f.write(json.dumps(evt) + "\n")
        total_events += len(day_events)
        print(f"  {day.date().isoformat()}: {len(day_events)} events, "
              f"{len(active_today)} active customers")

    with (out_root / "_converted_sessions.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["cart_id", "customer_id", "session_id", "converted_at"])
        writer.writeheader()
        writer.writerows(converted_sessions)

    with (out_root / "_converted_session_items.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["cart_id", "sku", "qty", "price_at_add"])
        writer.writeheader()
        writer.writerows(converted_items)

    print(f"Done. {total_events} total events across {args.days} days. "
          f"{len(converted_sessions)} converted sessions written to _converted_sessions.csv")


if __name__ == "__main__":
    main()
