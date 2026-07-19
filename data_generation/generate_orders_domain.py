"""Generate synthetic order-domain data: customers, products, inventory, orders, order_items,
shipments -- the batch/transactional counterpart to generate_clickstream.py.

If --clickstream-manifest-dir is given (pointing at the --out dir used by
generate_clickstream.py), a subset of orders is built directly from that run's converted
sessions (`_converted_sessions.csv` / `_converted_session_items.csv`), so those orders share a
customer_id, cart_id, sku set, and timestamp with a real clickstream trail. The remaining order
volume is generated independently with no clickstream trail, simulating phone/customer-service
channel orders -- this is deliberate: see docs/metric-glossary.md and
docs/architecture.md for why silver resolves clickstream<->order linkage probabilistically
rather than assuming every order has upstream behavioral data.

Usage:
    python generate_orders_domain.py --out data_generation/output/orders --days 90 \
        --clickstream-manifest-dir data_generation/output/clickstream
"""
from __future__ import annotations

import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from seed_catalog import CARRIERS, build_customers, build_products

CURRENCIES = [("USD", 1.00, 0.85), ("CAD", 0.73, 0.10), ("EUR", 1.08, 0.05)]
ORDER_STATUSES_TERMINAL = ["delivered", "cancelled"]


def pick_currency(rng: random.Random):
    codes, rates, weights = zip(*CURRENCIES)
    idx = rng.choices(range(len(codes)), weights=weights, k=1)[0]
    return codes[idx], rates[idx]


def pick_carrier(rng: random.Random):
    return rng.choice(CARRIERS)


def build_inventory(products, rng: random.Random) -> list[dict]:
    rows = []
    for p in products:
        backorder = rng.random() < 0.05
        rows.append({
            "sku": p.sku,
            "warehouse_id": f"WH{rng.randint(1, 4)}",
            "on_hand_qty": 0 if backorder else rng.randint(5, 500),
            "backorder_flag": backorder,
            "snapshot_date": datetime.now(timezone.utc).date().isoformat(),
        })
    return rows


def build_shipment(order_created_at: datetime, now: datetime, rng: random.Random,
                    force_late: bool | None = None):
    carrier, avg_transit_days, late_rate_pct = pick_carrier(rng)
    promised_delivery_date = (order_created_at + timedelta(days=avg_transit_days)).date()
    shipped_at = order_created_at + timedelta(hours=rng.randint(2, 36))

    is_late = force_late if force_late is not None else (rng.random() * 100 < late_rate_pct)
    if is_late:
        actual_transit_days = avg_transit_days + rng.randint(2, 6)
    else:
        actual_transit_days = max(1, avg_transit_days - rng.randint(0, 1))
    delivered_at_candidate = shipped_at + timedelta(days=actual_transit_days)

    if delivered_at_candidate <= now:
        delivered_at = delivered_at_candidate
        status = "delivered"
    else:
        delivered_at = None
        status = "in_transit"

    return {
        "carrier": carrier,
        "promised_delivery_date": promised_delivery_date.isoformat(),
        "shipped_at": shipped_at.isoformat(),
        "delivered_at": delivered_at.isoformat() if delivered_at else None,
        "status": status,
        "carrier_avg_transit_days": avg_transit_days,
        "carrier_historical_late_rate_pct": late_rate_pct,
    }


def load_manifest(manifest_dir: Path):
    sessions_path = manifest_dir / "_converted_sessions.csv"
    items_path = manifest_dir / "_converted_session_items.csv"
    if not sessions_path.exists() or not items_path.exists():
        return [], {}

    with sessions_path.open(encoding="utf-8") as f:
        sessions = list(csv.DictReader(f))
    with items_path.open(encoding="utf-8") as f:
        items_by_cart: dict[str, list[dict]] = {}
        for row in csv.DictReader(f):
            items_by_cart.setdefault(row["cart_id"], []).append(row)
    return sessions, items_by_cart


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--customers", type=int, default=2000)
    parser.add_argument("--products", type=int, default=400)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--clickstream-manifest-dir", default=None)
    parser.add_argument("--extra-orders-per-day", type=int, default=25,
                         help="Orders/day with no clickstream trail (phone/CS channel)")
    parser.add_argument("--bad-row-rate", type=float, default=0.01)
    args = parser.parse_args()

    rng = random.Random(args.seed + 7)
    customers = build_customers(args.customers, args.seed)
    products = build_products(args.products, args.seed)
    products_by_sku = {p.sku: p for p in products}

    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)

    if args.start_date:
        start = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start = now - timedelta(days=args.days)

    orders, order_items, shipments = [], [], []

    # --- orders derived from the clickstream manifest (real cart -> order linkage) ---
    if args.clickstream_manifest_dir:
        sessions, items_by_cart = load_manifest(Path(args.clickstream_manifest_dir))
        for sess in sessions:
            cart_items = items_by_cart.get(sess["cart_id"], [])
            if not cart_items:
                continue
            order_id = f"ORD-{uuid.uuid4()}"
            created_at = datetime.fromisoformat(sess["converted_at"]) + timedelta(minutes=rng.randint(1, 5))
            currency, fx_rate = pick_currency(rng)
            order_total = sum(float(i["qty"]) * float(i["price_at_add"]) for i in cart_items)
            cancelled = rng.random() < 0.03
            ship = None if cancelled else build_shipment(created_at, now, rng)

            orders.append({
                "order_id": order_id, "customer_id": sess["customer_id"], "cart_id": sess["cart_id"],
                "order_created_at": created_at.isoformat(), "channel": "web",
                "currency": currency, "fx_rate_to_usd": fx_rate,
                "order_total_usd": round(order_total * fx_rate, 2) if order_total >= 0 else order_total,
                "status": "cancelled" if cancelled else ship["status"],
                "promised_delivery_date": None if cancelled else ship["promised_delivery_date"],
            })
            for item in cart_items:
                order_items.append({
                    "order_id": order_id, "sku": item["sku"], "qty": item["qty"],
                    "unit_price": item["price_at_add"],
                })
            if ship:
                shipments.append({"order_id": order_id, **{k: v for k, v in ship.items()
                                                             if k not in ("promised_delivery_date",)},
                                   "promised_delivery_date": ship["promised_delivery_date"]})

    # --- standalone orders with no clickstream trail (phone / customer-service channel) ---
    for day_offset in range(args.days):
        day = start + timedelta(days=day_offset)
        for _ in range(args.extra_orders_per_day):
            customer = rng.choice(customers)
            order_id = f"ORD-{uuid.uuid4()}"
            created_at = day.replace(hour=rng.randint(8, 20), minute=rng.randint(0, 59))
            if created_at > now:
                continue
            n_items = rng.randint(1, 4)
            chosen = rng.sample(products, k=min(n_items, len(products)))
            currency, fx_rate = pick_currency(rng)
            order_total = sum(rng.randint(1, 3) * p.price for p in chosen)

            bad_row = rng.random() < args.bad_row_rate
            cancelled = rng.random() < 0.03
            ship = None if cancelled else build_shipment(created_at, now, rng)

            orders.append({
                "order_id": order_id, "customer_id": customer.customer_id, "cart_id": None,
                "order_created_at": created_at.isoformat(),
                "channel": rng.choice(["phone", "customer_service"]),
                "currency": currency, "fx_rate_to_usd": fx_rate,
                "order_total_usd": round(-abs(order_total) if bad_row else order_total * fx_rate, 2),
                "status": "cancelled" if cancelled else ship["status"],
                "promised_delivery_date": None if (cancelled or bad_row) else ship["promised_delivery_date"],
            })
            for p in chosen:
                order_items.append({
                    "order_id": order_id, "sku": p.sku, "qty": rng.randint(1, 3), "unit_price": p.price,
                })
            if ship:
                shipments.append({"order_id": order_id, **{k: v for k, v in ship.items()
                                                             if k not in ("promised_delivery_date",)},
                                   "promised_delivery_date": ship["promised_delivery_date"]})

    def write_csv(name: str, rows: list[dict]):
        if not rows:
            return
        path = out_root / name
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        print(f"  wrote {len(rows):>6} rows -> {path}")

    write_csv("customers.csv", [{
        "customer_id": c.customer_id, "first_seen_date": c.first_seen_date,
        "is_returning": c.is_returning, "home_tz_offset": c.home_tz_offset,
    } for c in customers])
    write_csv("products.csv", [{
        "sku": p.sku, "name": p.name, "category": p.category, "price": p.price,
    } for p in products])
    write_csv("inventory.csv", build_inventory(products, rng))
    write_csv("orders.csv", orders)
    write_csv("order_items.csv", order_items)
    write_csv("shipments.csv", shipments)

    print(f"Done. {len(orders)} orders "
          f"({sum(1 for o in orders if o['cart_id'])} from clickstream, "
          f"{sum(1 for o in orders if not o['cart_id'])} standalone) over {args.days} days.")


if __name__ == "__main__":
    main()
