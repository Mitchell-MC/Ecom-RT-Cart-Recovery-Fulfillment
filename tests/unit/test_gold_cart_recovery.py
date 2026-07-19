from datetime import datetime, timedelta, timezone

from gold_cart_recovery import aggregate_carts, filter_abandoned, score_carts

NOW = datetime.now(timezone.utc)


def _event(cart_id, customer_id, event_type, minutes_ago, price=None, qty=None):
    return {
        "cart_id": cart_id,
        "customer_id": customer_id,
        "event_type": event_type,
        "event_timestamp": NOW - timedelta(minutes=minutes_ago),
        "price_at_event": price,
        "quantity": qty,
    }


def _events_df(spark):
    rows = [
        # cart A: abandoned 45 min ago, within the 30min-7day window -> should be scored
        _event("cart-A", "CUST1", "add_to_cart", 45, price=50.0, qty=2),
        # cart B: too recent (10 min ago) -> excluded
        _event("cart-B", "CUST2", "add_to_cart", 10, price=20.0, qty=1),
        # cart C: too stale (10 days ago) -> excluded
        _event("cart-C", "CUST3", "add_to_cart", 10 * 24 * 60, price=30.0, qty=1),
        # cart D: converted -> excluded despite recent add_to_cart
        _event("cart-D", "CUST4", "add_to_cart", 45, price=40.0, qty=1),
        _event("cart-D", "CUST4", "checkout_complete", 40),
    ]
    return spark.createDataFrame(rows)


def test_filter_abandoned_keeps_only_the_true_abandoned_cart(spark):
    events = _events_df(spark)
    cart_agg = aggregate_carts(events)
    abandoned = filter_abandoned(cart_agg)

    cart_ids = {row["cart_id"] for row in abandoned.collect()}
    assert cart_ids == {"cart-A"}


def test_recoverable_cart_value_nets_out_removals(spark):
    rows = [
        _event("cart-E", "CUST5", "add_to_cart", 45, price=100.0, qty=1),
        _event("cart-E", "CUST5", "remove_from_cart", 44, price=100.0, qty=1),
        _event("cart-E", "CUST5", "add_to_cart", 43, price=25.0, qty=2),
    ]
    events = spark.createDataFrame(rows)
    abandoned = filter_abandoned(aggregate_carts(events))
    row = abandoned.filter("cart_id = 'cart-E'").collect()[0]

    # added 100 + 50, removed 100 -> net 50
    assert row["recoverable_cart_value"] == 50.0


def test_score_carts_gives_higher_loyalty_score_to_returning_customers(spark):
    events = _events_df(spark)
    abandoned = filter_abandoned(aggregate_carts(events))

    orders = spark.createDataFrame([
        {"customer_id": "CUST1", "status": "delivered"},
    ], schema="customer_id string, status string")

    scored = score_carts(abandoned, orders)
    row = scored.filter("cart_id = 'cart-A'").collect()[0]

    assert row["loyalty_score"] == 100.0
    assert 0 <= row["priority_score"] <= 100
