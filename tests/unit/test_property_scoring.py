"""Property-based tests (hypothesis) for the gold scoring functions.

These assert invariants from docs/metric-glossary.md that must hold for *any* input, not
just the hand-picked fixtures in the example-based tests:

* every sub-score and the composite score stay within [0, 100]; and
* the two scores the glossary describes as monotonic actually are -- fulfillment
  time-pressure falls as the promise date moves out, and cart priority rises with
  recoverable value when recency and loyalty are held fixed.

Each hypothesis example builds a small Spark DataFrame and runs the real transform;
runtime is dominated by Spark job startup, so max_examples is kept low and the
per-example deadline is disabled (Spark latency is too variable for the default).
"""

from datetime import date, datetime, timedelta, timezone

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from gold_cart_recovery import score_carts
from gold_fulfillment_risk import score

EPS = 1e-6
NOW = datetime.now(timezone.utc)

_CART_SCHEMA = (
    "cart_id string, customer_id string, last_activity_at timestamp, "
    "minutes_since_last_activity double, recoverable_cart_value double, item_count int"
)
_FULFILLMENT_SCHEMA = (
    "order_id string, customer_id string, order_total_usd double, carrier string, "
    "promised_delivery_date date, carrier_avg_transit_days int, "
    "has_backordered_item boolean, carrier_historical_late_rate_pct double"
)

_CART_ROWS = st.lists(
    st.fixed_dictionaries(
        {
            "recoverable_cart_value": st.floats(
                min_value=0, max_value=1e6, allow_nan=False, allow_infinity=False
            ),
            "minutes_since_last_activity": st.floats(
                min_value=0, max_value=1e5, allow_nan=False, allow_infinity=False
            ),
            "has_prior": st.booleans(),
        }
    ),
    min_size=1,
    max_size=8,
)

_FULFILLMENT_ROWS = st.lists(
    st.fixed_dictionaries(
        {
            "offset": st.integers(min_value=-30, max_value=60),
            "avg_transit": st.integers(min_value=1, max_value=14),
            "backorder": st.booleans(),
            "late_rate": st.floats(
                min_value=0, max_value=100, allow_nan=False, allow_infinity=False
            ),
            "total": st.floats(
                min_value=0, max_value=1e7, allow_nan=False, allow_infinity=False
            ),
        }
    ),
    min_size=1,
    max_size=8,
)

_DISTINCT_VALUES = st.lists(
    st.floats(min_value=1, max_value=1e6, allow_nan=False, allow_infinity=False),
    min_size=2,
    max_size=8,
    unique=True,
)

_DISTINCT_OFFSETS = st.lists(
    st.integers(min_value=-20, max_value=40),
    min_size=2,
    max_size=8,
    unique=True,
)

_SUPPRESS = [HealthCheck.too_slow, HealthCheck.function_scoped_fixture]


def _cart_df(spark, rows):
    data = []
    for i, r in enumerate(rows):
        minutes = float(r["minutes_since_last_activity"])
        data.append(
            (
                f"cart-{i}",
                f"CUST{i}",
                NOW - timedelta(minutes=minutes),
                minutes,
                float(r["recoverable_cart_value"]),
                1,
            )
        )
    return spark.createDataFrame(data, schema=_CART_SCHEMA)


def _orders_df(spark, customer_ids):
    data = [(cid, "delivered") for cid in customer_ids]
    return spark.createDataFrame(data, schema="customer_id string, status string")


def _fulfillment_df(spark, rows):
    today = date.today()
    data = []
    for i, r in enumerate(rows):
        data.append(
            (
                f"ORD-{i}",
                f"CUST{i}",
                float(r["total"]),
                "ParcelSwift",
                today + timedelta(days=int(r["offset"])),
                int(r["avg_transit"]),
                bool(r["backorder"]),
                float(r["late_rate"]),
            )
        )
    return spark.createDataFrame(data, schema=_FULFILLMENT_SCHEMA)


@given(rows=_CART_ROWS)
@settings(max_examples=20, deadline=None, suppress_health_check=_SUPPRESS)
def test_cart_recovery_scores_stay_in_0_100(spark, rows):
    abandoned = _cart_df(spark, rows)
    prior = [f"CUST{i}" for i, r in enumerate(rows) if r["has_prior"]]
    orders = _orders_df(spark, prior)

    for row in score_carts(abandoned, orders).collect():
        for col in ("recency_score", "value_score", "loyalty_score", "priority_score"):
            assert -EPS <= row[col] <= 100 + EPS, (col, row[col])


@given(values=_DISTINCT_VALUES)
@settings(max_examples=15, deadline=None, suppress_health_check=_SUPPRESS)
def test_cart_priority_nondecreasing_in_value(spark, values):
    # Recency (fixed 60 min) and loyalty (every customer has a prior order) are held
    # constant, so priority_score must move only with recoverable value -- and upward.
    values = sorted(values)
    rows = [
        {
            "recoverable_cart_value": v,
            "minutes_since_last_activity": 60.0,
            "has_prior": True,
        }
        for v in values
    ]
    abandoned = _cart_df(spark, rows)
    orders = _orders_df(spark, [f"CUST{i}" for i in range(len(values))])

    scored_rows = score_carts(abandoned, orders).collect()
    scored = {r["recoverable_cart_value"]: r["priority_score"] for r in scored_rows}
    ordered = [scored[v] for v in values]
    for lower, higher in zip(ordered, ordered[1:]):
        assert lower <= higher + EPS, (lower, higher)


@given(rows=_FULFILLMENT_ROWS)
@settings(max_examples=20, deadline=None, suppress_health_check=_SUPPRESS)
def test_fulfillment_scores_stay_in_0_100(spark, rows):
    for row in score(_fulfillment_df(spark, rows)).collect():
        for col in (
            "time_pressure_score",
            "inventory_risk_score",
            "carrier_risk_score",
            "fulfillment_risk_score",
        ):
            assert -EPS <= row[col] <= 100 + EPS, (col, row[col])


@given(offsets=_DISTINCT_OFFSETS)
@settings(max_examples=15, deadline=None, suppress_health_check=_SUPPRESS)
def test_time_pressure_nonincreasing_as_promise_moves_out(spark, offsets):
    # Carrier transit is fixed, so time_pressure_score depends only on days_to_promise;
    # a later promise date means fewer days of pressure, never more.
    rows = [
        {"offset": o, "avg_transit": 5, "backorder": False, "late_rate": 0.0}
        for o in offsets
    ]
    scored = score(_fulfillment_df(spark, rows)).collect()

    by_days = sorted((r["days_to_promise"], r["time_pressure_score"]) for r in scored)
    pressures = [tp for _, tp in by_days]
    for earlier, later in zip(pressures, pressures[1:]):
        assert earlier >= later - EPS, (earlier, later)
