from datetime import datetime, timedelta, timezone

from gold_exec_summary_marts import (
    delayed_order_revenue_risk,
    recoverable_revenue,
)

NOW = datetime.now(timezone.utc)


def test_recoverable_revenue_uses_only_the_latest_snapshot_run(spark):
    # Two runs of gold_cart_recovery_signal_history: an older run with a higher total that
    # should be ignored, and the latest run that should be summed.
    rows = [
        {
            "cart_id": "cart-old",
            "priority_score": 90.0,
            "recoverable_cart_value": 1000.0,
            "_snapshot_at": NOW - timedelta(hours=1),
        },
        {
            "cart_id": "cart-new-1",
            "priority_score": 75.0,
            "recoverable_cart_value": 100.0,
            "_snapshot_at": NOW,
        },
        {
            "cart_id": "cart-new-2",
            "priority_score": 40.0,  # below PRIORITY_ACTION_THRESHOLD -> excluded
            "recoverable_cart_value": 500.0,
            "_snapshot_at": NOW,
        },
    ]
    history = spark.createDataFrame(rows)

    assert recoverable_revenue(history) == 100.0


def test_delayed_order_revenue_risk_uses_only_the_latest_snapshot_run(spark):
    rows = [
        {
            "order_id": "order-old",
            "fulfillment_risk_score": 95.0,
            "order_total_usd": 2000.0,
            "_snapshot_at": NOW - timedelta(hours=4),
        },
        {
            "order_id": "order-new-1",
            "fulfillment_risk_score": 65.0,
            "order_total_usd": 300.0,
            "_snapshot_at": NOW,
        },
        {
            "order_id": "order-new-2",
            "fulfillment_risk_score": 10.0,  # below RISK_ACTION_THRESHOLD -> excluded
            "order_total_usd": 400.0,
            "_snapshot_at": NOW,
        },
    ]
    history = spark.createDataFrame(rows)

    assert delayed_order_revenue_risk(history) == 300.0
