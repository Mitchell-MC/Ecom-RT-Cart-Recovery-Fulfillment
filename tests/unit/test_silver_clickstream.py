from datetime import datetime, timezone

from silver_clickstream import dedupe, standardize

NOW = datetime.now(timezone.utc)


def test_standardize_normalizes_case_and_derives_event_date(spark):
    rows = [{
        "event_timestamp": NOW,
        "customer_id": " cust001 ",
        "cart_id": " cart-1 ",
        "device_type": "  MOBILE ",
    }]
    df = spark.createDataFrame(rows)
    result = standardize(df).collect()[0]

    assert result["customer_id"] == "CUST001"
    assert result["cart_id"] == "cart-1"
    assert result["device_type"] == "mobile"
    assert result["event_date"] == NOW.date()


def test_dedupe_cart_events_on_business_key_last_write_wins(spark):
    rows = [
        {"event_id": "e1", "cart_id": "cart-1", "event_type": "add_to_cart",
         "event_timestamp": NOW, "producer_ingested_at": NOW, "attempt": "first"},
        {"event_id": "e2", "cart_id": "cart-1", "event_type": "add_to_cart",
         "event_timestamp": NOW, "producer_ingested_at": NOW, "attempt": "retry"},
    ]
    df = spark.createDataFrame(rows)
    result = dedupe(df).collect()

    assert len(result) == 1
    assert result[0]["attempt"] == "retry"


def test_dedupe_non_cart_events_on_event_id(spark):
    rows = [
        {"event_id": "e1", "cart_id": None, "event_type": "page_view",
         "event_timestamp": NOW, "producer_ingested_at": NOW},
        {"event_id": "e1", "cart_id": None, "event_type": "page_view",
         "event_timestamp": NOW, "producer_ingested_at": NOW},
        {"event_id": "e2", "cart_id": None, "event_type": "page_view",
         "event_timestamp": NOW, "producer_ingested_at": NOW},
    ]
    df = spark.createDataFrame(rows)
    result = dedupe(df).collect()

    assert {row["event_id"] for row in result} == {"e1", "e2"}
