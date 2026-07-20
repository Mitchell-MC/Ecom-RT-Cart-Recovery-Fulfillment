from datetime import datetime, timedelta, timezone

from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from silver_clickstream import dedupe, standardize

# cart_id is null in every row of the non-cart-event tests, so Spark has no value to infer a
# type from and createDataFrame raises CANNOT_DETERMINE_TYPE. State the schema explicitly.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("cart_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("producer_ingested_at", TimestampType()),
    ]
)

NOW = datetime.now(timezone.utc)
# dedupe() breaks ties on producer_ingested_at; a retry is by definition ingested after the
# first attempt, so give it a strictly later value. Equal timestamps make row_number()
# pick arbitrarily and the assertion below only passes by luck.
LATER = NOW + timedelta(seconds=30)


def test_standardize_normalizes_case_and_derives_event_date(spark):
    rows = [
        {
            "event_timestamp": NOW,
            "customer_id": " cust001 ",
            "cart_id": " cart-1 ",
            "device_type": "  MOBILE ",
        }
    ]
    df = spark.createDataFrame(rows)
    result = standardize(df).collect()[0]

    assert result["customer_id"] == "CUST001"
    assert result["cart_id"] == "cart-1"
    assert result["device_type"] == "mobile"
    assert result["event_date"] == NOW.date()


def test_dedupe_cart_events_on_business_key_last_write_wins(spark):
    rows = [
        {
            "event_id": "e1",
            "cart_id": "cart-1",
            "event_type": "add_to_cart",
            "event_timestamp": NOW,
            "producer_ingested_at": NOW,
            "attempt": "first",
        },
        {
            "event_id": "e2",
            "cart_id": "cart-1",
            "event_type": "add_to_cart",
            "event_timestamp": NOW,
            "producer_ingested_at": LATER,
            "attempt": "retry",
        },
    ]
    df = spark.createDataFrame(rows)
    result = dedupe(df).collect()

    assert len(result) == 1
    assert result[0]["attempt"] == "retry"


def test_dedupe_non_cart_events_on_event_id(spark):
    rows = [
        {
            "event_id": "e1",
            "cart_id": None,
            "event_type": "page_view",
            "event_timestamp": NOW,
            "producer_ingested_at": NOW,
        },
        {
            "event_id": "e1",
            "cart_id": None,
            "event_type": "page_view",
            "event_timestamp": NOW,
            "producer_ingested_at": NOW,
        },
        {
            "event_id": "e2",
            "cart_id": None,
            "event_type": "page_view",
            "event_timestamp": NOW,
            "producer_ingested_at": NOW,
        },
    ]
    df = spark.createDataFrame(rows, schema=EVENT_SCHEMA)
    result = dedupe(df).collect()

    assert {row["event_id"] for row in result} == {"e1", "e2"}
