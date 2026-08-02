from datetime import datetime, timedelta, timezone

from dq_checks import apply_dq_rules
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from silver_clickstream import build_dq_rules, dedupe, standardize

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


def test_build_dq_rules_warns_on_unexpected_categorical_values(spark):
    schema = StructType(
        [
            StructField("session_id", StringType()),
            StructField("cart_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_timestamp", TimestampType()),
            StructField("price_at_event", StringType()),
            StructField("device_type", StringType()),
            StructField("page_type", StringType()),
        ]
    )
    rows = [
        {
            "session_id": "s1",
            "cart_id": None,
            "event_type": "page_view",
            "event_timestamp": NOW,
            "price_at_event": None,
            "device_type": "desktop",
            "page_type": "home",
        },
        {
            "session_id": "s2",
            "cart_id": None,
            "event_type": "page_scroll",  # unexpected event_type
            "event_timestamp": NOW,
            "price_at_event": None,
            "device_type": "desktop",
            "page_type": "home",
        },
        {
            "session_id": "s3",
            "cart_id": None,
            "event_type": "page_view",
            "event_timestamp": NOW,
            "price_at_event": None,
            "device_type": "smart_fridge",  # unexpected device_type
            "page_type": "home",
        },
        {
            "session_id": "s4",
            "cart_id": None,
            "event_type": "page_view",
            "event_timestamp": NOW,
            "price_at_event": None,
            "device_type": "desktop",
            "page_type": "wishlist",  # unexpected page_type
        },
    ]
    df = spark.createDataFrame(rows, schema=schema)
    clean_df, quarantine_df = apply_dq_rules(df, build_dq_rules())

    assert quarantine_df.count() == 0  # all new rules are warn severity
    warnings = {
        row["session_id"]: set(row["_dq_warnings"]) for row in clean_df.collect()
    }
    assert warnings["s1"] == set()
    assert warnings["s2"] == {"unexpected_event_type"}
    assert warnings["s3"] == {"unexpected_device_type"}
    assert warnings["s4"] == {"unexpected_page_type"}


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
