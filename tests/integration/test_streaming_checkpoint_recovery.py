"""Integration test: streaming checkpoint recovery for the silver clickstream job.

Exercises the real ``process_batch`` from silver_clickstream against a Structured
Streaming query that reads a Delta source (the production topology: silver streams from
the bronze Delta table) with a persisted checkpoint. The pipeline claims exactly-once
behaviour across restarts -- a success criterion in docs/project-charter.md. This test
proves it: after restarting against the same checkpoint, already-processed events are
neither reprocessed (no duplicates) nor lost, and new events land exactly once.

Marked ``integration`` so it is deselected by default (see pyproject.toml). It needs a
real local Spark + Delta session and is slower than the unit tests, so CI runs it in its
own job. The real bronze job's Autoloader (``cloudFiles``) source can't run locally, so
the source here is a plain Delta table the test appends to between rounds -- the
checkpoint and exactly-once behaviour under test are identical.
"""

import uuid
from datetime import datetime, timezone

import pytest

from silver_clickstream import process_batch

pytestmark = pytest.mark.integration

_SCHEMA = (
    "event_id string, event_type string, event_timestamp timestamp, "
    "session_id string, customer_id string, cart_id string, device_type string, "
    "producer_ingested_at timestamp, page_type string, sku string, quantity int, "
    "price_at_event double"
)


def _event(
    event_id,
    event_type,
    session_id,
    customer_id,
    cart_id,
    price=None,
    qty=None,
    page_type=None,
):
    now = datetime.now(timezone.utc)
    return (
        event_id,
        event_type,
        now,
        session_id,
        customer_id,
        cart_id,
        "mobile",
        now,
        page_type,
        None,
        qty,
        price,
    )


def _run_available_now(spark, bronze_table, silver_table, quarantine_table, checkpoint):
    def _batch(df, batch_id):
        process_batch(df, batch_id, spark, silver_table, quarantine_table)

    reader = spark.readStream.format("delta").table(bronze_table)
    reader = reader.withWatermark("event_timestamp", "2 hours")
    writer = reader.writeStream.foreachBatch(_batch)
    writer = writer.option("checkpointLocation", checkpoint)
    writer = writer.trigger(availableNow=True)
    query = writer.start()
    query.awaitTermination()


def _silver_ids(spark, silver_table):
    return {r["event_id"] for r in spark.table(silver_table).collect()}


def test_checkpoint_recovery_is_exactly_once(spark, tmp_path):
    uid = uuid.uuid4().hex[:8]
    bronze_table = f"bronze_ckpt_{uid}"
    silver_table = f"silver_ckpt_{uid}"
    quarantine_table = f"silver_ckpt_quarantine_{uid}"
    checkpoint = str(tmp_path / "ckpt")

    def run():
        _run_available_now(
            spark, bronze_table, silver_table, quarantine_table, checkpoint
        )

    round1 = [
        _event("e1", "page_view", "S1", "c1", None),
        _event("e2", "add_to_cart", "S1", "c1", "CART1", price=10.0, qty=1),
        _event("e3", "add_to_cart", "S2", "c2", "CART2", price=20.0, qty=2),
        # violates missing_session_id -> quarantined, must never reach silver
        _event("bad1", "add_to_cart", None, "c9", "CART9", price=5.0, qty=1),
    ]
    round2 = [
        _event("e4", "checkout_start", "S1", "c1", "CART1"),
        _event("e5", "add_to_cart", "S3", "c3", "CART3", price=5.0, qty=3),
    ]

    try:
        df1 = spark.createDataFrame(round1, schema=_SCHEMA)
        df1.write.format("delta").saveAsTable(bronze_table)
        run()

        assert _silver_ids(spark, silver_table) == {"e1", "e2", "e3"}
        assert spark.table(quarantine_table).count() == 1

        # new data arrives; the stream restarts from the same checkpoint
        df2 = spark.createDataFrame(round2, schema=_SCHEMA)
        df2.write.format("delta").mode("append").saveAsTable(bronze_table)
        run()

        assert _silver_ids(spark, silver_table) == {"e1", "e2", "e3", "e4", "e5"}

        # restart with no new source data must be a no-op: no reprocessing, no dupes
        run()
        assert spark.table(silver_table).count() == 5
    finally:
        for table in (silver_table, quarantine_table, bronze_table):
            spark.sql(f"DROP TABLE IF EXISTS {table}")
