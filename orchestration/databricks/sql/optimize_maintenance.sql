-- Nightly compaction/clustering maintenance. Delta's autoOptimize.optimizeWrite/autoCompact
-- (set at table-creation time in the ingestion jobs) handle incremental compaction, but a
-- periodic explicit OPTIMIZE + ZORDER still pays off for the high-churn clickstream table -- see
-- docs/distributed-compute-notes.md section 5 for the benchmark justifying ZORDER BY (cart_id).
OPTIMIZE ${catalog}.silver.clickstream_events ZORDER BY (cart_id);
OPTIMIZE ${catalog}.silver.orders ZORDER BY (customer_id);

VACUUM ${catalog}.bronze.clickstream_events RETAIN 168 HOURS;  -- 7 days
VACUUM ${catalog}.silver.clickstream_events RETAIN 168 HOURS;
