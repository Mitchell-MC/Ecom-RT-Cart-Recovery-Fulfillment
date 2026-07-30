# Distributed Compute Notes

Design decisions for how data is physically laid out and processed, and the tradeoffs behind
each one. This is the doc to point to in an interview when asked "walk me through a distributed
systems decision you made and why."

## 1. Partitioning strategy by layer

| Layer | Partitioning | Why |
|---|---|---|
| `bronze.clickstream_events` | none (Autoloader-managed file discovery) | Bronze is a landing zone written by a continuous stream in small, frequent micro-batches. Partitioning bronze by a business column would multiply the small-files problem (every micro-batch would fan out across every partition value); Delta's file-listing + checkpoint log already makes unpartitioned bronze scans cheap enough at this stage. |
| `silver.clickstream_events` | `event_date` (derived, `DATE`) | Every downstream gold query filters by a recent date range ("carts active in the last 7 days", "orders from the last 90 days"). Date is low-cardinality (one partition/day) and matches the query predicate almost every consumer uses — the textbook case for partition pruning. |
| `silver.orders` | `order_date` (derived) | Same reasoning; `gold.exec_summary_*` and `gold.fulfillment_risk_signal` both filter on a trailing date window. |
| `gold.*` | none | Gold tables are small, pre-aggregated signal/mart outputs (thousands–low millions of rows, not billions). Partitioning here would cost more in file-count overhead than it saves in pruning. |

**Explicitly rejected: partitioning by `cart_id` or `customer_id`.** Both are high-cardinality
(hundreds of thousands to millions of distinct values at realistic volume). Directory-per-value
partitioning at that cardinality produces enormous numbers of tiny files, which kills small-file
read performance and blows up the Delta transaction log. The right tool for "make point lookups
on a high-cardinality column fast" is data skipping via **Z-ORDER**, not partitioning — see the
benchmark below.

## 2. Streaming trigger interval

`bronze_clickstream_stream.py` and `silver_clickstream.py` both run on a fixed
`trigger(processingTime=...)` (1 minute at bronze, 2 minutes at silver) as a **continuous**
Databricks Workflow job, rather than `trigger(availableNow=True)` on a schedule.

| Option | Latency | Cost | Chosen? |
|---|---|---|---|
| Continuous job, `processingTime` trigger | Consistently low (~1-3 min end-to-end) | Cluster runs 24/7 (streaming cluster policy has `autotermination_minutes=0`) | **Yes** — required to hit the <5min bronze freshness SLA in `docs/project-charter.md` |
| Scheduled job, `trigger(availableNow=True)`, e.g. every 15 min | Bursty: fresh right after each run, stale right before the next | Cheaper — cluster only runs while draining the backlog, then terminates | No — 15 min worst-case latency violates the SLA; would revisit for a lower-priority stream |

At production scale, the honest follow-up here is **cost**, not correctness: a 24/7 job cluster
for a stream this size is over-provisioned. The natural next step is Databricks' serverless
Structured Streaming compute (autoscales to near-zero between events) — noted as a v2 item
rather than implemented, since it changes the Terraform compute module meaningfully and wasn't
worth the scope increase for a portfolio-scale event volume.

## 3. Join strategy

- `products` (hundreds of rows) and `customers` (thousands–low tens of thousands at this scale)
  are small enough that Spark's adaptive query execution (AQE) broadcasts them automatically
  against the `orders`/`clickstream_events` fact tables — confirmed via `EXPLAIN FORMATTED`
  showing `BroadcastHashJoin` rather than `SortMergeJoin` in the gold notebooks.
- `orders` ⋈ `order_items` ⋈ `shipments` (all fact-sized) uses a sort-merge join. All three share
  `order_id` as the join key and none is small enough to broadcast safely at production scale
  (broadcasting is capped via `spark.sql.autoBroadcastJoinThreshold`, left at Databricks'
  default rather than force-broadcast, to avoid driver OOM if a table grows past what the
  synthetic-data scale suggests).

## 4. Small-file management

Streaming micro-batches (bronze: every 1 min) naturally produce many small files. Delta's
`delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact` table properties are
enabled on every bronze/silver table (set at table-creation time in the ingestion jobs) so
compaction happens incrementally rather than requiring a separate maintenance job — a standalone
`maintenance_optimize` job additionally runs a nightly `OPTIMIZE ... ZORDER BY (...)` task (see
`orchestration/databricks/resources/maintenance_job.yml` and
`orchestration/databricks/sql/optimize_maintenance.sql`) for the columns the benchmark below
justifies.

## 5. Benchmark: partition-only vs. partition + Z-ORDER

**Question:** for the query pattern `gold.cart_recovery_signal` actually runs — "give me all
`clickstream_events` for `cart_id = X` in the last 7 days" — does adding `ZORDER BY (cart_id)` on
top of `event_date` partitioning meaningfully reduce bytes scanned, on top of the partition
pruning we already get from the date filter?

**Method** (`src/transform/silver/benchmark_layout.py`, run on a Databricks cluster against a
populated `silver.clickstream_events`):

1. Layout A: `silver.clickstream_events` as built (partitioned by `event_date` only).
2. Layout B: `CREATE TABLE ... AS SELECT * FROM silver.clickstream_events`, partitioned by
   `event_date`, then `OPTIMIZE ... ZORDER BY (cart_id)`.
3. Run the same representative point-lookup query against both, 5 times each after a cold cache
   (`CLEAR CACHE`), and record: wall-clock duration, `DESCRIBE DETAIL` file count, and bytes read
   from the Spark UI SQL tab / `EXPLAIN FORMATTED` file-pruning stats.

**Hypothesis:** partitioning by `event_date` already prunes to ~1 day of files; within that day,
`cart_id` is effectively random with respect to file layout, so a lookup for one cart still has
to open every file in that day's partition. Z-ORDER clusters rows with similar `cart_id` values
into the same files, so Delta's min/max column statistics can skip most files even within the
matching partition. Expect Layout B to read a small fraction of the bytes/files of Layout A for
this specific query shape, at the one-time cost of the `OPTIMIZE` job.

**Result:** *pending a run against a provisioned workspace — this repo doesn't have one attached.
Run `benchmark_layout.py` via `databricks bundle run -t dev benchmark_layout` after
`terraform apply` + a few days of generated data, then replace this paragraph with the actual
files-scanned / duration numbers.* The methodology above is what to reproduce; don't take the
hypothesis as a substitute for the measurement.

## 6. What we deliberately did not benchmark

- **Photon vs. non-Photon**: expected to help the gold-layer aggregation queries (heavy on
  `GROUP BY`/window functions) more than the streaming ingestion jobs; not benchmarked because
  it's a cluster-config toggle, not an architecture decision, and doesn't change any code here.
- **Liquid Clustering** (Databricks' newer alternative to manual `ZORDER`): a reasonable
  candidate to replace the manual `OPTIMIZE ZORDER` step once available on the target Databricks
  Runtime; not adopted in v1 to keep the benchmark comparable to the well-documented classic
  Z-ORDER behavior above.
