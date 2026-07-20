# Incident runbook

Scope: what to do when a pipeline alert fires, or when someone reports numbers that look wrong.
The ordering matters more than the commands — scope before fix, communicate before you have
answers.

## 0. Before anything: establish blast radius

Do not start fixing. Establish what was consuming the broken dataset and what state it is in,
because that decides everything else — including whether this is an engineering problem or a
business one.

```sql
-- What ran, what failed, and how far each run got
SELECT job_name, status, row_count, quarantined_count, started_at, finished_at, error_message
FROM ecom_<env>.gold.pipeline_audit_log
WHERE started_at > current_timestamp() - INTERVAL 3 DAYS
ORDER BY started_at DESC;
```

A row with `status = 'failed'` is a crash and carries its own `error_message`. **No row at all is
the more dangerous case**: nothing was alive to log anything, so the job never started — a paused
schedule, a deleted job, a cluster that could not launch.

Then check how stale each dataset actually is:

```sql
SELECT max(_bronze_ingested_at) FROM ecom_<env>.bronze.orders;
SELECT max(_silver_processed_at) FROM ecom_<env>.silver.orders;
SELECT max(_gold_computed_at)   FROM ecom_<env>.gold.cart_recovery_signal;
```

Downstream consumers are listed in [project-charter.md](project-charter.md) (persona table) and
[metric-glossary.md](metric-glossary.md). Anything reading `gold.*` is business-facing.

## 1. Contain before you reconstruct

Stop the bleeding first. Containment is cheap and reversible; reconstruction is neither.

- **Bad data actively being written** → pause the job's schedule in Workflows. Do not "fix and
  rerun" while the bad run is still writing.
- **Gold published from stale upstream** → the `assert_upstream_fresh` gate should have prevented
  this. If it published anyway, the gate's budget is wrong or the table is missing from
  `UPSTREAM_REQUIREMENTS` in [src/quality/freshness.py](../src/quality/freshness.py). Note it and
  fix the gate as part of the follow-up, not during the incident.
- **Consumers reading corrupt data** → tell the consumers before you repair anything (step 2).

Delta makes the actual repair recoverable, which is why containment beats speed:

```sql
DESCRIBE HISTORY ecom_<env>.silver.orders;                       -- find the last good version
RESTORE TABLE ecom_<env>.silver.orders TO VERSION AS OF <n>;     -- if a bad write must be undone
```

Check the retention window first — `maintenance_optimize` runs `VACUUM` nightly, so time travel
is not unlimited.

## 2. Communicate before you have the full picture

Send the first update once you know the blast radius, not once you know the fix. The cost of a
stakeholder discovering this through a broken report is higher than the cost of an early,
incomplete update from you.

State exactly four things:

1. What is affected (which datasets, which dashboards)
2. The time window of bad or missing data
3. What is known vs. still unknown
4. Next update time — and then actually send it

Say "we do not yet know whether the 3 days of order data are recoverable, next update at 14:00"
rather than waiting until you can say something complete.

## 3. Reconstruct

Only now. Replaying is not automatically safe:

- **Batch (order domain)** — bronze MERGEs are idempotent on the merge key and the source is
  deduped, so a rerun is safe *if the source CSVs are unchanged*. If the upstream export has been
  regenerated since the failure, you are replaying against different source data — that is a
  backfill, not a retry, and it needs the same blast-radius reasoning as the original incident.
- **Streaming** — the checkpoint means a restart resumes from the correct offset. If the stream
  died on a poison batch, the same batch replays; fix the cause or the batch will kill it again.
  Do not delete the checkpoint to "unstick" a stream — that silently reprocesses or skips data.
- **Gold** — full overwrite from silver, so rerunning is safe once silver is correct. Order:
  `gold_cart_recovery` / `gold_fulfillment_risk` before `gold_exec_summary_marts`, which reads
  both.

Run the manual DAG with `databricks bundle run -t <target> main_pipeline`.

## 4. Post-mortem: judge the monitoring, not just the bug

The question is not "what broke". It is **what does this failure say about our assumptions?** A
pipeline that fails silently was never production-grade, regardless of how good the fix is.

For each incident, answer explicitly:

- How long was it broken before anyone knew? If that number is hours or days, the detection gap
  is the finding — the bug is secondary.
- Which control *should* have caught it, and why didn't it? (`on_failure` notification, health
  rule, `freshness_check`, `assert_upstream_fresh`, quarantine rate guard.)
- Where else does this same gap exist? A missing freshness budget on one table usually means the
  whole class of tables is unmonitored.

Detection controls currently in place are listed in the SLA table in
[project-charter.md](project-charter.md).
