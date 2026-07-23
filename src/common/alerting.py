"""Slack alerts carrying enough context to act on.

Databricks' built-in `on_failure` email says "Job X failed." That is a notification, not an
alert: it names a task, not an impact, and it arrives with no indication of whether this blocks
the month-end close or a nightly VACUUM nobody would miss. The recipient's first three questions
-- what does this break, when did it last work, who owns the thing that broke it -- all require
leaving the email and going digging, at whatever hour it fired.

So the built-in notifications stay (they are the safety net for the case where the driver dies
before it can post anything) and jobs additionally post a message here that answers those three
questions up front.

The webhook URL is read from the SLACK_WEBHOOK_URL environment variable, injected from a
Databricks secret scope by the job cluster's spark_env_vars -- never checked in, and never
passed as a job parameter where it would be visible in the run UI and the Jobs API.

Every failure here is swallowed. An alerting path that can fail the job it is reporting on turns
one incident into two, and a job that dies while trying to say it died is strictly worse than
the built-in email it was meant to improve on.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

WEBHOOK_ENV_VAR = "SLACK_WEBHOOK_URL"
_TIMEOUT_SECONDS = 10

# Who to wake, and who owns the upstream that most often breaks each job. "job failed" plus a
# name is a page someone can act on; without the name it is a page that gets forwarded twice
# before it reaches anyone who can do something.
JOB_OWNERS: dict[str, dict[str, str]] = {
    "bronze_orders_domain": {
        "owner": "@data-platform",
        "upstream": "OMS nightly export (Commerce Ops)",
        "impact": "Order data stops refreshing; every downstream signal goes stale.",
    },
    "silver_orders_domain": {
        "owner": "@data-platform",
        "upstream": "bronze_orders_domain",
        "impact": "Order/shipment standardization stalls; gold signals go stale.",
    },
    "bronze_clickstream_stream": {
        "owner": "@data-platform",
        "upstream": "Clickstream producer (Web Platform)",
        "impact": "Cart events stop landing; cart recovery goes blind within the hour.",
    },
    "silver_clickstream": {
        "owner": "@data-platform",
        "upstream": "bronze_clickstream_stream",
        "impact": "Cart events stop reaching silver; cart recovery goes blind.",
    },
    "gold_cart_recovery": {
        "owner": "@data-platform",
        "upstream": "silver.clickstream_events, silver.orders",
        "impact": "Cart recovery worklist stops refreshing for Growth/Lifecycle.",
    },
    "gold_fulfillment_risk": {
        "owner": "@data-platform",
        "upstream": "silver.orders, silver.shipments, silver.inventory",
        "impact": "At-risk order list stops refreshing for Fulfillment Ops.",
    },
    "gold_exec_summary_marts": {
        "owner": "@data-platform",
        "upstream": "gold.cart_recovery_signal, gold.fulfillment_risk_signal",
        "impact": "Executive daily KPI summary is stale or missing.",
    },
    "freshness_check": {
        "owner": "@data-platform",
        "upstream": "all pipelines",
        "impact": "One or more datasets are outside their freshness SLA.",
    },
}


def job_context(job_name: str) -> dict[str, str]:
    return JOB_OWNERS.get(
        job_name,
        {
            "owner": "@data-platform",
            "upstream": "unknown",
            "impact": f"Unknown -- {job_name} is not in alerting.JOB_OWNERS.",
        },
    )


def post_alert(
    *,
    title: str,
    job_name: str,
    env: str,
    details: list[tuple[str, str]],
    run_url: str | None = None,
) -> bool:
    """Post one alert to Slack. Returns True if it was delivered.

    Never raises. A False return means the alert did not land -- the caller is expected to have
    already printed the same information to the driver log, which is the fallback.
    """
    webhook_url = os.environ.get(WEBHOOK_ENV_VAR)
    if not webhook_url:
        # Not an error: dev workspaces and local runs legitimately have no webhook configured.
        print(f"[alerting] {WEBHOOK_ENV_VAR} not set; skipping Slack alert: {title}")
        return False

    context = job_context(job_name)
    lines = [
        f"*{title}*",
        f"*Impact:* {context['impact']}",
        f"*Environment:* `{env}`   *Job:* `{job_name}`",
        f"*Owner:* {context['owner']}   *Upstream:* {context['upstream']}",
    ]
    lines += [f"*{label}:* {value}" for label, value in details]
    if run_url:
        lines.append(f"<{run_url}|Open the failed run>")

    payload = {"text": "\n".join(lines)}

    try:
        request = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, OSError, ValueError) as exc:
        # Swallowed on purpose -- see module docstring. Printed so the driver log shows the
        # alert was attempted and why it did not land.
        print(f"[alerting] failed to post Slack alert ({type(exc).__name__}: {exc})")
        return False


def run_url(spark) -> str | None:
    """Best-effort deep link to the current Databricks run."""
    try:
        host = spark.conf.get("spark.databricks.workspaceUrl", None)
        run_id = spark.conf.get("spark.databricks.job.runId", None)
    except Exception:  # pragma: no cover -- conf access varies by runtime
        return None
    if not host or not run_id:
        return None
    return f"https://{host}/#job/run/{run_id}"
