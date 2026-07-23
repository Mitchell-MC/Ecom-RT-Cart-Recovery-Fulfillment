"""Tests for Slack alerting.

Two things matter here. First, that the message carries what an on-call reader needs before they
can act -- impact, environment, owner, upstream -- because "Job X failed" is the notification
this replaces. Second, and more important, that nothing in this path can ever fail the job it is
reporting on: a job that dies while trying to say it died is worse than no alert at all.
"""

import json

import pytest

import alerting
from alerting import JOB_OWNERS, job_context, post_alert


@pytest.fixture
def posted(monkeypatch):
    """Capture the Slack payload instead of making a network call."""
    sent = []

    class _Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake_urlopen(request, timeout=None):
        sent.append(
            {
                "url": request.full_url,
                "payload": json.loads(request.data.decode("utf-8")),
            }
        )
        return _Response()

    monkeypatch.setenv(alerting.WEBHOOK_ENV_VAR, "https://hooks.slack.test/abc")
    monkeypatch.setattr(alerting.urllib.request, "urlopen", fake_urlopen)
    return sent


def _post(**overrides):
    kwargs = {
        "title": "Pipeline failure: bronze_orders_domain",
        "job_name": "bronze_orders_domain",
        "env": "staging",
        "details": [("Error", "`ValueError: boom`")],
    }
    kwargs.update(overrides)
    return post_alert(**kwargs)


def test_alert_is_delivered(posted):
    assert _post() is True
    assert len(posted) == 1
    assert posted[0]["url"] == "https://hooks.slack.test/abc"


def test_message_leads_with_business_impact(posted):
    _post()
    text = posted[0]["payload"]["text"]

    # The whole point: a reader learns what this breaks without opening anything.
    assert "Order data stops refreshing" in text
    assert "staging" in text
    assert "@data-platform" in text
    assert "Commerce Ops" in text  # who owns the upstream that most often breaks it


def test_details_and_run_link_are_included(posted):
    _post(
        details=[("Last good run", "2026-07-17 06:00 UTC")],
        run_url="https://adb-1.azuredatabricks.net/#job/run/42",
    )
    text = posted[0]["payload"]["text"]

    assert "Last good run" in text
    assert "2026-07-17 06:00 UTC" in text
    assert "#job/run/42" in text


def test_missing_webhook_is_not_an_error(monkeypatch, capsys):
    # Dev workspaces and local runs legitimately have no webhook configured.
    monkeypatch.delenv(alerting.WEBHOOK_ENV_VAR, raising=False)

    assert _post() is False
    assert "skipping Slack alert" in capsys.readouterr().out


def test_network_failure_is_swallowed(monkeypatch, capsys):
    monkeypatch.setenv(alerting.WEBHOOK_ENV_VAR, "https://hooks.slack.test/abc")

    def exploding_urlopen(request, timeout=None):
        raise OSError("connection refused")

    monkeypatch.setattr(alerting.urllib.request, "urlopen", exploding_urlopen)

    # Must not raise: alerting can never be the thing that fails a run.
    assert _post() is False
    assert "failed to post Slack alert" in capsys.readouterr().out


def test_non_2xx_response_reports_failure(monkeypatch):
    monkeypatch.setenv(alerting.WEBHOOK_ENV_VAR, "https://hooks.slack.test/abc")

    class _Response:
        status = 500

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(
        alerting.urllib.request, "urlopen", lambda request, timeout=None: _Response()
    )
    assert _post() is False


def test_unknown_job_still_produces_a_usable_alert(posted):
    _post(job_name="some_new_job")
    text = posted[0]["payload"]["text"]

    assert "some_new_job" in text
    assert "JOB_OWNERS" in text  # tells the reader how to fix the missing context


def test_every_job_in_the_bundle_has_an_owner():
    # An alert without an owner gets forwarded twice before reaching anyone who can act.
    assert set(JOB_OWNERS) == {
        "bronze_orders_domain",
        "silver_orders_domain",
        "bronze_clickstream_stream",
        "silver_clickstream",
        "gold_cart_recovery",
        "gold_fulfillment_risk",
        "gold_exec_summary_marts",
        "freshness_check",
    }


def test_every_owner_entry_is_complete():
    for job_name in JOB_OWNERS:
        context = job_context(job_name)
        assert context["owner"] and context["upstream"] and context["impact"]
