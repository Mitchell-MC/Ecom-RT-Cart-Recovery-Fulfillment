"""Tests for the job_run audit context manager.

These patch log_run rather than writing Delta, because the behaviour worth pinning down is
control flow -- which status gets recorded on which path, and whether the original exception
still escapes -- not the write itself. The old module could only ever record success, so the
failure path is the one that needs a regression test.
"""

import pytest

import audit
from audit import job_run


class _Cfg:
    def table(self, layer, name):
        return f"ecom_dev.{layer}.{name}"


@pytest.fixture
def logged(monkeypatch):
    """Capture log_run kwargs instead of writing to Delta."""
    calls = []
    monkeypatch.setattr(audit, "log_run", lambda *a, **kw: calls.append(kw))
    return calls


def _job_run(**overrides):
    kwargs = {
        "job_name": "gold_cart_recovery",
        "layer": "gold",
        "target_table": "ecom_dev.gold.cart_recovery_signal",
    }
    kwargs.update(overrides)
    return job_run(None, _Cfg(), **kwargs)


def test_success_logs_one_row_with_body_row_count(logged):
    with _job_run() as run:
        run.row_count = 42

    assert len(logged) == 1
    assert logged[0]["status"] == "success"
    assert logged[0]["row_count"] == 42
    assert logged[0]["error_message"] is None


def test_failure_logs_failed_status_and_reraises(logged):
    with pytest.raises(ValueError, match="upstream is stale"):
        with _job_run():
            raise ValueError("upstream is stale")

    assert len(logged) == 1
    assert logged[0]["status"] == "failed"
    # The exception must still escape: the task has to go red so on_failure notifies someone.
    assert "ValueError: upstream is stale" in logged[0]["error_message"]


def test_failure_before_any_rows_records_null_not_zero(logged):
    # A job that died before counting anything did not process zero rows -- it doesn't know.
    with pytest.raises(RuntimeError):
        with _job_run():
            raise RuntimeError("boom")

    assert logged[0]["row_count"] is None


def test_partial_progress_is_preserved_on_failure(logged):
    # Mirrors the bronze/silver loops, which update run.row_count per table.
    with pytest.raises(RuntimeError):
        with _job_run() as run:
            run.row_count = 300
            raise RuntimeError("failed on table 4 of 6")

    assert logged[0]["status"] == "failed"
    assert logged[0]["row_count"] == 300


def test_quarantined_count_is_recorded(logged):
    with _job_run() as run:
        run.row_count = 100
        run.quarantined_count = 7

    assert logged[0]["quarantined_count"] == 7


def test_audit_write_failure_does_not_mask_the_real_error(monkeypatch):
    def exploding_log_run(*a, **kw):
        raise ConnectionError("metastore unreachable")

    monkeypatch.setattr(audit, "log_run", exploding_log_run)

    # The outage that killed the job usually kills the audit write too; the job's own
    # exception is the one an on-call engineer needs to see.
    with pytest.raises(ValueError, match="the real problem"):
        with _job_run():
            raise ValueError("the real problem")


def test_keyboard_interrupt_is_recorded_and_propagates(logged):
    # BaseException, not Exception: a cancelled run should still leave a trace.
    with pytest.raises(KeyboardInterrupt):
        with _job_run():
            raise KeyboardInterrupt()

    assert logged[0]["status"] == "failed"
