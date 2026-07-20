"""Config resolution tests. No Spark/JVM needed -- these are pure argv parsing.

The behavior under test is the one that used to fail silently: `env` arriving from the command
line (how Databricks passes job parameters to a spark_python_task) rather than from
dbutils.widgets, and a missing/bad env crashing instead of defaulting to "dev".
"""

import pytest
from config import PlatformConfig, get_config


def test_env_comes_from_argv():
    cfg = get_config(["--env=staging", "--storage_suffix=sg01"])
    assert cfg.env == "staging"
    assert cfg.catalog == "ecom_staging"


def test_space_separated_form_also_parses():
    # Databricks emits --key=value, but a hand-run spark-submit may use --key value.
    assert get_config(["--env", "dev", "--storage_suffix", "dv01"]).env == "dev"


def test_missing_env_exits_rather_than_defaulting_to_dev():
    # The regression this guards: a staging job silently reading/writing ecom_dev.
    with pytest.raises(SystemExit):
        get_config([])


def test_unknown_env_exits():
    with pytest.raises(SystemExit):
        get_config(["--env=prod"])


def test_unknown_args_are_ignored():
    # A job parameter consumed by a sibling task must not crash this one.
    cfg = get_config(["--env=dev", "--storage_suffix=dv01", "--unrelated=x"])
    assert cfg.env == "dev"


def test_storage_paths_use_suffix():
    cfg = get_config(["--env=staging", "--storage_suffix=sg01"])
    assert cfg.storage_account == "stecomstagingsg01"
    assert (
        cfg.container_path("bronze")
        == "abfss://bronze@stecomstagingsg01.dfs.core.windows.net"
    )
    assert cfg.checkpoint_path("clickstream_bronze").endswith(
        "/staging/clickstream_bronze"
    )


def test_missing_storage_suffix_raises_only_when_storage_is_touched():
    cfg = get_config(["--env=dev"])
    assert cfg.table("bronze", "orders") == "ecom_dev.bronze.orders"  # fine without it
    with pytest.raises(ValueError, match="storage_suffix is required"):
        _ = cfg.storage_account


def test_direct_construction_validates_env():
    with pytest.raises(ValueError, match="unknown env"):
        PlatformConfig(env="produciton")


# --- uc_volume storage backend (serverless / Default-Storage workspaces) ---


def test_uc_volume_backend_uses_managed_paths_and_needs_no_suffix():
    cfg = get_config(["--env=dev", "--storage_backend=uc_volume"])
    assert cfg.storage_backend == "uc_volume"
    # Tables are unchanged (managed): catalog.schema.name.
    assert cfg.table("bronze", "orders") == "ecom_dev.bronze.orders"
    # Non-table paths point at the UC volume instead of abfss://, with no storage_suffix needed.
    assert cfg.container_path("bronze") == "/Volumes/ecom_dev/bronze/ops/bronze"
    assert (
        cfg.checkpoint_path("clickstream_bronze")
        == "/Volumes/ecom_dev/bronze/ops/checkpoints/dev/clickstream_bronze"
    )


def test_unknown_storage_backend_raises():
    with pytest.raises(ValueError, match="unknown storage_backend"):
        PlatformConfig(env="dev", storage_backend="gcs")


# --- trigger_mode (continuous vs available_now) ---


def test_trigger_mode_defaults_to_continuous():
    cfg = get_config(["--env=dev"])
    assert cfg.trigger_mode == "continuous"
    assert cfg.stream_trigger("1 minute") == {"processingTime": "1 minute"}


def test_available_now_trigger_drains_and_stops():
    cfg = get_config(["--env=dev", "--trigger_mode=available_now"])
    assert cfg.stream_trigger("2 minutes") == {"availableNow": True}


def test_unknown_trigger_mode_raises():
    with pytest.raises(ValueError, match="unknown trigger_mode"):
        PlatformConfig(env="dev", trigger_mode="microbatch")
