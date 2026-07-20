"""Environment/catalog resolution shared by every job in src/.

Every job reads its environment from the `env` job parameter, which Databricks passes to a
`spark_python_task` as a `--env=<value>` command-line argument (NOT as a notebook widget --
`dbutils.widgets` is a no-op under spark-submit and silently yields the declared default).
Every task in orchestration/databricks/resources/ is a spark_python_task, so argv is the only
correct source.

`env` is required and validated: there is deliberately no default. A missing or misspelled env
must crash the job, because the failure it otherwise causes is silent and cross-environment --
catalog is f"ecom_{env}", so a staging run that quietly fell back to "dev" would read and write
dev's tables while reporting success.

Table/path naming here must match infra/terraform/modules/unity-catalog (catalog = "ecom_{env}",
schemas bronze/silver/gold) and infra/terraform/modules/storage (container URLs).

Two storage backends. `adls` is the project's Azure default: external tables under
`abfss://…dfs.core.windows.net`, provisioned by Terraform. `uc_volume` is for a serverless /
Default-Storage workspace that has no ADLS account -- managed tables (unchanged: `cfg.table` is
already just `catalog.schema.name`) plus a Unity Catalog volume for the raw-file landing and
streaming checkpoints that would otherwise live in a storage container. The transform/quality
code is identical across both; only where bytes physically land differs.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass

# Must match the bundle targets in orchestration/databricks/databricks.yml.
VALID_ENVS = ("dev", "staging")
VALID_STORAGE_BACKENDS = ("adls", "uc_volume")
# continuous: the always-on micro-batch loop for production streaming. available_now: drain the
# current backlog once and stop -- how a streaming pipeline runs as a scheduled/on-demand batch
# (see docs/distributed-compute-notes.md), and what lets the serverless demo run terminate.
VALID_TRIGGER_MODES = ("continuous", "available_now")

# For uc_volume: a single managed volume holds everything that isn't a table. Path is
# /Volumes/{catalog}/{schema}/{volume}; created by scripts/bootstrap_uc.sh.
_VOLUME_SCHEMA = "bronze"
_VOLUME_NAME = "ops"


@dataclass(frozen=True)
class PlatformConfig:
    env: str
    storage_suffix: str = ""
    project: str = "ecom"
    storage_backend: str = "adls"
    trigger_mode: str = "continuous"

    def __post_init__(self) -> None:
        if self.env not in VALID_ENVS:
            raise ValueError(
                f"unknown env {self.env!r}; expected one of {list(VALID_ENVS)}"
            )
        if self.storage_backend not in VALID_STORAGE_BACKENDS:
            raise ValueError(
                f"unknown storage_backend {self.storage_backend!r}; expected one of "
                f"{list(VALID_STORAGE_BACKENDS)}"
            )
        if self.trigger_mode not in VALID_TRIGGER_MODES:
            raise ValueError(
                f"unknown trigger_mode {self.trigger_mode!r}; expected one of "
                f"{list(VALID_TRIGGER_MODES)}"
            )

    @property
    def catalog(self) -> str:
        return f"{self.project}_{self.env}"

    def schema(self, layer: str) -> str:
        assert layer in ("bronze", "silver", "gold"), f"unknown layer: {layer}"
        return f"{self.catalog}.{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    @property
    def storage_account(self) -> str:
        # An empty suffix resolves to a real-looking but wrong account name, so jobs that touch
        # storage fail here rather than at an opaque "path does not exist" much later.
        if not self.storage_suffix:
            raise ValueError(
                "storage_suffix is required to resolve a storage account; pass "
                "--storage_suffix (see the `storage_suffix` job parameter)"
            )
        return f"st{self.project}{self.env}{self.storage_suffix}"

    @property
    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/{_VOLUME_SCHEMA}/{_VOLUME_NAME}"

    def container_path(self, container: str) -> str:
        # In uc_volume mode there is no storage account; every non-table path is a subdirectory
        # of the one managed volume, so "container" becomes a top-level folder inside it.
        if self.storage_backend == "uc_volume":
            return f"{self.volume_root}/{container}"
        return f"abfss://{container}@{self.storage_account}.dfs.core.windows.net"

    def checkpoint_path(self, stream_name: str) -> str:
        return f"{self.container_path('checkpoints')}/{self.env}/{stream_name}"

    def stream_trigger(self, processing_time: str) -> dict:
        """Trigger kwargs to splat into DataStreamWriter.trigger().

        continuous uses the given micro-batch interval and runs forever; available_now processes
        whatever is currently in the source and then terminates, so the same streaming code runs
        as a bounded, re-runnable job (scheduled ingestion, or this repo's serverless demo).
        """
        if self.trigger_mode == "available_now":
            return {"availableNow": True}
        return {"processingTime": processing_time}


def get_config(argv: Sequence[str] | None = None) -> PlatformConfig:
    """Resolve config from job parameters passed on the command line.

    `argv` defaults to sys.argv[1:]. Unknown arguments are ignored so that adding a job
    parameter consumed by one task doesn't break sibling tasks that don't read it.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--env", required=True, choices=VALID_ENVS)
    parser.add_argument("--storage_suffix", default="")
    parser.add_argument(
        "--storage_backend", default="adls", choices=VALID_STORAGE_BACKENDS
    )
    parser.add_argument(
        "--trigger_mode", default="continuous", choices=VALID_TRIGGER_MODES
    )
    args, _unknown = parser.parse_known_args(argv)
    return PlatformConfig(
        env=args.env,
        storage_suffix=args.storage_suffix,
        storage_backend=args.storage_backend,
        trigger_mode=args.trigger_mode,
    )
