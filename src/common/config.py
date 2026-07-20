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
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass

# Must match the bundle targets in orchestration/databricks/databricks.yml.
VALID_ENVS = ("dev", "staging")


@dataclass(frozen=True)
class PlatformConfig:
    env: str
    storage_suffix: str = ""
    project: str = "ecom"

    def __post_init__(self) -> None:
        if self.env not in VALID_ENVS:
            raise ValueError(
                f"unknown env {self.env!r}; expected one of {list(VALID_ENVS)}"
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

    def container_path(self, container: str) -> str:
        return f"abfss://{container}@{self.storage_account}.dfs.core.windows.net"

    def checkpoint_path(self, stream_name: str) -> str:
        return f"{self.container_path('checkpoints')}/{self.env}/{stream_name}"


def get_config(argv: Sequence[str] | None = None) -> PlatformConfig:
    """Resolve config from job parameters passed on the command line.

    `argv` defaults to sys.argv[1:]. Unknown arguments are ignored so that adding a job
    parameter consumed by one task doesn't break sibling tasks that don't read it.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--env", required=True, choices=VALID_ENVS)
    parser.add_argument("--storage_suffix", default="")
    args, _unknown = parser.parse_known_args(argv)
    return PlatformConfig(env=args.env, storage_suffix=args.storage_suffix)
