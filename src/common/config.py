"""Environment/catalog resolution shared by every job in src/.

Every job reads its environment from a Databricks job-parameter widget named `env`
(dev|staging), defaulting to "dev" so notebooks can be run ad hoc without a Workflow around
them. Table/path naming here must match infra/terraform/modules/unity-catalog (catalog =
"ecom_{env}", schemas bronze/silver/gold) and infra/terraform/modules/storage (container URLs).
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PlatformConfig:
    env: str
    project: str = "ecom"

    @property
    def catalog(self) -> str:
        return f"{self.project}_{self.env}"

    def schema(self, layer: str) -> str:
        assert layer in ("bronze", "silver", "gold"), f"unknown layer: {layer}"
        return f"{self.catalog}.{layer}"

    def table(self, layer: str, name: str) -> str:
        return f"{self.schema(layer)}.{name}"

    def storage_account(self, storage_suffix: str) -> str:
        return f"st{self.project}{self.env}{storage_suffix}"

    def container_path(self, storage_account: str, container: str) -> str:
        return f"abfss://{container}@{storage_account}.dfs.core.windows.net"

    def checkpoint_path(self, storage_account: str, stream_name: str) -> str:
        return f"{self.container_path(storage_account, 'checkpoints')}/{self.env}/{stream_name}"


def get_config(dbutils, default_env: str = "dev") -> PlatformConfig:
    """Resolve config from a Databricks Workflow job parameter, falling back to `default_env`
    for interactive/notebook runs where the widget hasn't been set by a job."""
    dbutils.widgets.text("env", default_env)
    env = dbutils.widgets.get("env") or default_env
    return PlatformConfig(env=env)
