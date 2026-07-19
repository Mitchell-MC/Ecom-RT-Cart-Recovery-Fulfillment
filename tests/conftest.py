"""Local Spark session fixture for unit tests. Requires a JVM (Java 11/17) -- not runnable in
environments without one; CI (.github/workflows/ci.yml) installs Java before running pytest.
"""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]
for module_dir in ["src/common", "src/quality", "src/transform/silver", "src/transform/gold",
                    "src/ingestion/batch", "src/ingestion/streaming"]:
    sys.path.insert(0, str(REPO_ROOT / module_dir))


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("ecom-signal-platform-unit-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
