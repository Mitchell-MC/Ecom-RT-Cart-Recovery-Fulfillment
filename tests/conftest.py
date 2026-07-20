"""Local Spark session fixture for unit tests. Requires a JVM (Java 11/17) -- not runnable in
environments without one; CI (.github/workflows/ci.yml) installs Java before running pytest.
"""

import sys
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]
for module_dir in [
    "src/common",
    "src/quality",
    "src/transform/silver",
    "src/transform/gold",
    "src/ingestion/batch",
    "src/ingestion/streaming",
]:
    sys.path.insert(0, str(REPO_ROOT / module_dir))


@pytest.fixture(scope="session")
def spark():
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ecom-signal-platform-unit-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    # `pip install delta-spark` only ships the Python bindings -- the Scala JARs that actually
    # implement DeltaSparkSessionExtension/DeltaCatalog are resolved from Maven at session start.
    # Without this the two .config() lines above fail the session with
    # "Cannot find catalog plugin class for catalog 'spark_catalog'". This helper appends the
    # io.delta:delta-spark coordinate matching the installed delta-spark version.
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
