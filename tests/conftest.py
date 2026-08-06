import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("pytest-wind-turbine")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
    yield spark