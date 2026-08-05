"""
Ingestion layer -- reading the raw per-turbine-group CSVs into one DataFrame.

This module is provided complete as plumbing/boilerplate so you can spend
your time on clean.py / stats.py / anomalies.py, which is where the actual
assessment criteria live. Feel free to change it if you disagree with the
approach (e.g. you may prefer Auto Loader / structured streaming for the
"appended daily" nature of the source -- worth a mention in README.md either
way).
"""

from pyspark.sql import DataFrame, SparkSession
from .schema import RAW_SCHEMA

def read_raw(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    Read every data_group_*.csv under `data_dir` into a single DataFrame,
    enforcing RAW_SCHEMA.
    """
    return (
        spark.read
        .option("header", True)
        .schema(RAW_SCHEMA)
        .csv(f"{data_dir}/data_group_*.csv")
    )
