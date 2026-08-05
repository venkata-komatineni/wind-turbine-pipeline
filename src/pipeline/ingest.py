"""
Ingestion layer -- reading the raw per-turbine-group CSVs into one DataFrame.

This module is provided complete as plumbing/boilerplate so you can spend
your time on clean.py / stats.py / anomalies.py, which is where the actual
assessment criteria live. Feel free to change it if you disagree with the
approach (e.g. you may prefer Auto Loader / structured streaming for the
"appended daily" nature of the source -- worth a mention in README.md either
way).

Note: an earlier version of this function tagged each row with its source
file (via F.input_file_name(), then _metadata.file_path once we hit Unity
Catalog's block on the former). That column was dropped entirely -- nothing
downstream needs it, since `turbine_id` already tells you everything you'd
want to know (turbines 1-5 -> data_group_1.csv, 6-10 -> data_group_2.csv,
11-15 -> data_group_3.csv). Simpler is better here.
"""

from pyspark.sql import DataFrame, SparkSession

from .schema import RAW_SCHEMA


def read_raw(spark: SparkSession, data_dir: str) -> DataFrame:
    """
    Read every data_group_*.csv under `data_dir` into a single DataFrame,
    enforcing RAW_SCHEMA.

    Using a fixed schema (rather than inferSchema) means malformed rows
    become nulls instead of silently breaking column types -- which is what
    we want, since "missing/malformed values" is exactly what clean.py needs
    to handle.
    """
    return (
        spark.read
        .option("header", True)
        .schema(RAW_SCHEMA)
        .csv(f"{data_dir}/data_group_*.csv")
    )
