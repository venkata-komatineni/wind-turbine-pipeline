"""
Ingestion layer -- reading the raw per-turbine-group CSVs into one DataFrame.

Read every data_group_*.csv under `data_dir` into a single DataFrame,
    
- Enforcing RAW_SCHEMA.
Using a fixed schema (rather than inferSchema) means malformed rows
become nulls instead of silently breaking column types
since "missing/malformed values" is exactly what clean.py needs to handle.

"""

from pyspark.sql import DataFrame, SparkSession
from .schema import RAW_SCHEMA

def read_raw(spark: SparkSession, data_dir: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .schema(RAW_SCHEMA)
        .csv(f"{data_dir}/data_group_*.csv")
    )
