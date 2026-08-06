"""
Storage layer - persisting cleaned data + summary stats in a database.

On Databricks: a Delta table (via `saveAsTable`) is the natural choice 
  - It's ACID, queryable with SQL, and versioned. 
  - Running locally without Databricks/Delta: falls back to Parquet on disk
"""

from pyspark.sql import DataFrame


def write_table(df: DataFrame, path_or_table: str, mode: str = "overwrite",
                 as_delta_table: bool = True) -> None:
   
    writer = df.write.mode(mode)
    if as_delta_table:
        writer.format("delta").saveAsTable(path_or_table)
    else:
        writer.parquet(path_or_table)
