"""
Storage layer -- persisting cleaned data + summary stats "in a database".

Provided mostly complete as plumbing, but you should still make (and note
in README.md) a real decision about *what* "database" means here for a POC
vs. production:

  - On Databricks: a Delta table (via `saveAsTable`) is the natural choice --
    it's ACID, queryable with SQL, and versioned. That's what this defaults
    to.
  - Running locally without Databricks/Delta: falls back to Parquet on disk,
    which is fine for a POC but isn't really "a database" (no ACID, no easy
    querying) -- worth flagging as a limitation to discuss in the interview
    (e.g. "in production I'd use a Delta table on Unity Catalog / a
    warehouse like Postgres for the aggregated stats").
"""

from pyspark.sql import DataFrame


def write_table(df: DataFrame, path_or_table: str, mode: str = "overwrite",
                 as_delta_table: bool = True) -> None:
    """
    Persist `df`.

    If as_delta_table=True, `path_or_table` is treated as a table name and
    written with `.saveAsTable(...)` in Delta format (Databricks default
    format already). Otherwise `path_or_table` is treated as a filesystem
    path and written as Parquet.
    """
    writer = df.write.mode(mode)
    if as_delta_table:
        writer.format("delta").saveAsTable(path_or_table)
    else:
        writer.parquet(path_or_table)
