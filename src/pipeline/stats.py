"""
Summary statistics.

>>> CORE PIECE #2 -- implement yourself. <<<

Requirement:
    "Calculates summary statistics: For each turbine, calculate the
    minimum, maximum, and average power output over a given time period
    (e.g., 24 hours)."

Things to decide:
  - Is the "time period" a calendar day, or a trailing/rolling 24h window?
    Either is defensible for a POC -- pick one, state it.
  - groupBy + agg is the natural PySpark tool here. Should this run on the
    cleaned data (probably yes -- cleaning happens first in the pipeline).
"""

from pyspark.sql import DataFrame


def summary_statistics(df: DataFrame, window: str = "1 day") -> DataFrame:
    """
    Return a DataFrame with (turbine_id, window_start, window_end,
    min_power, max_power, avg_power) -- or whatever column names you
    prefer, just be consistent and document them.

    TODO: implement.
    """
    raise NotImplementedError("Implement your summary statistics logic here")
