"""
Data cleaning.

Cleans the raw turbine readings before they go into summary statistics /
anomaly detection: drops missing readings, drops physically invalid values
(negative or unrealistically high power output, out-of-range wind
direction), and drops duplicate (turbine_id, timestamp) rows.

Bounds are based on the observed data: real power_output values range
1.5-4.5, so 0-10 is used as a generous ceiling that still catches obvious
bad readings without being tuned to the exact sample range.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean(df: DataFrame) -> DataFrame:
    """
    Take the raw ingested DataFrame (see ingest.read_raw) and return a
    cleaned DataFrame with the same columns.
    """
    return (
        df
        .filter(F.col("power_output").isNotNull())
        .filter((F.col("power_output") >= 0) & (F.col("power_output") <= 10))
        .filter((F.col("wind_direction") >= 0) & (F.col("wind_direction") <= 360))
        .dropDuplicates(["turbine_id", "timestamp"])
    )
