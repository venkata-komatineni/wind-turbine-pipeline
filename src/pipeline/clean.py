"""
Data cleaning.

Cleans the raw turbine readings data before they go into summary statistics.

Remove the missing values data from the raw data - power_output and remove the
physically invalid values (negative or unrealistic power output, out-of-range
wind direction). Drops duplicate records based on turbine_id-timestamp pairs.

Bounds are based on the observed data: 
real power_output values range 1.5-4.5, so 0-10 is used as a generous ceiling - Bad power_output readings are
dropped. 

Wind direction is bounded by 0-360 degrees.
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
