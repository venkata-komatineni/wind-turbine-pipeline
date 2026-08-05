"""
Schema and constants shared across the pipeline.

The raw CSVs look like:
    timestamp,turbine_id,wind_speed,wind_direction,power_output
    2022-03-01 00:00:00,1,11.8,169,2.7

- 15 turbines total, split across 3 files (5 turbines each), but a turbine's
  ID always lands in the same file.
- Files are appended to daily with the last 24h of readings; the source
  system is known to sometimes drop rows (sensor malfunction) -> expect gaps
  in the timestamp sequence per turbine, not just NaNs inside existing rows.
"""

from pyspark.sql.types import (
    StructType, StructField, TimestampType, IntegerType, DoubleType
)

RAW_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("turbine_id", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True),
])

# TODO (you decide, and note the reasoning in README.md):
# What are physically/operationally plausible bounds for each column?
# e.g. wind_direction is degrees (0-360), power_output can't be negative,
# a turbine has a rated max capacity, etc. Hard-coding a couple of sane
# bounds here is a legitimate, defensible choice for a POC -- just say so.
VALID_RANGES = {
    "wind_direction": (0, 360),
    # "power_output": (0, ???),   # look at the data's own distribution first
}
