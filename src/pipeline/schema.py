"""
Schema and constants shared across the pipeline.

The raw CSVs look like:
    timestamp,turbine_id,wind_speed,wind_direction,power_output
    01/03/2022 01:00	5	12.1	165	4

Data validation: 
15 turbines total, split across 3 files (5 turbines each)
 -Same turbine's ID always lands in the same file.
 -Files are appended to daily with the last 24h of readings
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

VALID_RANGES = {
    "wind_direction": (0, 360)
}
