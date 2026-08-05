"""
Anomaly detection: flags turbine-days whose average power output is more
than 2 standard deviations from the fleet's average (computed across all
turbines/days in the stats DataFrame).

"Expected power output" is read here as fleet-wide behaviour -- turbines in
the same farm see broadly similar wind, so a turbine deviating from the
rest of the fleet is treated as the anomaly signal, rather than deviating
from its own historical average.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def flag_anomalies(stats_df: DataFrame, std_threshold: float = 2.0) -> DataFrame:
    fleet = stats_df.agg(
        F.mean("avg_power").alias("fleet_mean"),
        F.stddev("avg_power").alias("fleet_std"),
    ).collect()[0]

    return stats_df.withColumn(
        "is_anomaly",
        F.abs(F.col("avg_power") - F.lit(fleet["fleet_mean"]))
            > (std_threshold * F.lit(fleet["fleet_std"]))
    )
