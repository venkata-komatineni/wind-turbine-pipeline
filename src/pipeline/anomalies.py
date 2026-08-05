"""
Anomaly detection.

>>> CORE PIECE #3 -- implement yourself. <<<

Requirement:
    "Identifies anomalies: Identify any turbines that have significantly
    deviated from their expected power output over the same time period.
    Anomalies can be defined as turbines whose output is outside of 2
    standard deviations from the mean."

Things to decide:
  - Mean/stddev of what, computed over what group? Almost certainly:
    per-turbine, per time-window (same window as stats.py) -- but check
    whether "expected power output" should instead be compared against
    the *fleet* average for that timestamp (turbines in the same group
    should see similar wind). Either is defensible -- pick one and say why
    in README.md.
  - Row-level anomaly (a single reading > 2 std from that turbine's mean)
    vs. window-level anomaly (a turbine whose whole day's avg is > 2 std
    from its historical mean)? The brief's phrasing ("turbines that have...
    deviated... over the same time period") leans window-level, but a
    row-level flag is also useful -- consider offering both.
  - PySpark tools that help: `F.stddev`, `F.mean`, or a `Window` spec if you
    want this computed without a full groupBy/join.
"""

from pyspark.sql import DataFrame


def flag_anomalies(stats_df: DataFrame, std_threshold: float = 2.0) -> DataFrame:
    """
    Given the output of stats.summary_statistics (or the cleaned raw data,
    your call), return a DataFrame identifying which turbines/windows are
    anomalous, with enough columns to see *why* (e.g. the deviation value).

    TODO: implement.
    """
    raise NotImplementedError("Implement your anomaly detection logic here")
