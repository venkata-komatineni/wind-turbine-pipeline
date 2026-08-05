"""
Tests for stats.py -- confirms min/max/avg are computed correctly per
turbine per day.
"""

from pyspark.sql import Row
from src.pipeline.stats import summary_statistics


def test_summary_statistics_min_max_avg(spark):
    df = spark.createDataFrame([
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=2.0),
        Row(timestamp="2022-03-01 01:00:00", turbine_id=1, wind_speed=11.0,
            wind_direction=105.0, power_output=4.0),
        Row(timestamp="2022-03-01 02:00:00", turbine_id=1, wind_speed=12.0,
            wind_direction=108.0, power_output=3.0),
    ])

    result = summary_statistics(df).collect()

    assert len(result) == 1
    row = result[0]
    assert row["turbine_id"] == 1
    assert row["min_power"] == 2.0
    assert row["max_power"] == 4.0
    assert row["avg_power"] == 3.0


def test_summary_statistics_groups_by_turbine_and_day(spark):
    df = spark.createDataFrame([
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=2.0),
        Row(timestamp="2022-03-02 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=3.0),
        Row(timestamp="2022-03-01 00:00:00", turbine_id=2, wind_speed=10.0,
            wind_direction=100.0, power_output=5.0),
    ])

    result = summary_statistics(df)

    # 3 distinct (turbine_id, day) groups -> 3 rows
    assert result.count() == 3
