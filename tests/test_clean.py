"""
Tests for clean.py
Covers each rule the function enforces: 
    - null power_output
    - out-of-range power_output
    - out-of-range wind_direction
    - duplicate (turbine_id, timestamp) rows.
"""

from pyspark.sql import Row
from src.pipeline.clean import clean


def test_clean_drops_null_power_output(spark):
    df = spark.createDataFrame([
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=2.5),
        Row(timestamp="2022-03-01 01:00:00", turbine_id=1, wind_speed=11.0,
            wind_direction=105.0, power_output=None),
    ])

    result = clean(df)

    assert result.count() == 1
    assert result.filter(result.power_output.isNull()).count() == 0


def test_clean_drops_out_of_range_power_output(spark):
    df = spark.createDataFrame([
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=2.5),   # valid
        Row(timestamp="2022-03-01 01:00:00", turbine_id=1, wind_speed=11.0,
            wind_direction=105.0, power_output=999.0),  # too high
        Row(timestamp="2022-03-01 02:00:00", turbine_id=1, wind_speed=12.0,
            wind_direction=108.0, power_output=-5.0),   # negative
    ])

    result = clean(df)

    assert result.count() == 1
    assert result.collect()[0]["power_output"] == 2.5


def test_clean_drops_out_of_range_wind_direction(spark):
    df = spark.createDataFrame([
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=2.5),   # valid
        Row(timestamp="2022-03-01 01:00:00", turbine_id=1, wind_speed=11.0,
            wind_direction=400.0, power_output=2.8),   # invalid direction
    ])

    result = clean(df)

    assert result.count() == 1
    assert result.collect()[0]["wind_direction"] == 100.0


def test_clean_drops_duplicate_turbine_timestamp_rows(spark):
    df = spark.createDataFrame([
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=2.5),
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=2.5),   # exact duplicate
    ])

    result = clean(df)

    assert result.count() == 1
