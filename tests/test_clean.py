"""
Example test for clean.py -- shows the pattern (build a small known-bad
DataFrame, run it through `clean`, assert on the result). Add more cases of
your own: duplicates, out-of-range wind_direction, whatever your
implementation is meant to handle.
"""

from pyspark.sql import Row
from src.pipeline.clean import clean


def test_clean_removes_or_imputes_null_power_output(spark):
    df = spark.createDataFrame([
        Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0,
            wind_direction=100.0, power_output=2.5),
        Row(timestamp="2022-03-01 01:00:00", turbine_id=1, wind_speed=11.0,
            wind_direction=105.0, power_output=None),
    ])

    result = clean(df)

    # TODO: assert whatever your implementation guarantees, e.g.:
    # - no nulls remain in power_output, OR
    # - the null row was dropped, OR
    # - it was imputed to a specific expected value
    assert result.count() >= 0  # placeholder -- replace with a real assertion


# TODO: add a test for outlier handling (e.g. an impossibly high power_output)
# TODO: add a test for duplicate (turbine_id, timestamp) rows
