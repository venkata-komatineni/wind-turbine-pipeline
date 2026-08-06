"""
Tests for anomalies.py.

Confirms a turbine whose average is far from the fleet average gets flagged
Turbines close to the fleet average don't.
"""

from pyspark.sql import Row
from src.pipeline.anomalies import flag_anomalies


def test_flag_anomalies_flags_clear_outlier(spark):
    # 10 turbine-days clustered around ~3.0, one clear outlier at 6.0.
    # (Needs a reasonably sized "normal" group -- with too few points, a
    # single extreme value inflates its own std dev enough to mask itself.)
    
    normal_avgs = [3.0, 3.1, 2.9, 3.05, 2.95, 3.02, 2.98, 3.1, 2.9, 3.0]
    rows = [
        Row(turbine_id=i + 1, day="2022-03-01", min_power=avg - 0.5,
            max_power=avg + 0.5, avg_power=avg)
        for i, avg in enumerate(normal_avgs)
    ]
    rows.append(Row(turbine_id=99, day="2022-03-01", min_power=5.5,
                     max_power=6.5, avg_power=6.0))

    stats_df = spark.createDataFrame(rows)

    result = flag_anomalies(stats_df).collect()
    flagged = {row["turbine_id"] for row in result if row["is_anomaly"]}

    assert 99 in flagged
    assert 1 not in flagged
