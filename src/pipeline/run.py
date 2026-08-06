"""
End-to-end orchestration: ingest -> clean -> stats -> anomalies -> store.

    python -m src.pipeline.run --data-dir data --local

"""

import argparse

from pyspark.sql import SparkSession

from .ingest import read_raw
from .clean import clean
from .stats import summary_statistics
from .anomalies import flag_anomalies
from .storage import write_table

def main(data_dir: str, local: bool):
    spark = (
        SparkSession.builder.appName("wind-turbine-pipeline")
        .master("local[*]" if local else None)
        .getOrCreate()
    ) if local else SparkSession.builder.getOrCreate()

    raw_df = read_raw(spark, data_dir)
    cleaned_df = clean(raw_df)
    stats_df = summary_statistics(cleaned_df)
    anomalies_df = flag_anomalies(stats_df)

    print("=== cleaned sample ===")
    cleaned_df.show(5, truncate=False)
    print("=== summary statistics ===")
    stats_df.show(20, truncate=False)
    print("=== anomalies ===")
    anomalies_df.show(20, truncate=False)

    as_delta = not local  # Delta needs the delta-spark package locally
    write_table(cleaned_df,
                "output/turbine_readings_clean" if local else "turbine_readings_clean",
                as_delta_table=as_delta)
    write_table(stats_df,
                "output/turbine_summary_stats" if local else "turbine_summary_stats",
                as_delta_table=as_delta)
    write_table(anomalies_df,
                "output/turbine_anomalies" if local else "turbine_anomalies",
                as_delta_table=as_delta)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--local", action="store_true",
                         help="run with a local SparkSession instead of Databricks")
    args = parser.parse_args()
    main(args.data_dir, args.local)
