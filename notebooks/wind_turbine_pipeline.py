# Databricks notebook source
# MAGIC %md
# MAGIC # Wind Turbine Data Pipeline -- POC
# MAGIC
# MAGIC Colibri Digital take-home assessment.
# MAGIC
# MAGIC This notebook is the entry point. It imports the `src/pipeline` modules
# MAGIC and walks through: ingest -> clean -> summary stats -> anomaly detection
# MAGIC -> store. The actual logic lives in `src/pipeline/*.py` so it's testable
# MAGIC with pytest outside the notebook too (see tests/).
# MAGIC
# MAGIC **How to run this on Databricks:**
# MAGIC 1. Add this repo to your workspace via *Repos > Add Repo* (Databricks Repos
# MAGIC    supports Git-backed repos and lets notebooks import local `.py` modules
# MAGIC    directly, no packaging needed).
# MAGIC 2. Upload the `data/*.csv` files to a Unity Catalog Volume (DBFS isn't
# MAGIC    available on Free Edition -- Volumes are the supported option; see
# MAGIC    cell below).
# MAGIC 3. Free Edition is serverless-only -- just run all cells, no cluster to
# MAGIC    attach.

# COMMAND ----------

# MAGIC %md ### 0. Setup: make `src/` importable and point at the data

# COMMAND ----------

# MAGIC %md
# MAGIC If you've just edited a `src/pipeline/*.py` file and re-running a cell
# MAGIC still shows the *old* behaviour, Python has the old module cached from
# MAGIC an earlier import in this session. Uncomment and run the line below,
# MAGIC then re-run the import cell and whichever stage cell you're working on.

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

from src.pipeline.ingest import read_raw
from src.pipeline.clean import clean
from src.pipeline.stats import summary_statistics
from src.pipeline.anomalies import flag_anomalies
from src.pipeline.storage import write_table

# COMMAND ----------

# MAGIC %md
# MAGIC Upload the CSVs from the repo's `data/` folder to a Unity Catalog Volume
# MAGIC (Catalog > pick/create a catalog+schema > Create > Volume > Upload).
# MAGIC Set `DATA_DIR` below to the resulting path, e.g.
# MAGIC `/Volumes/workspace/default/wind_turbine_data`.

# COMMAND ----------

DATA_DIR = "/Volumes/workspace/default/wind_turbine_data"   # <-- change to your actual Volume path

# COMMAND ----------

# MAGIC %md ### 1. Ingest

# COMMAND ----------

raw_df = read_raw(spark, DATA_DIR)
display(raw_df.limit(20))
print(f"row count: {raw_df.count()}")

# COMMAND ----------

# MAGIC %md ### 2. Clean
# MAGIC
# MAGIC The provided sample data is actually clean (no nulls/outliers) -- see
# MAGIC the scratch cell below for a small hand-built DataFrame with known bad
# MAGIC values to develop and sanity-check `clean()` against, without needing
# MAGIC to touch any files or Volumes. **Delete the scratch cell before final
# MAGIC submission** -- it's a dev aid, not part of the pipeline.

# COMMAND ----------

# SCRATCH / DEV ONLY -- delete before submitting.
from pyspark.sql import Row

test_df = spark.createDataFrame([
    Row(timestamp="2022-03-01 00:00:00", turbine_id=1, wind_speed=10.0, wind_direction=100.0, power_output=2.5),
    Row(timestamp="2022-03-01 01:00:00", turbine_id=1, wind_speed=11.0, wind_direction=105.0, power_output=None),   # missing value
    Row(timestamp="2022-03-01 02:00:00", turbine_id=1, wind_speed=12.0, wind_direction=110.0, power_output=999.0), # obvious outlier
    Row(timestamp="2022-03-01 03:00:00", turbine_id=1, wind_speed=13.0, wind_direction=400.0, power_output=2.8),   # invalid wind_direction (>360)
    Row(timestamp="2022-03-01 00:00:00", turbine_id=2, wind_speed=9.0,  wind_direction=90.0,  power_output=3.1),
])

display(clean(test_df))   # once clean() is implemented, inspect the result here first

# COMMAND ----------

cleaned_df = clean(raw_df)
display(cleaned_df.limit(20))

# COMMAND ----------

# MAGIC %md ### 3. Summary statistics (min / max / avg power per turbine per window)

# COMMAND ----------

stats_df = summary_statistics(cleaned_df)
display(stats_df)

# COMMAND ----------

# MAGIC %md ### 4. Anomaly detection (> 2 std dev from expected output)

# COMMAND ----------

anomalies_df = flag_anomalies(stats_df)
display(anomalies_df)

# COMMAND ----------

# MAGIC %md ### 5. Persist to Delta tables

# COMMAND ----------

write_table(cleaned_df, "turbine_readings_clean")
write_table(stats_df, "turbine_summary_stats")
write_table(anomalies_df, "turbine_anomalies")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- sanity check
# MAGIC SELECT * FROM turbine_summary_stats ORDER BY turbine_id LIMIT 20;
