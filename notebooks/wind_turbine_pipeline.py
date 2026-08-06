# Databricks notebook source
# MAGIC %md
# MAGIC # Wind Turbine Data Pipeline - POC
# MAGIC
# MAGIC Colibri Digital take-home assessment.
# MAGIC
# MAGIC This notebook is the entry point. It imports the `src/pipeline` modules
# MAGIC and walks through: ingest -> clean -> summary stats -> anomaly detection
# MAGIC -> store. 
# MAGIC
# MAGIC The actual logic lives in `src/pipeline/*.py` 
# MAGIC
# MAGIC **How to run this on Databricks:**
# MAGIC 1. Add this repo to your workspace via *Repos > Add Repo* (Databricks Repos
# MAGIC    supports Git-backed repos and lets notebooks import local `.py` modules
# MAGIC    directly, no packaging needed).
# MAGIC 2. Upload the `data/*.csv` files to a Unity Catalog Volume - Volumes are the supported option; see
# MAGIC    cell below).
# MAGIC 3. Free Edition is serverless-only -- just run all cells, no cluster to
# MAGIC    attach.

# COMMAND ----------

#  dbutils.library.restartPython()

from src.pipeline.ingest import read_raw
from src.pipeline.clean import clean
from src.pipeline.stats import summary_statistics
from src.pipeline.anomalies import flag_anomalies
from src.pipeline.storage import write_table



# COMMAND ----------

# MAGIC %md
# MAGIC Upload the CSVs from the repo's `data/` folder to a Unity Catalog Volume
# MAGIC
# MAGIC Set `DATA_DIR` below to the resulting path, e.g.
# MAGIC `/Volumes/workspace/default/wind_turbine_data`.

# COMMAND ----------

DATA_DIR = "/Volumes/workspace/default/wind_turbine_data"  

# COMMAND ----------

# MAGIC %md ### 1. Ingest

# COMMAND ----------

raw_df = read_raw(spark, DATA_DIR)
display(raw_df.limit(20))
print(f"row count: {raw_df.count()}")

# COMMAND ----------

# MAGIC %md ### 2. Clean

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