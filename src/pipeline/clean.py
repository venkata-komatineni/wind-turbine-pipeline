"""
Data cleaning.

>>> THIS IS ONE OF THE CORE PIECES THE ASSESSMENT IS EVALUATING. <<<
Implement it yourself -- the scaffolding below just gets you started and
lists the things you need to decide on. Delete these comments once you've
made your calls and documented them in README.md.

Requirement (from the brief):
    "Cleans the data: The raw data contains missing values and outliers,
    which must be removed or imputed."

Things to decide (and write down in README.md -> "Assumptions"):
  1. What counts as "missing"? Null power_output? A missing timestamp
     row entirely for a turbine (sensor didn't report at all)?
  2. Impute or drop? If you impute, with what (per-turbine rolling mean?
     forward-fill? interpolate)? Different columns may deserve different
     treatment.
  3. What counts as an "outlier" here vs. an "anomaly" (see anomalies.py)?
     The brief separates the two concepts -- cleaning removes/fixes bad
     sensor readings (impossible values), anomaly detection flags real but
     unusual turbine behaviour. Keep the distinction clear and explain it.
  4. Duplicates: can the same (turbine_id, timestamp) appear twice? What do
     you do if it does?
"""

from pyspark.sql import DataFrame


def clean(df: DataFrame) -> DataFrame:
    """
    Take the raw ingested DataFrame (see ingest.read_raw) and return a
    cleaned DataFrame with the same columns.

    TODO: implement.
    """
    raise NotImplementedError("Implement your cleaning logic here")
