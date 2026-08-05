# Wind Turbine Data Pipeline

Colibri Digital take-home technical assessment.

## What it does

Reads turbine CSVs, cleans bad values, works out min/max/avg power per
turbine per day, flags turbines that look unusual, saves results as tables.

## How I cleaned the data

- Dropped rows with no power_output value.
- Dropped rows where power_output was negative or above 10 (real values
  only go up to 4.5, so this catches obviously broken readings).
- Dropped rows where wind_direction wasn't between 0 and 360.
- Removed duplicate rows for the same turbine and timestamp.

## How I calculated stats

Grouped by turbine and day, took min/max/avg of power_output.

## How I found anomalies

Compared each turbine's daily average to the average and standard
deviation across all turbines/days. Flagged anything more than 2 standard
deviations away.

## Assumptions

- Only treated a missing power_output value as "missing" -- didn't check
  for whole missing hours.
- Used simple min/max bounds instead of statistics for cleaning, to keep
  it separate from anomaly detection.
- Used the whole fleet's average as the "expected" baseline, not each
  turbine's own history.
- "Time period" is a calendar day, not a rolling 24h window.

## Project structure

```
src/pipeline/
  schema.py      expected schema
  ingest.py      read raw CSVs -> DataFrame
  clean.py       missing values / outlier handling
  stats.py       per-turbine min/max/avg per day
  anomalies.py   >2 std dev anomaly flagging
  storage.py     persist cleaned data + stats as Delta tables
  run.py         CLI orchestration (local or Databricks)
notebooks/
  wind_turbine_pipeline.py   Databricks notebook
tests/
  test_clean.py / test_stats.py / test_anomalies.py
data/
  data_group_1.csv, data_group_2.csv, data_group_3.csv (provided sample)
```

## How to run

See `GUIDE.md` for full local + Databricks setup, run, and test instructions.

## If this went to production

- Use Auto Loader instead of re-reading all CSVs each time.
- Use MERGE so re-running a day doesn't duplicate data.
- Add monitoring/alerts on anomalies.
- Talk to someone on the team about whether fleet-wide or per-turbine
  anomaly detection makes more sense.
- Partition the Delta tables for query performance at scale.
