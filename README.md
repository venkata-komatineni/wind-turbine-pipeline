# Wind Turbine Data Pipeline

Colibri Digital take-home technical assessment.

## What it does

- Reads turbine CSV files
- Cleans bad values.
- Works out min/max/avg power per turbine per day.
- Flags turbines that look unusual.
- Saves results as tables.

## How I cleaned the data

- Dropped rows with no power_output value.
- Dropped rows where power_output was negative or above 10 (real values
  only go up to 4.5, so this catches obviously broken readings).
- Dropped rows where wind_direction wasn't between 0 and 360.
- Removed duplicate rows for the same turbine and timestamp.

## How I calculated stats

Grouped by turbine and day and took min/max/avg of power_output.

## How I found anomalies

Compared each turbine's daily average to the average and standard
deviation across all turbines/days. Flagged anything more than 2 standard
deviations away.

## Assumptions

- Only treated a missing power_output value as "missing" 
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
data/
  data_group_1.csv, data_group_2.csv, data_group_3.csv (provided sample)
```


## Productionize suggestions 

- Use Auto Loader instead of re-reading all CSVs each time.
- Use MERGE so re-running a day doesn't duplicate data.
- Partition the Delta tables for query performance at scale.
