# PySpark End-to-End Guide — Wind Turbine Pipeline

This is the teaching companion to `GUIDE.md` (which covers install/run/test/deploy
mechanics). This document explains **PySpark itself** — the concepts and functions
you need — and walks through **what to build and why**, requirement by requirement,
so you can write `clean.py`, `stats.py` and `anomalies.py` yourself with an
understanding of the tradeoffs, not just copied code.

---

## Part 1 — PySpark fundamentals

### 1.1 Why Spark for this at all?

The brief says "your pipeline should be scalable." Your actual sample data is
~11,000 rows — pandas would handle that instantly. The point of using PySpark is
that the *same code* would work if this were 15,000 turbines reporting every
second instead of 15 turbines reporting hourly. Spark distributes the work across
multiple cores/machines; pandas runs on a single core in memory. That's the "why
PySpark" answer to have ready for the interview.

### 1.2 SparkSession — the entry point

Every PySpark program starts with a `SparkSession`. On Databricks it's already
created for you as the variable `spark` in every notebook. Locally, you create it
yourself:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("demo").getOrCreate()
```

`local[*]` means "run on this machine, use all available cores" — that's your
local dev stand-in for a real cluster.

### 1.3 DataFrames, transformations, actions, and lazy evaluation

A Spark **DataFrame** looks like a pandas DataFrame (rows + named, typed columns)
but the execution model is different, and this trips people up the most:

- **Transformations** (`.select()`, `.filter()`, `.withColumn()`, `.groupBy()`,
  `.join()`...) don't actually run anything. They just build up a plan —
  "when someone asks for the result, here's how to compute it."
- **Actions** (`.show()`, `.count()`, `.collect()`, `.write...`) are what trigger
  Spark to actually execute the plan.

Why this matters for you: you can chain ten `.withColumn()` calls and nothing
runs until you call `.show()` at the end — Spark optimizes the whole chain
together first. It also means if you write a bug in a transformation, you won't
see an error until you call an action later in the notebook — so test each step
with `.show()` as you go rather than writing the whole pipeline blind.

### 1.4 The building blocks you'll actually use

Import convention — you'll see this everywhere, including in the scaffold:

```python
from pyspark.sql import functions as F
```

| Need | Function(s) | Notes |
|---|---|---|
| Pick columns | `df.select("a", "b")` | |
| Add/replace a column | `df.withColumn("c", F.col("a") + 1)` | Never mutates `df` — always returns a new DataFrame. `df = df.withColumn(...)` is the pattern. |
| Filter rows | `df.filter(F.col("power_output").isNotNull())` or `df.where(...)` | `filter` and `where` are identical, pick one and be consistent |
| Conditional logic | `F.when(cond, val).otherwise(other_val)` | PySpark's if/else for columns — this is your main tool for imputation logic |
| Group + aggregate | `df.groupBy("turbine_id").agg(F.min("power_output"), F.max("power_output"), F.avg("power_output"))` | This is directly your `stats.py` |
| Row-level stats without collapsing rows | `Window` functions | See 1.5 below — needed for anomaly detection if you want per-row deviation without losing rows |
| Combine two DataFrames | `df.join(other_df, on=["turbine_id"], how="left")` | Useful if you compute stats separately then need to bring them back alongside raw rows |
| Null checks | `F.col("x").isNull()`, `df.na.drop()`, `df.na.fill(value)` | `.na.drop()` and `.na.fill()` are shortcuts for the common cases |

### 1.5 Window functions (needed for anomaly detection)

`groupBy().agg()` **collapses** rows — you get one row per turbine. Sometimes you
want to keep every original row but *also* know that row's group average (e.g. to
compute "how far is this specific reading from its turbine's mean?"). That's what
a `Window` is for:

```python
from pyspark.sql import Window

w = Window.partitionBy("turbine_id")
df = df.withColumn("turbine_avg", F.avg("power_output").over(w))
df = df.withColumn("turbine_stddev", F.stddev("power_output").over(w))
```

Now every row still exists, but each one also carries its turbine's average and
standard deviation as extra columns — which is exactly what you need to compute
"is this reading more than 2 standard deviations from the mean?" with a simple
`F.when()` afterward. This is one legitimate way to implement `anomalies.py`; the
alternative (compute stats with `groupBy`, then `join` back to the raw/summary
data) is equally valid — it's a style choice, pick whichever reads more clearly
to you and say why in README.md.

### 1.6 Explicit schemas over `inferSchema`

The scaffold's `ingest.py` reads CSVs with a fixed `StructType` rather than
`.option("inferSchema", True)`. Why: `inferSchema` makes Spark scan the data
first to guess types, which is slower and — more importantly for this
assignment — unpredictable. If a malformed row has text where a number should
be, a fixed schema turns it into a `null` in that column (which `clean.py` is
designed to catch), whereas `inferSchema` might just widen the column to string
type for everyone, silently breaking your downstream math. Explicit schemas are
also just a Spark production best practice worth naming as a design choice in
your README.

### 1.7 Common mistakes worth knowing before you start

- **Forgetting DataFrames are immutable.** `df.filter(...)` doesn't change `df`
  — it returns a new one. If you don't reassign it (`df = df.filter(...)`), your
  filter silently does nothing.
- **`==` vs `.eqNullSafe()` for null comparisons.** `F.col("x") == None` doesn't
  work the way you'd expect in SQL-style engines — use `F.col("x").isNull()`.
- **Calling `.show()`/`.count()` everywhere while debugging is fine** — it's a
  POC, not a hot loop. Just remove excessive ones before final submission for
  readability.
- **`collect()` pulls all data to the driver.** Fine for this data size, but
  it's the thing that breaks "scalable" at real volume — avoid it in your actual
  pipeline logic (using it in a `print` for your own debugging is fine).

---

## Part 2 — What "done" looks like, requirement by requirement

For each piece, this is the brief's ask, the decisions the scaffold flags, and
the PySpark technique that applies — with an illustrative snippet on **throwaway
example data**, not your actual pipeline, so you still write the real thing.

### 2.1 Clean (`src/pipeline/clean.py`)

**Ask:** raw data has missing values and outliers; remove or impute them.

**Decisions to make (write these in README → Assumptions):**
1. What counts as missing? A null `power_output` in an existing row is
   obviously missing. A turbine simply having *no row* for an hour (dropped
   entirely, per the brief's "system sometimes misses entries") is a different
   kind of missing — decide if you need to detect and backfill timestamp gaps,
   or if you're only handling in-row nulls for this POC (a reasonable scope
   call, just say so).
2. Impute or drop? Both are legitimate for a POC. If you impute, a common,
   defensible choice for a turbine's power output is **per-turbine mean or
   forward-fill**, since a turbine's output correlates with its own recent
   readings (wind conditions change gradually), not the fleet average.
3. What is an "outlier" for this domain? Two common approaches:
   - **Domain bounds**: values outside physically possible limits (e.g.
     negative power output, wind_direction outside 0–360°). Simple, explainable,
     good for a POC.
   - **Statistical**: values outside N standard deviations of that column's
     distribution. Note this uses the *same* "N std dev" idea the brief asks
     you to use for anomalies — worth explicitly distinguishing "outlier =
     bad sensor reading, filtered out before analysis" from "anomaly = a real
     but unusual turbine behaviour, the thing we want to surface," so you
     aren't accidentally solving the same problem twice with different names.

**Illustrative technique** (toy example, not your solution):

```python
# Example only -- shows the pattern, not your actual clean() logic.
toy = spark.createDataFrame(
    [(1, 10.0), (1, None), (1, 999.0), (2, 5.0)], ["id", "value"]
)

w = Window.partitionBy("id")
toy_imputed = toy.withColumn(
    "value_clean",
    F.when(F.col("value").isNull(), F.avg("value").over(w))
     .otherwise(F.col("value"))
)
```

Dropping instead of imputing is simpler: `df.filter(F.col("power_output").isNotNull())`
or `df.na.drop(subset=["power_output"])`.

### 2.2 Summary statistics (`src/pipeline/stats.py`)

**Ask:** min/max/avg power output per turbine, over a time period (e.g. 24h).

**Decisions to make:**
- Calendar day (`2022-03-01 00:00` to `23:59`) vs. rolling 24h trailing window
  from "now"? Calendar day is simpler and matches "the csv is updated with the
  last 24 hours" framing in the brief — a defensible default.
- Getting the calendar day from a timestamp: `F.date_trunc("day", F.col("timestamp"))`
  or `F.to_date(F.col("timestamp"))` gives you a groupable column.

**Illustrative technique:**

```python
# Example only.
toy.groupBy("id").agg(
    F.min("value").alias("min_value"),
    F.max("value").alias("max_value"),
    F.avg("value").alias("avg_value"),
)
```

For per-day-per-turbine, group by two columns:

```python
df.withColumn("day", F.to_date("timestamp")) \
  .groupBy("turbine_id", "day") \
  .agg(F.min("power_output").alias("min_power"), ...)
```

### 2.3 Anomaly detection (`src/pipeline/anomalies.py`)

**Ask:** flag turbines whose output is outside 2 standard deviations from "the
expected power output" over the same time period.

**Decisions to make:**
- Baseline population for mean/stddev: per-turbine across time (is *today*
  unusual for *this turbine*?), or across the turbine fleet at a point in time
  (is this turbine unusual *right now* compared to its neighbours, who should
  see similar wind)? Both are valid readings of "expected power output" —
  pick one, and mention the other as something you'd validate with a domain
  expert in production.
- Row-level flag vs. window/day-level flag — the brief's wording ("turbines
  that have... deviated... over the same time period") suggests day-level
  (using the min/max/avg you already computed in stats.py), which also means
  `anomalies.py` can consume `stats.py`'s output directly rather than
  recomputing from raw data.

**Illustrative technique** — computing mean/stddev of the *whole* stats.py
output (i.e., "is this turbine's daily average unusual compared to the whole
fleet's daily averages?"):

```python
# Example only -- operates on already-aggregated stats, not raw readings.
fleet_stats = stats_df.agg(
    F.mean("avg_power").alias("fleet_mean"),
    F.stddev("avg_power").alias("fleet_std"),
).collect()[0]  # small aggregate result, safe to collect

flagged = stats_df.withColumn(
    "is_anomaly",
    F.abs(F.col("avg_power") - F.lit(fleet_stats["fleet_mean"]))
        > 2 * F.lit(fleet_stats["fleet_std"])
)
```

The per-turbine-over-time version instead partitions a `Window` by
`turbine_id` (not the fleet) and compares each day's value to that turbine's
own historical mean/stddev — same `F.when`/comparison pattern, different
`Window.partitionBy(...)`.

### 2.4 Storage (`src/pipeline/storage.py`)

Provided complete — Delta table on Databricks, Parquet locally. Your job here
is just to justify the choice in README, not implement more Spark logic:
Delta gives you ACID writes and SQL queryability for "further analysis" (as
the brief asks), which plain Parquet or CSV doesn't.

---

## Part 3 — Step-by-step build process

1. **Environment up.** Follow `GUIDE.md` Part 0 — either local venv + `pytest`
   for fast iteration, or Databricks directly. Recommended: local first.
2. **Look at the data yourself** before writing anything:
   ```python
   df = spark.read.option("header", True).csv("data/data_group_1.csv")
   df.printSchema()
   df.show(10)
   df.describe("power_output").show()
   ```
   Confirms column names/types match what `schema.py` expects, and gives you a
   feel for the value ranges before you decide on bounds/thresholds.
3. **Generate dirty test data**: `python scripts/make_dirty_data.py` — the
   provided sample is clean, so you need this to actually exercise your
   cleaning logic.
4. **Implement `clean.py`.** Use `data/dirty/` as input. Print/`.show()` before
   and after on a few rows to eyeball that nulls/outliers were handled the way
   you intended. Then write the assertions in `tests/test_clean.py` to lock
   that behaviour in.
5. **Implement `stats.py`.** Sanity check by hand: filter the raw data to one
   turbine, one day, in a spreadsheet or with `.toPandas()`, confirm your
   Spark output's min/max/avg match.
6. **Implement `anomalies.py`.** Deliberately construct a small DataFrame
   where one turbine is obviously unusual (like `make_dirty_data.py`'s outlier
   injection) and confirm your logic flags it, and doesn't flag normal ones.
7. **Run end-to-end**: `python -m src.pipeline.run --data-dir data/dirty --local`
   (or the full pipeline via the notebook on Databricks).
8. **Run `pytest tests/ -v`** — all green before you consider this done.
9. **Fill in `README.md`** — design + assumptions, now that you know what you
   actually built.
10. **Push to GitHub**, send the link. See `GUIDE.md` Part 3 for exact commands.
11. **Re-read your own "Productionising" notes** before the follow-up
    interview — see `GUIDE.md` Part 4.

---

## Quick reference — functions cheat sheet

```python
from pyspark.sql import functions as F
from pyspark.sql import Window

F.col("x")                          # reference a column
F.lit(5)                            # a literal value as a column
F.when(cond, val).otherwise(other)  # if/else
F.isnull(F.col("x")) / F.col("x").isNull()
df.na.drop(subset=[...]) / df.na.fill(value, subset=[...])
df.groupBy(...).agg(F.min(...), F.max(...), F.avg(...), F.stddev(...))
Window.partitionBy(...).orderBy(...)
F.col("x").over(window)
F.to_date(...), F.date_trunc("day", ...)
df.withColumn("new_col", <expr>)
df.filter(<condition>) / df.where(<condition>)
df.join(other, on=[...], how="left")
```
