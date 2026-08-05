# End-to-End Guide

This is your companion for actually getting this running -- setup, coding,
testing, and submitting. The scaffold in `src/pipeline/` deliberately leaves
`clean.py`, `stats.py` and `anomalies.py` as `NotImplementedError` stubs:
that's the part the assessment is evaluating, so write those yourself. This
guide handles everything around that.

---

## 0. Two ways to run this: local PySpark, or Databricks

You only need one. Databricks matches what the recruiter asked for
("Databricks notebook"), so do that as your primary path -- but local is
much faster to iterate on while writing `clean.py` etc., since you skip
cluster startup time on every change. Recommended flow: write + test locally
first, then paste the working notebook into Databricks for the final
submission and to confirm it runs there.

### Option A -- Local PySpark (fast iteration + running pytest)

**Install (one-time):**

```bash
# You need Java 8/11/17 (Spark requires a JDK) and Python 3.10-3.11
java -version          # if missing: install via your OS package manager /
                        # https://adoptium.net (Temurin JDK 17 is a safe pick)
python3 --version

cd wind-turbine-pipeline
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Note on Delta locally: `delta-spark` needs the SparkSession configured with
the Delta extensions, which is more setup than a POC needs. The scaffold's
`run.py --local` flag writes Parquet instead of Delta locally for
simplicity (see `storage.py`) -- so you don't need to fight Delta config
just to test on your laptop. On Databricks, Delta works out of the box.

**Run the pipeline end-to-end:**

```bash
python -m src.pipeline.run --data-dir data --local
```

This will fail with `NotImplementedError` until you've implemented
`clean.py` / `stats.py` / `anomalies.py` -- that's expected, work through
them one at a time (comment out the later stages in `run.py` if you want to
test `clean` in isolation first).

**Run the tests:**

```bash
pytest tests/ -v
```

The tests import your implementations directly, so `pytest` is the fastest
feedback loop while you build -- no need to launch the notebook each time.

**Generate dirty test data** (the provided sample CSVs are actually clean --
see `scripts/make_dirty_data.py`):

```bash
python scripts/make_dirty_data.py
python -m src.pipeline.run --data-dir data/dirty --local
```

### Option B -- Databricks (Free Edition)

Note: Databricks **Community Edition retired in 2025** and was replaced by
**Databricks Free Edition** (community.cloud.databricks.com no longer applies
-- see full account + Git setup walkthrough below). Free Edition is
serverless-only, so there's no cluster to create/manage -- notebooks just run.

See **"Full Databricks account + Git setup, step by step"** below for the
detailed walkthrough (account creation, connecting GitHub, creating a Git
folder, uploading data, running the notebook).

---

## 0b. Full Databricks account + Git setup, step by step

### Step 1 -- Create your GitHub repo (if you haven't already)

1. Go to github.com, sign in (or create a free account).
2. Click **New repository**. Name it `wind-turbine-pipeline`, set it
   **Private** (this is a job assessment, not public portfolio work), don't
   initialize with a README (you already have one).
3. Push your local project:
   ```bash
   cd wind-turbine-pipeline
   git init
   git add .
   git commit -m "Wind turbine data pipeline POC"
   git remote add origin https://github.com/<your-username>/wind-turbine-pipeline.git
   git branch -M main
   git push -u origin main
   ```
4. Confirm the files show up on github.com.

### Step 2 -- Create a Databricks Free Edition account

1. Go to the [Databricks Free Edition signup page](https://login.databricks.com/?intent=CE_SIGN_UP)
   (this replaced the old Community Edition, which retired in 2025 -- if you
   land on a page mentioning Community Edition, it will redirect you to Free
   Edition sign-up).
2. Sign up with your email (no credit card, no cloud account needed --
   Free Edition provisions its own workspace for you).
3. Verify your email if prompted, then log in. Databricks creates a
   workspace for you automatically.

Free Edition is **serverless-only**: there's no "create a cluster" step like
older Databricks tutorials describe -- notebooks attach to serverless compute
automatically. One less thing to configure.

### Step 3 -- Connect your GitHub account to Databricks

1. In the Databricks workspace, click your username (top-right corner) >
   **Settings**.
2. Click **Linked accounts** in the left sidebar.
3. Click **Add Git credential**, choose **GitHub** from the dropdown.
4. Easiest path: use the **Databricks GitHub App** (OAuth) rather than a
   personal access token -- click through the GitHub authorization prompt
   ("Authorize Databricks") and it links automatically, no token to manage.
   - If you'd rather use a PAT: on GitHub go to **Settings > Developer
     settings > Personal access tokens > Generate new token**, scope =
     `repo`, copy it, paste into the **Token** field in Databricks along
     with your GitHub email, **Save**.

### Step 4 -- Bring your repo into Databricks as a Git folder

1. In the Databricks workspace sidebar, click **Workspace** > your user
   folder, then the **Create** button > **Git folder** (this is the current
   name for what used to be called "Repos").
2. Paste your GitHub repo URL
   (`https://github.com/<your-username>/wind-turbine-pipeline`), confirm the
   provider auto-detects as GitHub, click **Create**.
3. You'll now see your whole repo -- `src/`, `notebooks/`, `data/`, etc. --
   as a folder inside the Databricks workspace, kept in sync with GitHub.
4. Open `notebooks/wind_turbine_pipeline.py` from inside that folder. Because
   it's a Git folder (not an uploaded standalone notebook), `from
   src.pipeline.ingest import read_raw` etc. import correctly without any
   packaging step.

### Step 5 -- Get your data into the workspace

Simplest option for a POC: use a Unity Catalog **Volume**.

1. Left sidebar > **Catalog** > pick (or create) a catalog/schema > **Create**
   > **Volume**. Give it a name, e.g. `wind_turbine_data`.
2. Open the volume, **Upload to this volume**, and add the three CSVs from
   `data/` in your repo (drag-and-drop from your computer).
3. Note the path shown, something like
   `/Volumes/<catalog>/<schema>/wind_turbine_data/`.
4. Back in the notebook, set `DATA_DIR` to that path.

### Step 6 -- Run the notebook

1. With the Git folder's notebook open, click **Run all** at the top.
2. Since Free Edition is serverless, Databricks attaches compute
   automatically -- no cluster picker needed.
3. Watch each cell's output; `display(...)` calls render as interactive
   tables you can sort/filter in the UI.
4. Once `write_table(...)` runs, check **Catalog** in the sidebar to see your
   new Delta tables (`turbine_readings_clean`, `turbine_summary_stats`,
   `turbine_anomalies`), or run the `%sql` cell at the bottom to query them
   directly.

### Step 7 -- Push changes back to GitHub as you iterate

Editing inside a Git folder is a real Git working copy:

1. Make your edits (e.g., filling in `clean.py`) either directly in the
   Databricks file editor, or locally and let the Git folder pull your pushes.
2. In the Git folder, click the **Git** icon (branch icon) in the top bar to
   see changes, write a commit message, **Commit & Push**.
3. This is the same repo you already share with the recruiter -- pushing
   from Databricks updates the exact link you send them.

---

## 1. Suggested build order

1. Get `read_raw` working and confirm the schema looks right
   (`raw_df.printSchema()`, `raw_df.show()`).
2. Run `scripts/make_dirty_data.py` so you have real nulls/outliers/dupes to
   work against, not the clean sample data.
3. Implement `clean.py` against the dirty data. Write `tests/test_clean.py`
   assertions as you go -- they'll tell you fast if your logic does what you
   think.
4. Implement `stats.py`. Sanity-check by hand: pick one turbine, one day,
   eyeball the raw values, confirm min/max/avg match.
5. Implement `anomalies.py`. Test it by deliberately engineering an
   obviously-anomalous turbine in a small synthetic DataFrame (like the
   `make_dirty_data.py` outlier injection, but at the aggregate level) and
   confirming it gets flagged, and a normal one doesn't.
6. Wire it all up via `run.py` / the notebook, run end-to-end.
7. Fill in `README.md` (design + assumptions) -- do this last, once you
   actually know what you built, not before.

## 2. Testing

`pytest tests/ -v` runs everything. The provided `conftest.py` gives you a
session-scoped local `SparkSession` fixture (`spark`) that any test function
can take as an argument -- see `test_clean.py` for the pattern. Add cases as
you implement each module; a couple of well-chosen edge cases (a null, an
extreme outlier, a duplicate row, an empty group) demonstrate "testable"
much better than one big happy-path test.

## 3. Submitting: GitHub repo

The recruiter asked for a link to a GitHub repo within 1 day.

```bash
cd wind-turbine-pipeline
git init
git add .
git commit -m "Wind turbine data pipeline POC"
gh repo create wind-turbine-pipeline --private --source=. --push
# no gh CLI? create an empty repo on github.com first, then:
#   git remote add origin <your-repo-url>
#   git branch -M main
#   git push -u origin main
```

Double-check before sending the link:
- `README.md` is filled in (design + assumptions -- explicitly requested).
- `clean.py` / `stats.py` / `anomalies.py` have no leftover
  `NotImplementedError`.
- `pytest tests/ -v` passes.
- The notebook runs top-to-bottom on a fresh Databricks cluster (Run All),
  not just cell-by-cell in whatever order you happened to edit it.
- The `data/` folder either has the original sample CSVs, or you note in
  README how to point the pipeline at data.

## 4. For the follow-up interview

Re-read the "Productionising" section you filled in in `README.md` before
the call -- that's the section they explicitly said you'll be talking
through. Also be ready to explain, in your own words:
- Why you made each call in "Things to decide" (clean.py / stats.py /
  anomalies.py docstrings) the way you did.
- What you'd change if data volume were 10,000 turbines instead of 15, or
  readings came in every second instead of hourly.
- What's actually missing from this POC for production (monitoring,
  retries, schema evolution, backfills, access control on the Delta
  tables...).
