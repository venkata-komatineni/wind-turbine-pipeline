# Databricks notebook source
# MAGIC %md
# MAGIC # Run tests
# MAGIC
# MAGIC Runs the `tests/` suite (pytest) against `src/pipeline/*.py` from inside
# MAGIC Databricks. Kept as a separate notebook from
# MAGIC `wind_turbine_pipeline.py` so the main pipeline notebook stays focused
# MAGIC on the actual pipeline run.
# MAGIC

# COMMAND ----------

# MAGIC %pip install pytest

# COMMAND ----------

# MAGIC %md
# MAGIC `%pip install` restarts the Python process on Databricks -- this cell
# MAGIC runs after that restart, so it's a fresh interpreter with pytest now
# MAGIC available.

# COMMAND ----------

import sys, os
import pytest

sys.dont_write_bytecode = True

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# notebook_path looks like: /Users/<you>/wind-turbine-pipeline/notebooks/run_tests
repo_root = "/Workspace" + os.path.dirname(os.path.dirname(notebook_path))

os.chdir(repo_root)
exit_code = pytest.main(["tests/", "-v", "-p", "no:cacheprovider"])
print("pytest exit code:", exit_code, "(0 = all passed)")

# COMMAND ----------

assert exit_code == 0, "Tests failed -- see output above."
print("All tests passed.")