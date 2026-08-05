"""
Utility: produce a "dirty" copy of the sample data with injected nulls,
outliers, duplicates and missing timestamp gaps -- so you have something
real to test your clean.py / anomalies.py logic against (the provided
sample data is actually clean, as the brief hints: "feel free to add/remove
data ... in order to test/satisfy the requirements").

Usage:
    python scripts/make_dirty_data.py
    # writes data/dirty/data_group_*.csv

This script is test-data tooling, not part of the pipeline / assessment
logic itself.
"""

import numpy as np
import pandas as pd
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent.parent / "data"
DST_DIR = SRC_DIR / "dirty"
DST_DIR.mkdir(exist_ok=True)

rng = np.random.default_rng(42)

for csv_path in sorted(SRC_DIR.glob("data_group_*.csv")):
    df = pd.read_csv(csv_path)
    n = len(df)

    # 1. Null out ~2% of power_output readings (sensor glitch, value present
    #    but unreadable)
    null_idx = rng.choice(n, size=max(1, n // 50), replace=False)
    df.loc[null_idx, "power_output"] = np.nan

    # 2. Inject a handful of extreme outliers (e.g. sensor fault reporting
    #    an impossible spike)
    outlier_idx = rng.choice(n, size=5, replace=False)
    df.loc[outlier_idx, "power_output"] = rng.uniform(50, 100, size=5)

    # 3. Duplicate a few rows (same turbine_id + timestamp reported twice)
    dup_rows = df.sample(n=5, random_state=1)
    df = pd.concat([df, dup_rows], ignore_index=True)

    # 4. Drop a handful of rows entirely (missing sensor entries -> gaps in
    #    the timestamp sequence for a turbine)
    drop_idx = rng.choice(df.index, size=max(1, n // 100), replace=False)
    df = df.drop(index=drop_idx)

    out_path = DST_DIR / csv_path.name
    df.to_csv(out_path, index=False)
    print(f"wrote {out_path} ({len(df)} rows, was {n})")
