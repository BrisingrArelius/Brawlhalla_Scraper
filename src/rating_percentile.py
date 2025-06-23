#!/usr/bin/env python3
"""
rating_percentile.py
────────────────────
Find the percentile for one or more Elo ratings in a CSV.

Usage
  python rating_percentile.py Data-22Jun/season_ratings_SEA.csv 1500
  python rating_percentile.py ratings.csv 1400 1600 1800
"""

import argparse
from pathlib import Path

import pandas as pd
import numpy as np

# ── CLI parsing ──────────────────────────────────────────────────────────────
cli = argparse.ArgumentParser(
    description="Report the percentile rank of one or more ratings.")
cli.add_argument("csv_file",
                 help="CSV whose FIRST column contains Elo ratings")
cli.add_argument("ratings", type=float, nargs="+",
                 help="one or more ratings to query")
cli.add_argument("--ascending", action="store_true",
                 help="treat lower numbers as better (rare)")
args = cli.parse_args()

csv_path = Path(args.csv_file).resolve()
if not csv_path.exists():
    cli.error(f"{csv_path} not found")

# ── load ratings ─────────────────────────────────────────────────────────────
data = (
    pd.read_csv(csv_path, usecols=[0])     # only first column
      .squeeze("columns")
      .dropna()
      .astype(float)
      .to_numpy()
)
if data.size == 0:
    cli.error("CSV has no numeric values in its first column")

total = data.size
data_sorted = np.sort(data)                # ascending

# ── percentile look-up helper ────────────────────────────────────────────────
def percentile(rank: float) -> float:
    """
    Return the percentage of observations STRICTLY below *rank*.
    If you want "≤", change 'side' to 'right'.
    """
    # np.searchsorted returns the insertion index that keeps order
    pos = np.searchsorted(data_sorted, rank, side="left")
    return 100.0 * pos / total             # fraction → percent

# ── output ───────────────────────────────────────────────────────────────────
direction = "below" if not args.ascending else "above (better)"
for r in args.ratings:
    pct = percentile(r) if not args.ascending else 100.0 - percentile(r)
    print(f"{pct:5.1f}% of players are {direction} {r:g}")
