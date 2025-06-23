#!/usr/bin/env python3
"""
rating_tables.py
────────────────
Print two tables derived from a Brawlhalla ratings CSV:

1️⃣ Tier → Percentile (Tin, Bronze, Silver, Gold, Plat, Diamond)
2️⃣ Percentile → Rating  (Top 0.01 %, 0.1 %, 1 %, 10 %, 50 %)

Only the first column of the CSV is read; everything else is ignored.
"""

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from textwrap import dedent


# ── CLI ──────────────────────────────────────────────────────────────────────
cli = argparse.ArgumentParser(
    description="Show tier-percentile and percentile-rating tables.")
cli.add_argument("csv_file",
                 help="CSV whose FIRST column contains numeric ratings")
args = cli.parse_args()

csv_path = Path(args.csv_file).resolve()
if not csv_path.exists():
    cli.error(f"{csv_path} not found")


# ── Load ratings ────────────────────────────────────────────────────────────
ratings = (
    pd.read_csv(csv_path, usecols=[0])
      .squeeze("columns")
      .dropna()
      .astype(float)
      .to_numpy()
)
if ratings.size == 0:
    cli.error("CSV contains no numeric data in its first column")

ratings.sort()                  # ascending for percentile math
N = ratings.size


# ── Helpers ─────────────────────────────────────────────────────────────────
def pct_below(value: float, *, strictly: bool = True) -> float:
    """
    Percentage of observations < value (if strictly) or ≤ value.
    """
    side = "left" if strictly else "right"
    rank = np.searchsorted(ratings, value, side=side)
    return 100.0 * rank / N


def rating_for_top(top_pct: float) -> float:
    """
    Return the *minimum* rating required to be within the top `top_pct` percent.

    Example: top_pct=1 → rating at the 99th percentile (≥ that puts you in top 1 %).
    """
    # position whose fraction below is 100 - top_pct
    q = 1.0 - top_pct / 100.0          # quantile ∈ [0,1]
    index = int(np.ceil(q * N)) - 1    # convert to 0-based index
    index = max(0, min(index, N - 1))  # clamp
    return ratings[index]


# ── 1️⃣ Tier → Percentile table ─────────────────────────────────────────────
tiers = [
    ("Tin",      0),     # 0-910; nothing to beat
    ("Bronze",  910),
    ("Silver", 1130),
    ("Gold",   1390),
    ("Plat",   1680),
    ("Diamond",2000),
]

tier_percentiles = []
for name, threshold in tiers:
    below = pct_below(threshold)            # how many players below threshold
    top   = 100.0 - below                   # % you must beat
    tier_percentiles.append(top)

# ── 2️⃣ Percentile → Rating table ───────────────────────────────────────────
top_brackets = [0.01, 0.1, 1, 10, 50]       # in %
bracket_ratings = [rating_for_top(p) for p in top_brackets]


# ── Display ─────────────────────────────────────────────────────────────────
def fmt_pct(p: float) -> str:
    return f"{p:0.2f} %" if p < 1 else f"{p:0.1f} %"

# Table 1
row1 = " | ".join(f"{t[0]:^8}" for t in tiers)
row2 = " | ".join(f"{fmt_pct(p):^8}" for p in tier_percentiles)
print(dedent(f"""\
    Tier → Percentile
    ─────────────────
    {row1}
    {row2}
    """))

# Table 2
row1 = " | ".join(f"Top {p:g}%".rjust(8) for p in top_brackets)
row2 = " | ".join(f"{r:8.0f}" for r in bracket_ratings)
print(dedent(f"""\
    Percentile → Rating
    ───────────────────
    {row1}
    {row2}
    """))
