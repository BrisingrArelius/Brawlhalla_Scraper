#!/usr/bin/env python3
"""
plot_csv_hist.py
─────────────
Plot a colored Elo histogram.

Usage examples
    python plot_csv_hist.py peak_ratings_SEA.csv
    python plot_csv_hist.py season_ratings_global.csv --bin 50
"""

# ── editable defaults ────────────────────────────────────────────────────────
CSV_FILE    = None     # if None, supply on CLI
BIN_WIDTH   = 25       # width of each bar
SHOW_COUNTS = False     # ⇦ set False to hide numbers above bars
# ─────────────────────────────────────────────────────────────────────────────

import argparse, math, locale
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

locale.setlocale(locale.LC_ALL, "")          # for thousands separator


def elo_colour(right: float) -> str:
    if right <=  910:  return "#27ae60"      # green
    if right <= 1130:  return "#8e5a2a"      # brown
    if right <= 1390:  return "#bfbfbf"      # silver
    if right <= 1680:  return "#f1c40f"      # gold
    if right <= 2000:  return "#3498db"      # electric-blue
    return "#9b59b6"                         # purple


# ── optional CLI overrides ───────────────────────────────────────────────────
cli = argparse.ArgumentParser(add_help=False)
cli.add_argument("csv_file", nargs="?", default=CSV_FILE)
cli.add_argument("--bin",    type=int, default=BIN_WIDTH)
cli.add_argument("--counts", action="store_true",
                 help="force show counts even if SHOW_COUNTS False")
args, _ = cli.parse_known_args()
if args.csv_file is None:
    cli.error("CSV filename required (or set CSV_FILE variable)")

csv_path  = Path(args.csv_file).resolve()
BIN_WIDTH = args.bin
SHOW_CNT  = SHOW_COUNTS or args.counts
if not csv_path.exists():
    cli.error(f"{csv_path} not found")

# ── load first column numbers ────────────────────────────────────────────────
data = (
    pd.read_csv(csv_path)
      .iloc[:, 0]
      .dropna()
      .astype(float)
      .tolist()
)
if not data:
    cli.error("No numeric data in first column")

# ── compute bins ─────────────────────────────────────────────────────────────
bw  = BIN_WIDTH
lo  = math.floor(min(data)/bw)*bw
hi  = math.ceil(max(data) /bw)*bw + bw
bins = range(lo, hi + bw, bw)

# ── plotting look & feel ─────────────────────────────────────────────────────
plt.rcParams.update({
    "font.size": 9,
    "axes.titlesize": 11,
    "axes.labelsize": 10,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
})
fig, ax = plt.subplots(figsize=(8, 4.5), dpi=120)

counts, edges, patches = ax.hist(data, bins=bins,
                                 edgecolor="black", linewidth=0.6)

# colouring + (optional) count labels
for patch, left, count in zip(patches, edges[:-1], counts):
    right = left + bw
    patch.set_facecolor(elo_colour(right))
    if SHOW_CNT and count:
        ax.text(left + bw/2, count + max(counts)*0.01,
                locale.format_string("%d", int(count), grouping=True),
                ha="center", va="bottom", fontsize=8)

# x-axis tick labels every 100 Elo
LABEL_STEP = 100
label_lefts = range(math.floor(lo/LABEL_STEP)*LABEL_STEP, hi, LABEL_STEP)
tick_pos = [l + LABEL_STEP/2 for l in label_lefts]
ax.set_xticks(tick_pos)
ax.set_xticklabels([f"{l}-{l+LABEL_STEP}" for l in label_lefts],
                   rotation=90, va="top")

ax.yaxis.set_major_formatter(
    FuncFormatter(lambda x, _: locale.format_string("%d", int(x), grouping=True)))
ax.set_xlabel(csv_path.stem)
ax.set_ylabel("Number of Players")
ax.set_title(f"Histogram of {csv_path.stem}")
ax.grid(axis="y", alpha=0.25, linestyle="--")

fig.tight_layout()
out_png = csv_path.with_suffix(".png")
fig.savefig(out_png)
plt.show()
print(f"Saved → {out_png}")
