#!/usr/bin/env python3
"""
sort_ratings_csv.py
───────────────────
Sort one CSV – or every CSV in a folder – by the values in *column 0*.

By default it writes the result to a sibling folder named
    Sorted-<input-folder-name>/
or, for single-file mode, to <original_stem>_sorted.csv

Usage examples
──────────────
# sort every CSV in Data-22Jun/ (highest rating first)
python sort_ratings_csv.py Data-22Jun

# sort one file ascending (lower numbers first) and overwrite in place
python sort_ratings_csv.py Data-22Jun/season_ratings_SEA.csv --asc --inplace
"""

import argparse
from pathlib import Path

import pandas as pd


def sort_one(csv_path: Path, out_path: Path, ascending: bool, inplace: bool) -> None:
    """Sort *csv_path* on column-0 and write to *out_path* (or overwrite if inplace)."""
    df = pd.read_csv(csv_path)
    if df.empty:
        print(f"[skip] {csv_path.name}: empty file")
        return

    # Numerically sort the first column; keep other columns with their rows
    df_sorted = df.sort_values(df.columns[0], ascending=ascending, kind="mergesort")

    target = csv_path if inplace else out_path
    df_sorted.to_csv(target, index=False)
    print(f"Saved → {target.relative_to(target.parent.parent) if not inplace else target}")


# ── CLI parsing ──────────────────────────────────────────────────────────────
cli = argparse.ArgumentParser(
    description="Sort ratings CSV(s) by column-0 numeric value.")
cli.add_argument("path",
                 help="CSV filename *or* directory containing CSVs")
cli.add_argument("--asc", "--ascending", dest="ascending",
                 action="store_true", help="sort low→high instead of high→low")
cli.add_argument("--inplace", action="store_true",
                 help="overwrite the original file(s)")
args = cli.parse_args()

input_path = Path(args.path).resolve()
ascending   = args.ascending
inplace     = args.inplace

# ── single CSV vs folder ─────────────────────────────────────────────────────
if input_path.is_file() and input_path.suffix.lower() == ".csv":
    # single-file mode
    out_file = input_path.with_stem(input_path.stem + "_sorted")
    sort_one(input_path, out_file, ascending, inplace)

elif input_path.is_dir():
    # folder mode: build output folder next to the input one
    if not inplace:
        out_dir = input_path.parent / f"Sorted-{input_path.name}"
        out_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(input_path.glob("*.csv"))
    if not csv_files:
        cli.error("No *.csv files found in the given directory")

    for csv_file in csv_files:
        out_path = (csv_file if inplace
                    else (out_dir / csv_file.name))
        sort_one(csv_file, out_path, ascending, inplace)

else:
    cli.error("Path must be a .csv file or a directory containing CSVs")
