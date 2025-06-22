#!/usr/bin/env python3
"""
Scrape the *global* 1‑v‑1 leaderboard from Brawlhalla’s public rankings API and
split the rows by region, re‑using the same on‑disk layout produced by the
regional scraper.

Folder structure created on **first run** (example taken 22 Jun)
────────────────────────────────────────────────────────────
Data-22Jun/
├── season_ratings_US-E.csv
├── peak_ratings_US-E.csv
├── … one pair per region …
├── season_ratings_global.csv
├── peak_ratings_global.csv
├── last_page_global.txt       ← progress checkpoint (allows resume)
└── failed_pages_global.txt    ← pages that still returned **no** data

The grabber proceeds page‑by‑page until it meets ``STOP_AFTER`` consecutive
empty pages (defaults to 10).  Progress and failures are persisted so the script
can be re‑run safely without starting over or re‑hitting pages that were empty
last time.
"""

from __future__ import annotations

# ── Standard library ─────────────────────────────────────────────────────────
import csv
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime

# ── 3rd‑party ────────────────────────────────────────────────────────────────
import requests

# ─────────────────────────────────────────────────────────────────────────────
# ████ USER‑CONFIGURABLE CONSTANTS ████████████████████████████████████████████
#   *Change these if you need different behaviour – they are **not** touched
#    elsewhere in the file.*
# ----------------------------------------------------------------------------
REGIONS: set[str] = {
    "US-E", "EU", "SEA", "BRZ", "AUS", "US-W", "JPN", "SA", "ME"
}
"""Two‑letter‑plus region identifiers recognised by Brawlhalla."""

BASE_URL: str = (
    "https://www.brawlhalla.com/rankings/game/all/1v1/{page}/__data.json?sortBy=rank"
)
"""Endpoint template.  ``{page}`` is replaced with the zero‑based page index."""

HEADERS: dict[str, str] = {"User-Agent": "Mozilla/5.0 bh-global-scraper"}
"""Static HTTP headers.  Only UA is needed to avoid basic bot blocks."""

TIMEOUT_S: int = 12          # seconds – wait for server before giving up
PAUSE_S:   float = 0         # polite delay between successive requests
STOP_AFTER: int = 10         # hard stop after N empty pages in a row

# ─────────────────────────────────────────────────────────────────────────────
# ████ FOLDER/FILE PATH SETUP  ████████████████████████████████████████████████
#   These are auto‑created; there is no need to pre‑populate anything.
# ----------------------------------------------------------------------------
DATA_DIR: Path = Path(f"Data-{datetime.now():%d%b}")  # e.g. Data-22Jun
DATA_DIR.mkdir(parents=True, exist_ok=True)

PATH_PROGRESS      = DATA_DIR / "last_page_global.txt"
PATH_FAILED        = DATA_DIR / "failed_pages_global.txt"
PATH_GLOBAL_SEASON = DATA_DIR / "season_ratings_global.csv"
PATH_GLOBAL_PEAK   = DATA_DIR / "peak_ratings_global.csv"

# ─────────────────────────────────────────────────────────────────────────────
# ████ Implementation  ███████████████████████████████████████████████████████
# ----------------------------------------------------------------------------

def find_data_array(obj: Any) -> List[Any]:
    """Depth‑first search for the first element whose ``type`` is ``"data"``.

    The JSON returned by ``/__data.json`` is a mess of deeply nested dicts /
    lists created by Astro (vue).  Somewhere inside lives an object like::

        {"type": "data", "data": [...]}

    This helper walks the structure recursively and returns the contained list
    so downstream code can treat it as a flat array of cells.

    Args:
        obj: Arbitrary Python structure decoded from JSON.

    Returns:
        The *first* list value associated with a mapping where ``type == 'data'``
        or an empty list if none found.
    """
    if isinstance(obj, dict):
        if obj.get("type") == "data" and isinstance(obj.get("data"), list):
            return obj["data"]  # found it – bail out early
        # otherwise inspect children
        for v in obj.values():
            res = find_data_array(v)
            if res:
                return res
    elif isinstance(obj, list):
        for item in obj:
            res = find_data_array(item)
            if res:
                return res
    return []


def scrape_page(page: int) -> Dict[str, List[Tuple[int, int]]]:
    """Scrape a *single* leaderboard page and bucket rows by region.

    The table cells come back as *four* parallel columns, but their order is
    **dynamic** – instead of sending semantic keys the API returns an offset for
    every row:

    ``offset_to_region = cell["region"] - cell["rank"]``

    The true value for *region*, *seasonRating* and *peakRating* are therefore
    located *after* the header row, offset by the difference from the current
    rank.  This function reconstructs each triple `(region, season, peak)` and
    appends them into a mapping keyed by region.

    Args:
        page: Zero‑based page number to fetch.

    Returns:
        Mapping ``{region: [(seasonRating, peakRating), …]}``.  If the request
        fails or the page is genuinely empty the mapping is empty.
    """
    url = BASE_URL.format(page=page)
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
    except requests.exceptions.RequestException:
        return {}

    if r.status_code != 200:
        return {}

    root = json.loads(r.text)
    data = find_data_array(root)
    if not data:
        return {}

    by_region: Dict[str, List[Tuple[int, int]]] = {}

    # Walk the flattened table.  When we hit a row that contains the magic
    # fields we compute where the companion cells live (see docstring above).
    i = 0
    while i < len(data):
        item = data[i]
        # The set‑comparison trick ensures *all* required keys exist.
        if isinstance(item, dict) and {"region", "seasonRating", "peakRating", "rank"} <= item.keys():
            m = item
            start_idx = i + 1      # first real cell lives right after header
            try:
                off_reg  = m["region"]       - m["rank"]
                off_seas = m["seasonRating"] - m["rank"]
                off_peak = m["peakRating"]   - m["rank"]

                region = data[start_idx + off_reg]
                season = data[start_idx + off_seas]
                peak   = data[start_idx + off_peak]

                if (
                    isinstance(region, str) and region in REGIONS and
                    isinstance(season, int) and isinstance(peak, int)
                ):
                    by_region.setdefault(region, []).append((season, peak))
            except IndexError:
                # Defensive – offsets sometimes exceed list bounds when the API
                # slips new columns in‑between builds; ignore and continue.
                pass
            i = start_idx          # jump straight to the row after header block
        else:
            i += 1

    return by_region


def ensure_header(path: Path) -> None:
    """Create a new CSV file with a single header row *iff* it does not exist."""
    if not path.exists():
        with path.open("w", newline="") as f:
            csv.writer(f).writerow(["Rating"])


def append_rows(path: Path, values: List[int]) -> None:
    """Append one value per line to *path*.

    Args:
        path: Destination CSV file.
        values: Sequence of integers to write.
    """
    with path.open("a", newline="") as f:
        csv.writer(f).writerows([[v] for v in values])
        # flush immediately so progress is not lost on Ctrl‑C
        f.flush()


def main() -> None:
    """Entry‑point – orchestrates scraping loop and persistence.

    Resumes from the last successful page (if a checkpoint exists) and bails out
    after ``STOP_AFTER`` empty pages.  Progress and error information are
    printed to *stdout* so that long‑running jobs inside tmux / CI logs are
    easy to follow.
    """
    # Where to resume from (default page 1 on fresh run)
    page: int = int(PATH_PROGRESS.read_text().strip()) + 1 if PATH_PROGRESS.exists() else 1

    # Ensure *all* target CSVs have a header – avoids branching later.
    ensure_header(PATH_GLOBAL_SEASON)
    ensure_header(PATH_GLOBAL_PEAK)
    for reg in REGIONS:
        ensure_header(DATA_DIR / f"season_ratings_{reg}.csv")
        ensure_header(DATA_DIR / f"peak_ratings_{reg}.csv")

    consec_empty = 0  # number of back‑to‑back pages with zero usable rows
    while consec_empty < STOP_AFTER:
        rows_by_region = scrape_page(page)

        if rows_by_region:
            consec_empty = 0  # reset streak on success

            for reg, rows in rows_by_region.items():
                seasons, peaks = zip(*rows)
                append_rows(DATA_DIR / f"season_ratings_{reg}.csv", list(seasons))
                append_rows(DATA_DIR / f"peak_ratings_{reg}.csv",   list(peaks))
                append_rows(PATH_GLOBAL_SEASON, list(seasons))
                append_rows(PATH_GLOBAL_PEAK,   list(peaks))

            PATH_PROGRESS.write_text(str(page))
            total = sum(len(v) for v in rows_by_region.values())
            print(f"✅  Page {page} saved ({total} rows)")
        else:
            consec_empty += 1
            with PATH_FAILED.open("a") as f:
                f.write(f"{page}\n")
            print(
                f"⚠️   Page {page} empty/missing "
                f"({consec_empty}/{STOP_AFTER} in a row)"
            )

        page += 1
        time.sleep(PAUSE_S)

    print("🛑  Stopped after 10 consecutive empty pages.")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
