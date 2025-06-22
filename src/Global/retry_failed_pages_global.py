#!/usr/bin/env python3
"""retry_failed_pages_global.py – re-scrape pages listed in *failed_pages_global.txt*
===============================================================================

This helper retries pages that failed during the **global** leaderboard scrape.
It must sit in the **same repo** as `scrape_global_regions.py` and share the
same folder layout (date-stamped output directory).

Running the script:
    $ python retry_failed_pages_global.py

Key behaviour
-------------
* Reads `failed_pages_global.txt` (one page number per line).
* Tries to download each page up to **MAX_RETRIES**.
* If a page is recovered:
    – ratings are appended to the correct per-region CSV and the global CSV.
    – the page number is removed from *failed_pages_global.txt*.
* Pages that still fail after all retries are written back so you can try again
  later.

Nothing in the scraper logic is modified – only comments and doc-strings have
been added for clarity and maintainability.
"""

from __future__ import annotations

import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

###############################################################################
# CONFIGURATION CONSTANTS                                                     #
###############################################################################
# Regions that appear on the global leaderboard and that we decide to keep.
# The set is case-sensitive and matches the values returned by the API.
REGIONS: set[str] = {
    "US-E", "EU", "SEA", "BRZ", "AUS", "US-W", "JPN", "SA", "ME",
}

# Top-level output directory – one folder per run, stamped with today’s date.
DATA_DIR = Path(f"Data-{datetime.now():%d%b}")  # e.g. Data-22Jun
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Endpoint template for the Brawlhalla global leaderboard.
BASE_URL = (
    "https://www.brawlhalla.com/rankings/game/all/1v1/{page}"
    "/__data.json?sortBy=rank"
)

# HTTP settings
HEADERS: dict[str, str] = {"User-Agent": "Mozilla/5.0 bh-global-retry"}
TIMEOUT_S: int = 12  # seconds
PAUSE_S: float = 0.5  # polite delay between retries
MAX_RETRIES: int = 3  # attempts per failed page

# Derived paths inside *DATA_DIR*
PATH_GLOBAL_SEASON = DATA_DIR / "season_ratings_global.csv"
PATH_GLOBAL_PEAK = DATA_DIR / "peak_ratings_global.csv"
PATH_FAILED = DATA_DIR / "failed_pages_global.txt"

###############################################################################
# UTILITY FUNCTIONS (identical logic to main scraper, plus extra commentary)  #
###############################################################################

def find_data_array(obj: Any) -> List[Any]:
    """Recursively walk *obj* until the long API "data" list is found.

    The Brawlhalla JSON structure nests the leaderboard rows under objects of
    the form:
        { "type": "data", "data": [...] }

    Args:
        obj: Parsed JSON (dict / list / scalar).

    Returns:
        The first list encountered that matches the structure above, or an
        empty list if not found.
    """
    if isinstance(obj, dict):
        if obj.get("type") == "data" and isinstance(obj.get("data"), list):
            return obj["data"]
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
    """Download *page* and parse rows into a region-keyed mapping.

    Args:
        page: Leaderboard page number (1-based).

    Returns:
        ``{region: [(seasonRating, peakRating), ...], ...}``
        – an empty dict signals that the request failed *or* the page contained
        no usable rows.
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
    if not data:  # malformed or empty page
        return {}

    by_region: Dict[str, List[Tuple[int, int]]] = {}
    i = 0  # cursor over *data*
    while i < len(data):
        item = data[i]
        # Each row starts with a mapping dict containing offsets to the actual
        # values (see main scraper for details).
        if isinstance(item, dict) and {"region", "seasonRating", "peakRating", "rank"} <= item.keys():
            m = item
            start_idx = i + 1  # index of the first scalar after *m*
            try:
                # Offsets from *rank* to the desired value.
                off_reg = m["region"] - m["rank"]
                off_seas = m["seasonRating"] - m["rank"]
                off_peak = m["peakRating"] - m["rank"]

                region = data[start_idx + off_reg]
                season = data[start_idx + off_seas]
                peak = data[start_idx + off_peak]

                if (
                    isinstance(region, str)
                    and region in REGIONS
                    and isinstance(season, int)
                    and isinstance(peak, int)
                ):
                    by_region.setdefault(region, []).append((season, peak))
            except IndexError:  # corrupt row – ignore
                pass
            i = start_idx  # jump past the scalar block we just processed
        else:
            i += 1  # continue scanning
    return by_region


def ensure_header(path: Path) -> None:
    """Create *path* with a single ``Rating`` header if it doesn’t exist."""
    if not path.exists():
        with path.open("w", newline="") as f:
            csv.writer(f).writerow(["Rating"])


def append_rows(path: Path, rows: List[int]) -> None:
    """Append each *value* in *rows* as its own line to *path*."""
    with path.open("a", newline="") as f:
        csv.writer(f).writerows([[v] for v in rows])

###############################################################################
# MAIN ROUTINE                                                                 #
###############################################################################

def main() -> None:  # noqa: C901  (complexity okay for a small script)
    """Retry all pages listed in *failed_pages_global.txt*.

    The function is intentionally imperative and linear for clarity – it mirrors
    the workflow of the main scraper so you can read them side-by-side.
    """

    # ── 1. Load the list of pages that previously failed ────────────────────
    if not PATH_FAILED.exists():
        print("failed_pages_global.txt not found – nothing to do.")
        return

    pages = sorted(
        {
            int(line.strip())
            for line in PATH_FAILED.read_text().splitlines()
            if line.strip().isdigit()
        }
    )
    if not pages:
        print("failed_pages_global.txt is empty – nothing to retry.")
        return

    # ── 2. Ensure all output CSVs at least have their header row ────────────
    ensure_header(PATH_GLOBAL_SEASON)
    ensure_header(PATH_GLOBAL_PEAK)
    for reg in REGIONS:
        ensure_header(DATA_DIR / f"season_ratings_{reg}.csv")
        ensure_header(DATA_DIR / f"peak_ratings_{reg}.csv")

    print(f"🔄  Retrying {len(pages)} failed page(s)…")

    still_failed: List[int] = []  # pages that keep failing this run

    # ── 3. Attempt each failed page up to *MAX_RETRIES* ────────────────────
    for page in pages:
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            rows_by_region = scrape_page(page)
            if rows_by_region:  # recovered!
                # Write recovered ratings to per-region and global CSVs.
                for reg, rows in rows_by_region.items():
                    seasons, peaks = zip(*rows)
                    append_rows(DATA_DIR / f"season_ratings_{reg}.csv", list(seasons))
                    append_rows(DATA_DIR / f"peak_ratings_{reg}.csv", list(peaks))
                    append_rows(PATH_GLOBAL_SEASON, list(seasons))
                    append_rows(PATH_GLOBAL_PEAK, list(peaks))

                total_rows = sum(len(v) for v in rows_by_region.values())
                print(f"✅  Page {page} recovered ({total_rows} rows)")
                success = True
                break  # no further retries needed for this page

            # No data this attempt – wait briefly and try again.
            print(f"⏳  Page {page} attempt {attempt}/{MAX_RETRIES} failed")
            time.sleep(PAUSE_S)

        if not success:
            still_failed.append(page)

    # ── 4. Persist the still-failing pages for the next run ────────────────
    PATH_FAILED.write_text("\n".join(map(str, still_failed)))
    if still_failed:
        print(
            f"⚠️   Could not recover {len(still_failed)} page(s); "
            f"list saved back to {PATH_FAILED}"
        )
    else:
        print("🎉  All failed pages recovered; failed_pages_global.txt emptied.")


if __name__ == "__main__":
    main()
