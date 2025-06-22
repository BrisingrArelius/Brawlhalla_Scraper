#!/usr/bin/env python3
"""
Brawlhalla SEA 1‑v‑1 leaderboard scraper – **resilient edition**
===============================================================

Key features
------------
* Collects **both** ``seasonRating`` and ``peakRating`` for every player on the
  *SEA* 1‑v‑1 ranking.
* Streams data page‑by‑page into two parallel CSV files (season / peak) so that
  partial progress is instantly durable.
* Terminates after ``STOP_AFTER`` consecutive pages return no usable data,
  thereby saving bandwidth when the API stops serving rows beyond the last
  ranked player.
* Persists state:
    * ``last_page.txt`` – lets the script resume exactly where it left off.
    * ``failed_pages.txt`` – pages that responded but contained **no** data.

Directory structure (auto‑created) on first run looks like::

    Data-22Jun/
    ├── season_ratings_sea.csv
    ├── peak_ratings_sea.csv
    ├── last_page.txt
    └── failed_pages.txt

This script **does not** perform any plotting; its sole purpose is reliable
collection of leaderboard snapshots.
"""

from __future__ import annotations

# ── Standard library ─────────────────────────────────────────────────────────
import csv
import json
import time
from pathlib import Path
from typing import Any, List, Tuple
from datetime import datetime

# ── 3rd‑party ────────────────────────────────────────────────────────────────
import requests

# ─────────────────────────────────────────────────────────────────────────────
# ████ USER‑CONFIGURABLE CONSTANTS ████████████████████████████████████████████
#   Adjust these knobs to scrape a different region or to be more/less polite.
# ----------------------------------------------------------------------------
DATA_DIR: Path = Path(f"Data-{datetime.now():%d%b}")  # e.g. Data-22Jun
DATA_DIR.mkdir(parents=True, exist_ok=True)            # ensure dir exists

REGION: str = "sea"  # change to "us-e", "eu", … as needed

BASE_URL: str = (
    "https://www.brawlhalla.com/rankings/game/{region}/1v1/{page}"  # noqa: E501
    "/__data.json?sortBy=rank"
)

HEADERS: dict[str, str] = {"User-Agent": "Mozilla/5.0 bh-scraper"}
TIMEOUT_S:  int   = 12   # seconds before aborting HTTP request
PAUSE_S:    float = 0    # polite delay between pages (seconds)
PER_PAGE:   int   = 25   # leaderboard rows per page (informational)
STOP_AFTER: int   = 10   # break loop after this many empty pages

# ── Persisted file paths ─────────────────────────────────────────────────────
PATH_PEAK:      Path = DATA_DIR / f"peak_ratings_{REGION}.csv"
PATH_SEASON:    Path = DATA_DIR / f"season_ratings_{REGION}.csv"
PATH_FAIL:      Path = DATA_DIR / "failed_pages.txt"
PATH_PROGRESS:  Path = DATA_DIR / "last_page.txt"

# ─────────────────────────────────────────────────────────────────────────────
# ████ Helper functions ███████████████████████████████████████████████████████
# ----------------------------------------------------------------------------

def find_data_array(obj: Any) -> List[Any]:
    """Recursively locate the list that holds table rows in the Astro payload.

    The rankings endpoint returns JSON where the actual data lives inside the
    first object matching ``{"type": "data", "data": [...]}``.  We perform a
    depth‑first traversal until we find it and return the embedded list.
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


def get_one_page(page: int) -> Tuple[List[int], List[int]]:
    """Fetch a leaderboard *page* and extract season/peak ratings.

    The endpoint encodes each header row with *offsets* relative to ``rank``—to
    retrieve the real value one needs ``value = data[start_idx + offset]``.
    We follow the same strategy used by the global scraper.

    Args:
        page: Zero‑based page index to request.

    Returns:
        Two parallel lists ``(season_ratings, peak_ratings)``.  An empty list
        indicates the page was missing, empty, or retrieval failed.
    """
    url = BASE_URL.format(region=REGION, page=page)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
    except requests.exceptions.RequestException:
        return [], []

    if resp.status_code != 200:
        return [], []

    root = json.loads(resp.text)
    data = find_data_array(root)
    if not data:
        return [], []

    seasons: List[int] = []
    peaks:   List[int] = []

    i = 0
    while i < len(data):
        item = data[i]
        if isinstance(item, dict) and {"peakRating", "seasonRating", "rank"} <= item.keys():
            m         = item
            start_idx = i + 1  # first real cell after header block
            try:
                peak   = data[start_idx + (m["peakRating"]   - m["rank"])]
                season = data[start_idx + (m["seasonRating"] - m["rank"])]
                if isinstance(peak, int) and isinstance(season, int):
                    peaks.append(peak)
                    seasons.append(season)
            except IndexError:
                # Offsets occasionally overshoot list bounds; skip gracefully.
                pass
            i = start_idx  # skip straight past this header row
        else:
            i += 1
    return seasons, peaks


def append_rows(path: Path, rows: List[int], header: bool = False) -> None:
    """CSV helper – append each integer in *rows* as its own line.

    Args:
        path:   Destination CSV file.
        rows:   Integers to append.  May be empty (no‑op).
        header: When *True* a one‑time header row ["Rating"] is written first.
    """
    with path.open("a", newline="") as f:
        writer = csv.writer(f)
        if header:
            writer.writerow(["Rating"])
        writer.writerows([[r] for r in rows])
        # Force flush so progress survives abrupt termination
        f.flush()

# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    """Main control loop – fetches pages until the stop condition is hit."""

    # ── Determine resume point ───────────────────────────────────────────────
    page: int
    if PATH_PROGRESS.exists():
        page = int(PATH_PROGRESS.read_text().strip()) + 1
    else:
        page = 1

    # ── Initialise CSVs on first run ─────────────────────────────────────────
    if page == 1 and not PATH_PEAK.exists():
        append_rows(PATH_PEAK,   [], header=True)
        append_rows(PATH_SEASON, [], header=True)

    consecutive_empty = 0  # counter for back‑to‑back empty pages

    while consecutive_empty < STOP_AFTER:
        seasons, peaks = get_one_page(page)

        if peaks:
            # Success – persist data & reset empty counter
            append_rows(PATH_SEASON, seasons)
            append_rows(PATH_PEAK,   peaks)
            PATH_PROGRESS.write_text(str(page))
            consecutive_empty = 0
            print(f"✅  Page {page} saved ({len(peaks)} rows)")
        else:
            consecutive_empty += 1
            with PATH_FAIL.open("a") as f:
                f.write(f"{page}\n")
            print(
                f"⚠️   Page {page} empty/missing "
                f"({consecutive_empty}/{STOP_AFTER} in a row)"
            )

        page += 1
        time.sleep(PAUSE_S)

    print("🛑  Stopped after 10 consecutive empty pages.")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
