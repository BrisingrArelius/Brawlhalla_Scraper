#!/usr/bin/env python3
"""
retry_failed_pages.py – Recover pages that previously returned **no data**
=======================================================================

This utility re-scans page numbers listed in ``failed_pages.txt`` that were
recorded by the *regional* leaderboard scraper.  The folder layout expected is
identical to the one produced by the main scraper::

    Data-22Jun/
    ├── peak_ratings_<region>.csv
    ├── season_ratings_<region>.csv
    └── failed_pages.txt          ← one page number per line (0-based)

Operational rules
-----------------
* For each page in *failed_pages.txt*:
    • Attempt the request up to ``MAX_RETRIES`` times.
    • On success: append the ratings to both per-region CSVs **and** delete the
      page from the failure list.
    • On repeated failure: leave the page number in the file for next run.

The JSON decoding logic is a verbatim copy of the one used by the main scraper
so that any API quirks are handled identically.
"""

from __future__ import annotations

# ── Standard library ─────────────────────────────────────────────────────────
import csv
import json
import time
from pathlib import Path
from typing import Any, List
from datetime import datetime

# ── 3rd-party ────────────────────────────────────────────────────────────────
import requests

# ─────────────────────────────────────────────────────────────────────────────
# ████ USER-CONFIGURABLE CONSTANTS ████████████████████████████████████████████
#   Update these if you scrape a different region or want to fine-tune retry
#   behaviour.  The rest of the script *only reads* them.
# ----------------------------------------------------------------------------
REGION: str   = "sea"                                 # e.g. "us-e", "eu", …
DATA_DIR: Path = Path(f"Data-{datetime.now():%d%b}")  # matches main scraper
DATA_DIR.mkdir(parents=True, exist_ok=True)            # ensure dir exists

BASE_URL: str = (
    "https://www.brawlhalla.com/rankings/game/{region}/1v1/{page}"  # noqa: E501
    "/__data.json?sortBy=rank"
)

HEADERS: dict[str, str] = {"User-Agent": "Mozilla/5.0 bh-scraper"}
TIMEOUT_S:   int   = 12       # seconds to wait before aborting a request
PAUSE_S:     float = 0.5      # polite delay between retries
MAX_RETRIES: int   = 3        # attempts per page before giving up
PER_PAGE:    int   = 25       # players per leaderboard page (not used directly)

PATH_PEAK   = DATA_DIR / f"peak_ratings_{REGION}.csv"
PATH_SEASON = DATA_DIR / f"season_ratings_{REGION}.csv"
PATH_FAIL   = DATA_DIR / "failed_pages.txt"

# ─────────────────────────────────────────────────────────────────────────────
# ████ Helper utilities (identical to main scraper) ███████████████████████████
# ----------------------------------------------------------------------------

def find_data_array(obj: Any) -> List[Any]:
    """Locate the *first* object having ``{"type": "data", "data": [...]}``.

    The Brawlhalla rankings endpoint nests the actual table cells within a
    deeply-nested structure emitted by Astro (Vue).  We recursively traverse
    the object, returning the list once found so callers can treat the dataset
    as a flat array.
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


def scrape_page(page: int) -> tuple[List[int], List[int]]:
    """Return *season* and *peak* rating lists for the given page.

    The endpoint does **not** deliver well-structured JSON: rows are split into
    four parallel columns whose absolute positions vary per page.  To recover
    the numeric ratings we exploit the invariant that

        ``offset = cell_value - cell["rank"]``

    for the *header* row, then fetch the actual values located at
    ``start_idx + offset``.

    Args:
        page: Zero-based leaderboard page number.

    Returns:
        A pair ``(season_ratings, peak_ratings)``.  Either list may be empty to
        signal failure or an empty data page.
    """
    url = BASE_URL.format(region=REGION, page=page)
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT_S)
    except requests.exceptions.RequestException:
        return [], []

    if r.status_code != 200:
        return [], []

    root = json.loads(r.text)
    data = find_data_array(root)
    if not data:
        return [], []

    seasons: List[int] = []
    peaks:   List[int] = []

    i = 0
    while i < len(data):
        item = data[i]
        if isinstance(item, dict) and {"peakRating", "seasonRating", "rank"} <= item.keys():
            m = item
            start_idx = i + 1  # actual cells start right after header row
            try:
                peak   = data[start_idx + (m["peakRating"]   - m["rank"])]
                season = data[start_idx + (m["seasonRating"] - m["rank"])]
                if isinstance(peak, int) and isinstance(season, int):
                    peaks.append(peak)
                    seasons.append(season)
            except IndexError:
                # Offsets occasionally point outside list bounds – ignore row.
                pass
            i = start_idx  # skip to next header block
        else:
            i += 1
    return seasons, peaks


def append_rows(path: Path, rows: List[int]) -> None:
    """Append each integer in *rows* as its own CSV line."""
    with path.open("a", newline="") as f:
        csv.writer(f).writerows([[r] for r in rows])

# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    """Retry scraping all page numbers still present in ``failed_pages.txt``.

    The file is read, deduplicated and sorted so progress messages appear in
    ascending order.  After the loop we **overwrite** the file with the list of
    pages that still could not be recovered.
    """
    if not PATH_FAIL.exists():
        print("failed_pages.txt not found – nothing to do.")
        return

    # ── Load & sanitise page list ────────────────────────────────────────────
    pages = sorted({
        int(line.strip())
        for line in PATH_FAIL.read_text().splitlines()
        if line.strip().isdigit()
    })
    if not pages:
        print("failed_pages.txt is empty – nothing to retry.")
        return

    print(f"🔄  Retrying {len(pages)} failed pages…")
    still_failed: List[int] = []  # accumulate leftovers here

    # ── Retry loop ────────────────────────────────────────────────────────────
    for page in pages:
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            seasons, peaks = scrape_page(page)
            if peaks:  # non-empty ⇒ success
                append_rows(PATH_SEASON, seasons)
                append_rows(PATH_PEAK,   peaks)
                print(f"✅  Page {page} recovered ({len(peaks)} rows)")
                success = True
                break
            else:
                print(f"⏳  Page {page} attempt {attempt}/{MAX_RETRIES} failed")
                time.sleep(PAUSE_S)
        if not success:
            still_failed.append(page)

    # ── Persist remaining failures ───────────────────────────────────────────
    PATH_FAIL.write_text("\n".join(map(str, still_failed)))
    if still_failed:
        print(
            f"⚠️   Could not recover {len(still_failed)} pages; "
            f"list saved back to {PATH_FAIL}"
        )
    else:
        print("🎉  All failed pages recovered; failed_pages.txt emptied.")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
