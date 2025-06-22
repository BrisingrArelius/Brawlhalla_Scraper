#!/usr/bin/env python3
"""
retry_failed_pages_global.py – re-scrape pages listed in Data-<date>/failed_pages_global.txt
===========================================================================================

Expected files inside the same date-stamped folder as the main global scraper:

    Data-22Jun/
    ├── peak_ratings_global.csv
    ├── season_ratings_global.csv
    ├── peak_ratings_<REGION>.csv      (one per region)
    ├── season_ratings_<REGION>.csv    (one per region)
    └── failed_pages_global.txt        (one page number per line)

For every page in failed_pages_global.txt it will
  • try up to MAX_RETRIES times
  • append recovered rows to the appropriate CSVs
  • remove the page from failed_pages_global.txt on success
  • keep the page in the list if it still fails

Uses the same JSON-decoding logic as the main global scraper.
"""

from __future__ import annotations
import csv, json, time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime

import requests


# ── CONFIG ────────────────────────────────────────────────────────────────────
REGIONS     = {"US-E", "EU", "SEA", "BRZ", "AUS", "US-W", "JPN", "SA", "ME"}
DATA_DIR    = Path(f"Data-{datetime.now():%d%b}")       # e.g. Data-22Jun
DATA_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL    = ("https://www.brawlhalla.com/rankings/game/all/1v1/{page}"
               "/__data.json?sortBy=rank")

HEADERS     = {"User-Agent": "Mozilla/5.0 bh-global-retry"}
TIMEOUT_S   = 12
PAUSE_S     = 0.5              # polite delay between requests
MAX_RETRIES = 3
# ──────────────────────────────────────────────────────────────────────────────

PATH_GLOBAL_SEASON = DATA_DIR / "season_ratings_global.csv"
PATH_GLOBAL_PEAK   = DATA_DIR / "peak_ratings_global.csv"
PATH_FAILED        = DATA_DIR / "failed_pages_global.txt"


# ── helpers copied from the main global scraper ───────────────────────────────
def find_data_array(obj: Any) -> List[Any]:
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
    """Return {region: [(season, peak), …]}. Empty dict → failure / no data."""
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
    i = 0
    while i < len(data):
        item = data[i]
        if isinstance(item, dict) and {"region", "seasonRating",
                                       "peakRating", "rank"} <= item.keys():
            m         = item
            start_idx = i + 1
            try:
                off_reg   = m["region"]       - m["rank"]
                off_seas  = m["seasonRating"] - m["rank"]
                off_peak  = m["peakRating"]   - m["rank"]

                region = data[start_idx + off_reg]
                season = data[start_idx + off_seas]
                peak   = data[start_idx + off_peak]

                if (isinstance(region, str) and region in REGIONS
                        and isinstance(season, int) and isinstance(peak, int)):
                    by_region.setdefault(region, []).append((season, peak))
            except IndexError:
                pass
            i = start_idx
        else:
            i += 1
    return by_region


def ensure_header(path: Path):
    if not path.exists():
        with path.open("w", newline="") as f:
            csv.writer(f).writerow(["Rating"])


def append_rows(path: Path, rows: List[int]):
    with path.open("a", newline="") as f:
        csv.writer(f).writerows([[v] for v in rows])
# -----------------------------------------------------------------------------


def main() -> None:
    if not PATH_FAILED.exists():
        print("failed_pages_global.txt not found – nothing to do.")
        return

    # Load and deduplicate page numbers
    pages = sorted({int(line.strip())
                    for line in PATH_FAILED.read_text().splitlines()
                    if line.strip().isdigit()})
    if not pages:
        print("failed_pages_global.txt is empty – nothing to retry.")
        return

    # Make sure all CSVs have headers
    ensure_header(PATH_GLOBAL_SEASON)
    ensure_header(PATH_GLOBAL_PEAK)
    for reg in REGIONS:
        ensure_header(DATA_DIR / f"season_ratings_{reg}.csv")
        ensure_header(DATA_DIR / f"peak_ratings_{reg}.csv")

    print(f"🔄  Retrying {len(pages)} failed page(s)…")
    still_failed: List[int] = []

    for page in pages:
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            rows_by_region = scrape_page(page)
            if rows_by_region:
                # write recovered rows
                for reg, rows in rows_by_region.items():
                    seasons, peaks = zip(*rows)
                    append_rows(DATA_DIR / f"season_ratings_{reg}.csv", list(seasons))
                    append_rows(DATA_DIR / f"peak_ratings_{reg}.csv",   list(peaks))
                    append_rows(PATH_GLOBAL_SEASON, list(seasons))
                    append_rows(PATH_GLOBAL_PEAK,   list(peaks))
                print(f"✅  Page {page} recovered ({sum(len(v) for v in rows_by_region.values())} rows)")
                success = True
                break
            else:
                print(f"⏳  Page {page} attempt {attempt}/{MAX_RETRIES} failed")
                time.sleep(PAUSE_S)

        if not success:
            still_failed.append(page)

    # Write back the pages that still failed
    PATH_FAILED.write_text("\n".join(map(str, still_failed)))
    if still_failed:
        print(f"⚠️   Could not recover {len(still_failed)} page(s); "
              f"list saved back to {PATH_FAILED}")
    else:
        print("🎉  All failed pages recovered; failed_pages_global.txt emptied.")


if __name__ == "__main__":
    main()
