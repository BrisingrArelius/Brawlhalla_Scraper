#!/usr/bin/env python3
"""
retry_failed_pages.py  –  re-scrape pages listed in failed_pages.txt
-------------------------------------------------------------------

• Expects:
      peak_ratings_sea.csv
      season_ratings_sea.csv
      failed_pages.txt      (one page number per line)
• For every page in failed_pages.txt:
      – try up to MAX_RETRIES
      – if data comes back, append to both CSVs and remove from failed list
      – otherwise keep it in the file
• Uses the same JSON-decoding logic as the main scraper.
"""

from __future__ import annotations
import csv, json, time
from pathlib import Path
from typing import Any, List

import requests


# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_URL   = ("https://www.brawlhalla.com/rankings/game/sea/1v1/{page}"
              "/__data.json?sortBy=rank")
HEADERS    = {"User-Agent": "Mozilla/5.0 bh-scraper"}
TIMEOUT_S  = 12
PAUSE_S    = 0.5          # polite delay between requests
MAX_RETRIES = 3
PER_PAGE   = 25           # players per leaderboard page
PATH_PEAK   = Path("peak_ratings_sea.csv")
PATH_SEASON = Path("season_ratings_sea.csv")
PATH_FAIL   = Path("failed_pages.txt")
# ──────────────────────────────────────────────────────────────────────────────


# ---------- helper functions (same as in main scraper) -----------------------
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


def scrape_page(page: int) -> tuple[List[int], List[int]]:
    """Return (seasons, peaks); empty lists mean failure / no data."""
    url = BASE_URL.format(page=page)
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

    seasons, peaks = [], []
    i = 0
    while i < len(data):
        item = data[i]
        if isinstance(item, dict) and {"peakRating", "seasonRating", "rank"} <= item.keys():
            m         = item
            start_idx = i + 1
            try:
                peak   = data[start_idx + (m["peakRating"]   - m["rank"])]
                season = data[start_idx + (m["seasonRating"] - m["rank"])]
                if isinstance(peak, int) and isinstance(season, int):
                    peaks.append(peak)
                    seasons.append(season)
            except IndexError:
                pass
            i = start_idx
        else:
            i += 1
    return seasons, peaks


def append_rows(path: Path, rows: List[int]):
    with path.open("a", newline="") as f:
        csv.writer(f).writerows([[r] for r in rows])
# -----------------------------------------------------------------------------


def main():
    if not PATH_FAIL.exists():
        print("failed_pages.txt not found – nothing to do.")
        return

    # read and deduplicate page list
    pages = sorted({int(line.strip()) for line in PATH_FAIL.read_text().splitlines() if line.strip().isdigit()})
    if not pages:
        print("failed_pages.txt is empty – nothing to retry.")
        return

    print(f"🔄  Retrying {len(pages)} failed pages…")

    still_failed: List[int] = []

    for page in pages:
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            seasons, peaks = scrape_page(page)
            if peaks:
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

    # write back the remaining failures
    PATH_FAIL.write_text("\n".join(map(str, still_failed)))
    if still_failed:
        print(f"⚠️   Could not recover {len(still_failed)} pages; "
              f"list saved back to {PATH_FAIL}")
    else:
        print("🎉  All failed pages recovered; failed_pages.txt emptied.")


if __name__ == "__main__":
    main()
