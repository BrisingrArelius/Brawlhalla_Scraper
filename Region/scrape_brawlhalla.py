#!/usr/bin/env python3
"""
Brawlhalla SEA 1-v-1 scraper  –  resilient version
--------------------------------------------------
• Scrapes BOTH seasonRating & peakRating
• Writes two parallel CSVs, page by page
• Continues until it hits 10 missing/empty pages in a row
• Logs every failed page to failed_pages.txt
• Remembers progress in last_page.txt  →  safe restart with no duplicates
(No plotting – scrape only)
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
PAUSE_S    = 0                    # polite delay
PER_PAGE   = 25                     # players per leaderboard page
STOP_AFTER = 10                     # empty pages in a row → stop
PATH_PEAK  = Path("peak_ratings_sea.csv")
PATH_SEASON = Path("season_ratings_sea.csv")
PATH_FAIL  = Path("failed_pages.txt")
PATH_PROGRESS = Path("last_page.txt")
# ──────────────────────────────────────────────────────────────────────────────


def find_data_array(obj: Any) -> List[Any]:
    """Walk JSON until we hit the long 'data' array that holds rows."""
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


def get_one_page(page: int) -> tuple[List[int], List[int]]:
    """Return (season_ratings, peak_ratings). Empty lists → page missing/empty."""
    url = BASE_URL.format(page=page)
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


def append_rows(path: Path, rows: List[int], header=False):
    with path.open("a", newline="") as f:
        w = csv.writer(f)
        if header:
            w.writerow(["Rating"])
        w.writerows([[r] for r in rows])
        f.flush()


def main():
    # ── figure out where to resume ────────────────────────────────────────────
    if PATH_PROGRESS.exists():
        page = int(PATH_PROGRESS.read_text().strip()) + 1
    else:
        page = 1

    # create headers if starting fresh
    if page == 1 and not PATH_PEAK.exists():
        append_rows(PATH_PEAK,   [], header=True)
        append_rows(PATH_SEASON, [], header=True)

    consecutive_empty = 0
    while consecutive_empty < STOP_AFTER:
        seasons, peaks = get_one_page(page)

        if peaks:
            append_rows(PATH_SEASON, seasons)
            append_rows(PATH_PEAK,   peaks)

            PATH_PROGRESS.write_text(str(page))
            consecutive_empty = 0
            print(f"✅  Page {page} saved ({len(peaks)} rows)")
        else:
            consecutive_empty += 1
            with PATH_FAIL.open("a") as f:
                f.write(f"{page}\n")
            print(f"⚠️   Page {page} empty/missing "
                  f"({consecutive_empty}/{STOP_AFTER} in a row)")

        page += 1
        time.sleep(PAUSE_S)

    print("🛑  Stopped after 10 consecutive empty pages.")


if __name__ == "__main__":
    main()
