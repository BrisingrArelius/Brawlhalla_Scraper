#!/usr/bin/env python3
"""
Scrape the *global* 1-v-1 leaderboard and split rows by region.

Files produced
──────────────
  season_ratings_<REGION>.csv   – one seasonRating per line
  peak_ratings_<REGION>.csv     – one peakRating   per line
  season_ratings_global.csv
  peak_ratings_global.csv
  last_page_global.txt          – progress checkpoint
  failed_pages_global.txt       – pages that still return no data

Regions we keep (case–sensitive):
    US-E  EU  SEA  BRZ  AUS  US-W  JPN  SA  ME
"""

from __future__ import annotations
import csv, json, time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests


# ── USER CONFIG ──────────────────────────────────────────────────────────────
REGIONS = {"US-E", "EU", "SEA", "BRZ", "AUS", "US-W", "JPN", "SA", "ME"}
BASE_URL = ("https://www.brawlhalla.com/rankings/game/all/1v1/{page}"
            "/__data.json?sortBy=rank")
HEADERS = {"User-Agent": "Mozilla/5.0 bh-global-scraper"}
TIMEOUT_S = 12
PAUSE_S = 0          # polite delay between requests
PER_PAGE = 25          # expected rows per leaderboard page
STOP_AFTER = 10        # consecutive empty pages → stop
# ──────────────────────────────────────────────────────────────────────────────

# file paths
DIR = Path(".")
PATH_PROGRESS = DIR / "last_page_global.txt"
PATH_FAILED   = DIR / "failed_pages_global.txt"
PATH_GLOBAL_SEASON = DIR / "season_ratings_global.csv"
PATH_GLOBAL_PEAK   = DIR / "peak_ratings_global.csv"


# ── helper: locate the big "data" array in the JSON blob ─────────────────────
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


# ── scrape one page → {region: [(season, peak), …]} ──────────────────────────
def scrape_page(page: int) -> Dict[str, List[Tuple[int, int]]]:
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
        # mapping dict must have all three keys + rank baseline
        if isinstance(item, dict) and {"region", "seasonRating",
                                       "peakRating", "rank"} <= item.keys():
            m         = item
            start_idx = i + 1
            try:
                # offset from rank-value to desired value
                off_reg   = m["region"]        - m["rank"]
                off_seas  = m["seasonRating"]  - m["rank"]
                off_peak  = m["peakRating"]    - m["rank"]
                region    = data[start_idx + off_reg]
                season    = data[start_idx + off_seas]
                peak      = data[start_idx + off_peak]
                if (isinstance(region, str) and region in REGIONS
                        and isinstance(season, int) and isinstance(peak, int)):
                    by_region.setdefault(region, []).append((season, peak))
            except IndexError:
                pass
            i = start_idx
        else:
            i += 1
    return by_region


# ── file helpers ──────────────────────────────────────────────────────────────
def append_rows(path: Path, values: List[int]):
    with path.open("a", newline="") as f:
        csv.writer(f).writerows([[v] for v in values])
        f.flush()


def ensure_header(path: Path):
    if not path.exists():
        with path.open("w", newline="") as f:
            csv.writer(f).writerow(["Rating"])


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    # start / resume page
    if PATH_PROGRESS.exists():
        page = int(PATH_PROGRESS.read_text().strip()) + 1
    else:
        page = 1

    # create headers upfront
    ensure_header(PATH_GLOBAL_SEASON)
    ensure_header(PATH_GLOBAL_PEAK)
    for reg in REGIONS:
        ensure_header(DIR / f"season_ratings_{reg}.csv")
        ensure_header(DIR / f"peak_ratings_{reg}.csv")

    consec_empty = 0
    while consec_empty < STOP_AFTER:
        rows_by_region = scrape_page(page)

        if rows_by_region:
            consec_empty = 0
            # write each region + global
            for reg, rows in rows_by_region.items():
                seasons, peaks = zip(*rows)
                append_rows(DIR / f"season_ratings_{reg}.csv", list(seasons))
                append_rows(DIR / f"peak_ratings_{reg}.csv",   list(peaks))
                append_rows(PATH_GLOBAL_SEASON, list(seasons))
                append_rows(PATH_GLOBAL_PEAK,   list(peaks))

            PATH_PROGRESS.write_text(str(page))
            total_rows = sum(len(v) for v in rows_by_region.values())
            print(f"✅  Page {page} saved ({total_rows} rows)")
        else:
            consec_empty += 1
            with PATH_FAILED.open("a") as f:
                f.write(f"{page}\n")
            print(f"⚠️   Page {page} empty/missing "
                  f"({consec_empty}/{STOP_AFTER} in a row)")

        page += 1
        time.sleep(PAUSE_S)

    print("🛑  Stopped after 10 consecutive empty pages.")


if __name__ == "__main__":
    main()
