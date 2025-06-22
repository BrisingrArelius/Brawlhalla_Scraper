# Brawlhalla Leaderboard Scraper

Scrapes Brawlhalla 1‑v‑1 leaderboards and writes ratings to **CSV** files that are easy to analyse or plot later.

* Regional scraper ⇒ `src/Region/scrape_brawlhalla.py`
* Global (multi‑region) scraper ⇒ `src/Global/scrape_global_regions.py`
* Retry helpers ⇒ automatically re‑query pages listed in `failed_pages*.txt`
* `plot_csv_hist.py` ⇒ example Matplotlib histogram (optional)

Each run stores its output in a date‑stamped folder (e.g. `Data-22Jun/`) so nothing is overwritten.

---

## 1️⃣  Quick‑start

```bash
# clone & enter the repo
git clone https://github.com/BrisingrArelius/Brawlhalla_Scraper.git
cd Brawlhalla_Scraper

# create + activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1

# install dependencies (just `requests` for now)
pip install -r requirements.txt
```

That’s all the setup you need.

---

## 2️⃣  Scraping just one region

The **regional** scraper defaults to `SEA`, but you can pick any region listed on the official website.

```bash
# Default (SEA)
python src/Region/scrape_brawlhalla.py

# EU example
python src/Region/scrape_brawlhalla.py --region eu
```

### What you get

```
Data-22Jun/
├── peak_ratings_sea.csv
├── season_ratings_sea.csv
├── failed_pages.txt       ← pages that 404 or time‑out
└── last_page.txt          ← resume checkpoint
```

Running the scraper again resumes automatically and never duplicates rows.

---

## 3️⃣  Scraping the global leaderboard

```bash
python src/Global/scrape_global_regions.py
```

This pulls *all* regions in one go and writes one pair of CSVs **per region** plus a combined "global" pair.

```
Data-22Jun/
├── peak_ratings_global.csv
├── season_ratings_global.csv
├── peak_ratings_EU.csv
├── season_ratings_EU.csv
├── …
├── failed_pages_global.txt
└── last_page_global.txt
```

---

## 4️⃣  Retrying failed pages

If the connection hiccups you can re‑query just the failed pages instead of rerunning the whole scraping job:

```bash
# for a single‑region run
python src/Region/retry_failed_pages.py

# for the global run
python src/Global/retry_failed_pages_global.py
```

Each script tries up to **3** times per page and updates the CSVs in‑place.

---

## 5️⃣  Plotting example

After scraping you can visualise the distribution, e.g.:

```bash
python plot_csv_hist.py Data-22Jun/peak_ratings_sea.csv
```

This is purely optional—feel free to plug the CSVs into Excel, Pandas, or anything else.

---

## 6️⃣  Customisation

| Setting                            | How to change                                      | Default                      |
| ---------------------------------- | -------------------------------------------------- | ---------------------------- |
| **Region** (single‑region scraper) | `--region eu` or edit `REGION` in the config block | `sea`                        |
| Output folder name                 | edit the `DATA_DIR` definition                     | `Data-<today>`               |
| Request timeout                    | `TIMEOUT_S` constant                               | 12 s                         |
| Delay between requests             | `PAUSE_S` constant                                 | 0 s (global) / 0.5 s (retry) |
| Max empty pages before stop        | `STOP_AFTER` constant                              | 10                           |

---

## 7️⃣  Troubleshooting

* \`\` – install Python ≥3.8 from your package manager.
* **Authentication errors when pushing** – use an SSH remote or PAT; see the Git commands in the conversation above.
* \`\` – you’re running an old script version; update and make sure both `region` *and* `page` placeholders are filled.

---

## 8️⃣  Licence

MIT License – see `LICENSE` for details.

Happy scraping! 🎮
