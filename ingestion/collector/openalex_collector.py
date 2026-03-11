"""
OpenAlex Collector
------------------
Self-contained — no project imports needed.
Run from anywhere:
    python openalex_collector.py
"""

import os
import json
import time
import logging
import requests
from datetime import datetime
from typing import Optional


AVAILABLE_CONCEPTS = {
    "1":  {"id": "C41008148",  "name": "computer_science"},
    "2":  {"id": "C154945302", "name": "artificial_intelligence"},
    "3":  {"id": "C119857082", "name": "machine_learning"},
    "4":  {"id": "C121332964", "name": "physics"},
    "5":  {"id": "C33923547",  "name": "mathematics"},
    "6":  {"id": "C86803240",  "name": "biology"},
    "7":  {"id": "C71924100",  "name": "medicine"},
    "8":  {"id": "C185592680", "name": "chemistry"},
    "9":  {"id": "C127413603", "name": "engineering"},
    "10": {"id": "C144024400", "name": "sociology"},
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def reconstruct_abstract(inverted_index: Optional[dict]) -> Optional[str]:
    if not inverted_index:
        return None
    position_word = {}
    for word, positions in inverted_index.items():
        for pos in positions:
            position_word[pos] = word
    if not position_word:
        return None
    return " ".join(position_word[i] for i in sorted(position_word.keys()))


def normalize_work(raw: dict) -> dict:
    authors = [
        a.get("author", {}).get("display_name")
        for a in raw.get("authorships", [])
        if a.get("author", {}).get("display_name")
    ]

    primary_location = raw.get("primary_location") or {}
    source_name = (primary_location.get("source") or {}).get("display_name")

    categories = [t["display_name"] for t in raw.get("topics", []) if t.get("display_name")]
    if not categories:
        categories = [c["display_name"] for c in raw.get("concepts", []) if c.get("display_name")]

    doi = raw.get("doi") or ""
    if doi.startswith("https://doi.org/"):
        doi = doi.replace("https://doi.org/", "")

    ids = raw.get("ids") or {}
    arxiv_url = ids.get("arxiv")
    arxiv_id  = arxiv_url.split("/")[-1] if arxiv_url else None

    return {
        "source":       "openalex",
        "paper_id":     raw.get("id", "").replace("https://openalex.org/", ""),
        "title":        raw.get("title"),
        "abstract":     reconstruct_abstract(raw.get("abstract_inverted_index")),
        "authors":      authors,
        "year":         raw.get("publication_year"),
        "source_name":  source_name,
        "categories":   categories,
        "doi":          doi or None,
        "citation_count": raw.get("cited_by_count"),
        "url":          primary_location.get("landing_page_url"),
        "arxiv_id":     arxiv_id,
        "type":         raw.get("type"),
        "updated_date": raw.get("updated_date"),
    }


def load_checkpoint(path: str, concept_id: str) -> dict:
    if os.path.exists(path):
        with open(path, "r") as f:
            data = json.load(f)
        if data.get("concept_id") == concept_id:
            log.info(f"Resuming '{concept_id}' — {data.get('total_saved', 0):,} records saved so far.")
            return data
    return {
        "concept_id":  concept_id,
        "cursor":      "*",
        "total_saved": 0,
        "last_year":   None,
        "last_month":  None,
        "last_updated": None,
    }


def save_checkpoint(path: str, concept_id: str, cursor: str, total_saved: int,
                    year: int = None, month: int = None):
    with open(path, "w") as f:
        json.dump({
            "concept_id":   concept_id,
            "cursor":       cursor,
            "total_saved":  total_saved,
            "last_year":    year,
            "last_month":   month,
            "last_updated": datetime.utcnow().isoformat(),
        }, f, indent=2)


def fetch_page(session: requests.Session, params: dict, retries: int = 5) -> Optional[dict]:
    url = "https://api.openalex.org/works"
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                wait = 2 ** attempt
                log.warning(f"Rate limited. Waiting {wait}s... (attempt {attempt+1}/{retries})")
                time.sleep(wait)
            elif response.status_code >= 500:
                wait = 2 ** attempt
                log.warning(f"Server error {response.status_code}. Waiting {wait}s...")
                time.sleep(wait)
            else:
                log.error(f"Status {response.status_code}: {response.text[:200]}")
                return None
        except requests.exceptions.RequestException as e:
            wait = 2 ** attempt
            log.warning(f"Request error: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    log.error("Max retries exceeded.")
    return None


def get_count(session: requests.Session, filter_str: str, email: str) -> int:
    params = {
        "filter":   filter_str,
        "per-page": 1,
        "mailto":   email,
        "select":   "id",
    }
    try:
        data = fetch_page(session, params)
        return data.get("meta", {}).get("count", 0) if data else 0
    except Exception:
        return 0


def fetch_all_pages(
    session:        requests.Session,
    filter_str:     str,
    select_fields:  str,
    email:          str,
    request_delay:  float,
    output_path:    str,
    checkpoint_path:str,
    concept_id:     str,
    records:        list,
    total_saved:    int,
    start_cursor:   str = "*",
    year:           int = None,
    month:          int = None,
) -> tuple[list, int]:

    cursor   = start_cursor
    page_num = 0

    base_params = {
        "filter":   filter_str,
        "select":   select_fields,
        "per-page": 200,
        "mailto":   email,
    }

    while True:
        page_num += 1
        params = {**base_params, "cursor": cursor}

        data = fetch_page(session, params)
        if data is None:
            log.error(f"Failed on page {page_num}. Saving progress and stopping.")
            break

        results = data.get("results", [])
        if not results:
            break

        for work in results:
            records.append({
                "raw":        work,
                "normalized": normalize_work(work),
            })

        total_saved     += len(results)
        meta             = data.get("meta", {})
        next_cursor      = meta.get("next_cursor")
        total_available  = meta.get("count", "?")

        log.info(
            f"Page {page_num} — +{len(results)} | "
            f"total so far: {total_saved:,} / {total_available}"
        )

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        save_checkpoint(checkpoint_path, concept_id, next_cursor or "*",
                        total_saved, year, month)

        if next_cursor:
            cursor = next_cursor
        else:
            break

        time.sleep(request_delay)

    return records, total_saved


def crawl_concept(concept: dict, cfg: dict, session: requests.Session) -> int:
    concept_id   = concept["id"]
    concept_name = concept["name"]
    email        = cfg["email"]

    os.makedirs(cfg["output_dir"], exist_ok=True)
    os.makedirs(cfg["log_dir"],    exist_ok=True)

    output_path     = os.path.join(cfg["output_dir"], f"{concept_name}_{cfg['year_from']}_{cfg['year_to']}.json")
    checkpoint_path = os.path.join(cfg["log_dir"], "openalex_progress.json")

    checkpoint  = load_checkpoint(checkpoint_path, concept_id)
    total_saved = checkpoint["total_saved"]

    records = []
    if total_saved > 0 and os.path.exists(output_path):
        log.info(f"Loading {total_saved:,} existing records from disk...")
        with open(output_path, "r", encoding="utf-8") as f:
            records = json.load(f)

    select_fields = ",".join(cfg["select_fields"])

    log.info(f"Concept : '{concept_name}' ({concept_id})")
    log.info(f"Output  : {output_path}")

    for year in range(cfg["year_from"], cfg["year_to"] + 1):

        last_year = checkpoint.get("last_year")
        if last_year and year < last_year:
            log.info(f"Skipping year {year} (already done)")
            continue

        year_filter = (
            f"publication_year:{year},"
            f"concepts.id:{concept_id},"
            f"type:article|preprint"
        )

        count = get_count(session, year_filter, email)
        log.info(f"Year {year}: {count:,} papers found")

        if count == 0:
            continue

        if count <= 9_000:
            log.info(f"Fetching year {year} as single block...")
            start_cursor = (
                checkpoint.get("cursor", "*")
                if checkpoint.get("last_year") == year and not checkpoint.get("last_month")
                else "*"
            )
            records, total_saved = fetch_all_pages(
                session, year_filter, select_fields, email,
                cfg["request_delay"], output_path, checkpoint_path,
                concept_id, records, total_saved,
                start_cursor=start_cursor, year=year, month=None,
            )

        else:
            log.info(f"{count:,} papers > 9000 — slicing by month...")
            for month in range(1, 13):

                last_month = checkpoint.get("last_month")
                if (checkpoint.get("last_year") == year
                        and last_month and month < last_month):
                    log.info(f"Skipping {year}-{month:02d} (already done)")
                    continue

                month_str    = f"{year}-{month:02d}"
                month_filter = (
                    f"from_publication_date:{month_str}-01,"
                    f"to_publication_date:{month_str}-28,"
                    f"concepts.id:{concept_id},"
                    f"type:article|preprint"
                )

                month_count = get_count(session, month_filter, email)
                log.info(f"Month {month_str}: {month_count:,} papers")

                if month_count == 0:
                    continue

                start_cursor = (
                    checkpoint.get("cursor", "*")
                    if (checkpoint.get("last_year") == year
                        and checkpoint.get("last_month") == month)
                    else "*"
                )

                records, total_saved = fetch_all_pages(
                    session, month_filter, select_fields, email,
                    cfg["request_delay"], output_path, checkpoint_path,
                    concept_id, records, total_saved,
                    start_cursor=start_cursor, year=year, month=month,
                )

                time.sleep(cfg["request_delay"])

        log.info(f"Year {year} complete. Total so far: {total_saved:,}")

    log.info(f"Concept '{concept_name}' complete. Total: {total_saved:,} records.")
    save_checkpoint(checkpoint_path, concept_id, "*", total_saved)
    return total_saved


def prompt_year(label: str, default: int) -> int:
    while True:
        val = input(f"{label} [{default}]: ").strip()
        if not val:
            return default
        if val.isdigit() and 1900 <= int(val) <= datetime.now().year:
            return int(val)
        print(f"Enter a valid year (1900–{datetime.now().year}).")


def prompt_concepts() -> list:
    print("\nAvailable concepts:")
    for key, val in AVAILABLE_CONCEPTS.items():
        print(f"{key}. {val['name']}")

    print("\nEnter numbers separated by commas (e.g. 1,3)")
    print("Press Enter to select ALL.")

    while True:
        raw = input("Your choice: ").strip()
        if not raw:
            return list(AVAILABLE_CONCEPTS.values())
        parts   = [s.strip() for s in raw.split(",")]
        invalid = [s for s in parts if s not in AVAILABLE_CONCEPTS]
        if invalid:
            print(f"Invalid: {invalid}. Try again.")
            continue
        return [AVAILABLE_CONCEPTS[s] for s in parts]


def run_interactive():
    print("OpenAlex Paper Scraper")

    print("\nYear Range")
    year_from = prompt_year("From year", 1900)
    year_to   = prompt_year("To year", 2026)
    if year_from > year_to:
        print("Swapping years.")
        year_from, year_to = year_to, year_from

    print("\nConcepts")
    chosen = prompt_concepts()

    print("\nOutput Directory")
    print("Press Enter for default [raw/openalex]")
    out_dir = input("Output dir: ").strip() or "raw/openalex"

    print("\nSummary")
    print(f"Years    : {year_from} → {year_to}")
    print(f"Concepts : {', '.join(c['name'] for c in chosen)}")
    print(f"Output   : {out_dir}/")

    if input("\nStart scraping? [Y/n]: ").strip().lower() == "n":
        print("Aborted.")
        return

    cfg = {
        "email":         "ash007414@gmail.com",
        "year_from":     year_from,
        "year_to":       year_to,
        "per_page":      200,
        "request_delay": 0.1,
        "select_fields": [
            "id", "title", "abstract_inverted_index",
            "authorships", "publication_year",
            "primary_location", "concepts", "topics",
            "doi", "cited_by_count", "type",
            "ids", "updated_date",
        ],
        "output_dir": out_dir,
        "log_dir":    "logs",
    }

    session = requests.Session()
    session.headers.update({
        "User-Agent": f"ResearchIntelligenceEngine/1.0 (mailto:{cfg['email']})"
    })

    summary = []
    for concept in chosen:

        count = crawl_concept(concept, cfg, session)
        summary.append({"concept": concept["name"], "records": count})

    print("\nFinal Summary")
    for s in summary:
        print(f"{s['concept']:30s} → {s['records']:,} records")
    print("Done.")


if __name__ == "__main__":
    run_interactive()