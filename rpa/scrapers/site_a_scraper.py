"""
Scraper — now driven by an input JSON file instead of a hardcoded URL list.

Input file format (a JSON array):
    [
        {"url": "https://books.toscrape.com/catalogue/.../index.html", "source_name": "books_toscrape"},
        {"url": "https://books.toscrape.com/catalogue/.../index.html", "source_name": "books_toscrape"}
    ]

Records are grouped by source_name — each group is scraped and enqueued
as its own Celery batch, same as before.

NOTE: the XPath selectors below are specific to books.toscrape.com's page
structure. This works for any number of books.toscrape URLs regardless of
what you label source_name, but if you ever point this at a genuinely
different site's HTML, these selectors won't match — that would need a
per-source_name selector mapping, which isn't built yet.

Run from the command line:
    python -m rpa.scrapers.site_a_scraper --seed_file input/urls.json

Or import and call scrape_records(records) directly (used by the
Streamlit dashboard's "Run New Scrape" section).
"""

import argparse
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests
from lxml import html

from rpa.utils.retry import retry_and_recover
from ingestion.celery_worker import process_scraped_batch

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# XPath expressions for the product detail page (books.toscrape.com structure)
XPATH_TITLE = "//h1/text()"
XPATH_PRICE = "//div[@class='col-sm-6 product_main']/p[@class='price_color']//text()"
XPATH_STOCK = "//div[@class='col-sm-6 product_main']/p[@class='instock availability']//text()"


@retry_and_recover(max_attempts=3, base_delay=2.0)
def scrape_product(url: str, source_name: str) -> dict:
    """Fetch a product page via requests, then extract fields with XPath."""
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()

    tree = html.fromstring(response.content)

    title_parts = tree.xpath(XPATH_TITLE)
    title = title_parts[0].strip() if title_parts else None

    price_parts = tree.xpath(XPATH_PRICE)
    price_text = "".join(price_parts).strip()
    price = float(price_text.replace("£", "").replace(",", "").strip())

    stock_parts = tree.xpath(XPATH_STOCK)
    stock_text = " ".join(part.strip() for part in stock_parts if part.strip())
    is_in_stock = "in stock" in stock_text.lower()

    if not title:
        raise ValueError(f"Could not extract title from {url} — selector may need updating")

    return {
        "source_site": source_name,
        "source_url": url,
        "title": title,
        "price": price,
        "currency": "GBP",
        "in_stock": is_in_stock,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def load_input_records(input_path: str) -> list[dict]:
    """
    Load and validate the input file. Accepts EITHER format:
      - a single JSON array: [{"url": ..., "source_name": ...}, ...]
      - JSONL: one JSON object per line (no surrounding brackets/commas)
    """
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with open(path, "r") as f:
        text = f.read()

    records = None

    # Try as a single JSON array first.
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            records = parsed
    except json.JSONDecodeError:
        pass

    # Fall back to JSONL: one JSON object per line.
    if records is None:
        records = []
        for line_num, line in enumerate(text.strip().splitlines(), start=1):
            import pdb; pdb.set_trace()
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Could not parse {input_path} as a JSON array OR as JSONL. "
                    f"Line {line_num} failed: {exc}"
                )

    for rec in records:
        if "url" not in rec or "source_name" not in rec:
            raise ValueError(f"Each record needs 'url' and 'source_name' keys, got: {rec}")

    return records


def scrape_records(records: list[dict]) -> dict:
    """
    Groups records by source_name, scrapes each group, and enqueues one
    Celery batch per group. Returns a summary dict for logging/display.
    """
    grouped = defaultdict(list)
    for rec in records:
        grouped[rec["source_name"]].append(rec["url"])

    summary = {}

    for source_name, urls in grouped.items():
        print(f"\n=== Source: {source_name} ({len(urls)} URLs) ===")
        results = []
        for url in urls:
            try:
                data = scrape_product(url, source_name)
                results.append(data)
                print(f"OK   {url} -> £{data['price']}")
            except Exception as exc:
                print(f"FAIL {url} -> {exc}")
            time.sleep(1.0)

        if results:
            async_result = process_scraped_batch.delay(results, source_name)
            print(f"Enqueued {len(results)} records for '{source_name}' -> task id {async_result.id}")
            summary[source_name] = {"scraped": len(results), "attempted": len(urls), "task_id": async_result.id}
        else:
            summary[source_name] = {"scraped": 0, "attempted": len(urls), "task_id": None}

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape product pages listed in a JSON input file.")
    parser.add_argument(
        "--seed_file",
        type=str,
        default="input/urls.json",
        help="Path to a JSON file containing a list of {url, source_name} objects (default: input/urls.json)",
    )
    args = parser.parse_args()

    records = load_input_records(args.seed_file)
    print(f"Loaded {len(records)} records from {args.seed_file}")
    scrape_records(records)