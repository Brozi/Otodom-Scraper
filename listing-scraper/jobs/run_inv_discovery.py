import os
import json
import time
import random
import sys
import logging

# Ensure Python can find the otodomscraper modules
current_dir = os.path.dirname(os.path.abspath(__file__))
scraper_dir = os.path.dirname(current_dir)
sys.path.append(scraper_dir)
os.chdir(scraper_dir)

from crawler.crawler import Crawler
from settings.s_types import PropertyType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout

)
"""
This script is used to discover primary investments. It runs parallel with 
the main discovery script
"""


def discover():
    # 1. Initialize Crawler and set to investments
    crawler = Crawler()
    crawler.settings.property_type = PropertyType.INVESTMENT

    total_pages, _ = crawler.count_pages()
    investment_urls = set()

    print(f"Starting Investment Discovery. Total pages to scan: {total_pages}")

    for page in range(1, (total_pages or 1) + 1):
        print(f"\n--- Scanning page {page}/{total_pages} ---")
        items = crawler.extract_listings_from_page(page)

        # 2. Extract and print each URL
        new_on_page = 0
        for item in items:
            slug = item.get("slug")
            if slug:
                url = f"https://www.otodom.pl/pl/oferta/{slug}"
                if url not in investment_urls:
                    investment_urls.add(url)
                    new_on_page += 1
                    print(f" [NEW] Saved investment URL: {url}")

        print(f"Found {new_on_page} new investments on page {page}.")

        # 3. Anti-DataDome: Stop early if we reached the end
        if page == total_pages:
            break

        # 4. Anti-DataDome: Random delays between requests
        delay = random.uniform(2.5, 5.5)
        print(f"Sleeping for {delay:.2f}s to prevent DataDome block...")
        time.sleep(delay)

        # 5. Anti-DataDome: Rotate session every 5 pages
        if page % 5 == 0:
            print("Rotating session (Re-initializing crawler) to avoid fingerprinting...")
            crawler = Crawler()
            crawler.settings.property_type = PropertyType.INVESTMENT

    # 6. Prepare matrix output
    urls = list(investment_urls)
    print(f"\n==========================================")
    print(f"Discovery Complete! Total unique investments: {len(urls)}")
    print(f"==========================================\n")

    # Chunk them into arrays of 5 for the GitHub Actions Matrix
    chunk_size = 5
    chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
    if not chunks:
        chunks = [[]]

    # Output to GitHub Actions
    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as env:
            env.write(f'matrix={json.dumps(chunks)}\n')


if __name__ == "__main__":
    discover()