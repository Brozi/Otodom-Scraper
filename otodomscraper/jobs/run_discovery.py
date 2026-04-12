import sys
import os
import json
import uuid

# Add the parent directory to the path so it can import your modules
#sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

current_dir = os.path.dirname(os.path.abspath(__file__))  # path/to/jobs
scraper_dir = os.path.dirname(current_dir)                # path/to/otodomscraper

# Add scraper_dir to path so it can import your modules
sys.path.append(scraper_dir)

# --- ADD THIS LINE ---
# Change the working directory so Python finds settings.json exactly where it expects it!
os.chdir(scraper_dir)

from crawler import Crawler
from services.discovery import RangeDiscoverer


def export_to_github_actions(ranges: list):
    # Convert [{"min": 0, "max": 10000}, ...] into ["0-10000", ...]
    # This completely bypasses the GitHub Actions JSON secret scanner
    flattened_ranges = [f"{r['min']}-{r['max']}" for r in ranges]
    matrix_json = json.dumps(flattened_ranges)

    print(f"\nFlattened Matrix: {flattened_ranges}")

    if "GITHUB_OUTPUT" in os.environ:
        delimiter = f"EOF-{uuid.uuid4()}"
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"matrix<<{delimiter}\n")
            f.write(f"{matrix_json}\n")
            f.write(f"{delimiter}\n")


def main():
    crawler = Crawler()

    # 1. Read limits and chunk configurations directly from settings
    global_min = crawler.settings.price_min
    global_max = crawler.settings.price_max

    # Use getattr() just in case an older settings.json doesn't have the key yet
    chunk_limit = getattr(crawler.settings, "max_listings_per_chunk", 2800)

    print(f"Loaded global limits: {global_min} - {global_max} PLN")
    print(f"Configured Max Listings Per Chunk: {chunk_limit}")

    # 2. Pass them to the Discoverer
    discoverer = RangeDiscoverer(max_listings_per_chunk=chunk_limit, global_max=global_max)

    # 3. Discover
    discoverer.discover(crawler, global_min, global_max)

    final_ranges = discoverer.get_final_matrix()
    export_to_github_actions(final_ranges)


if __name__ == "__main__":
    main()