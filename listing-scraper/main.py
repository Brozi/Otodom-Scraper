from crawler import Crawler
import time
import random
import re
import sys
import datetime
import logging

from jobs import export_to_github_actions
from services import ExportService


class TerminalLogger:
    def __init__(self, filename, stream):
        self.terminal = stream
        self.log_file = open(filename, "a", encoding="utf-8")
        self.ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    def write(self, message):
        self.terminal.write(message)
        self.terminal.flush()
        clean_msg = self.ansi_escape.sub('', message)
        self.log_file.write(clean_msg)
        self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()


log_filename = datetime.datetime.now().strftime("log/scraper_log_%Y-%m-%d_%H-%M-%S.txt")
sys.stdout = TerminalLogger(log_filename, sys.stdout)
sys.stderr = TerminalLogger(log_filename, sys.stderr)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout

)



def scrape_dynamic_chunk(crawler, current_min, current_max, master_list):
    """Recursively splits chunks if they have more than 100 pages."""
    if current_min > current_max:
        return

    print(f"\n---> Checking chunk: {current_min} PLN to {current_max} PLN")

    # 1. Setup crawler for this exact chunk
    crawler.settings.price_min = current_min
    crawler.settings.price_max = current_max
    crawler.params = crawler.generate_params()

    # 2. Count the pages
    pages, total_listings = crawler.count_pages()
    page_limit = 10

    # 3. Base case: 0 pages (Skip)
    if pages == 0:
        print(f"Skipping chunk {current_min} - {current_max} PLN (0 listings)")
        return

    # 4. Recursive case: Over 100 pages (Split in half)
    if pages > page_limit:
        print(
            f"Chunk {current_min} - {current_max} has {pages} pages (>{page_limit} limit), {total_listings} listings. Splitting in half...")
        mid_price = (current_min + current_max) // 2

        scrape_dynamic_chunk(crawler, current_min, mid_price, master_list)
        time.sleep(random.uniform(3.0, 5.0))
        scrape_dynamic_chunk(crawler, mid_price + 1, current_max, master_list)

    # 5. Base case: Safe to scrape (1 to 100 pages)
    else:
        print(f"Scraping SAFE chunk: {current_min} - {current_max} PLN ({pages} pages, {total_listings} listings.)")

        crawler.start(pages)

        # ADD THIS BLOCK to process investments found in this chunk:
        if hasattr(crawler, 'investments_queue') and crawler.investments_queue:
            crawler.process_investment_queue()

        if hasattr(crawler, 'listings'):
            master_list.extend(crawler.listings)
            crawler.listings = []

        print(f"Waiting ~30 seconds before the next chunk...")
        time.sleep(random.uniform(45.00, 60.00))


def main():
    export_service = ExportService()
    base_crawler = Crawler()

    # 1. Read the EXACT range assigned to this specific GitHub Action runner
    # The workflow file already injected this runner's specific bounds into settings.json
    target_min = base_crawler.settings.price_min
    target_max = base_crawler.settings.price_max

    all_listings = []

    try:
        for p_type in base_crawler.settings.property_types:
            print(f"\n{'=' * 60}")
            print(f"Starting property type: {p_type.value.upper()} for range {target_min} - {target_max} PLN")
            print(f"{'=' * 60}")

            # 2. Create a fresh crawler and explicitly assign the correct property type
            crawler = Crawler()
            crawler.settings.property_type = p_type

            # 3. Scrape the exact range assigned to this runner
            scrape_dynamic_chunk(crawler, target_min, target_max, all_listings)

    except KeyboardInterrupt:
        print("\nManually stopped by user!")
    except Exception as e:
        print(f"\nBLOCK DETECTED OR CRITICAL ERROR: {e}")

    finally:
        print(f"\nScript finished! Gathered {len(all_listings)} total listings in this chunk.")
        print("Saving gathered data to CSV...")

        if hasattr(base_crawler, 'listings'):
            base_crawler.listings = all_listings
            export_service.to_csv_file(all_listings,"listings.csv")
            export_service.to_excel_file(all_listings,"listings.xlsx")
        else:
            print("Could not find the listings list to save the CSV.")


if __name__ == "__main__":
    main()