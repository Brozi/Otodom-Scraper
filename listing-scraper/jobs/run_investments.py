import sys
import json
import os
current_dir = os.path.dirname(os.path.abspath(__file__))  # path/to/jobs
scraper_dir = os.path.dirname(current_dir)                # path/to/otodomscraper
# Add scraper_dir to path so it can import modules

sys.path.append(scraper_dir)
from crawler import Crawler
os.chdir(scraper_dir)
#change the dir to the default one
import logging
import time
from services import ExportService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout

)
"""
This script is used for processing investments found by run_inv_discovery.py, and 
for processing hidden investments added to found_investments.txt during normal
crawler operation.
"""


def main():
    if len(sys.argv) < 2:
        return

    urls = json.loads(sys.argv[1])
    if not urls: return

    crawler = Crawler()
    for url in urls:
        crawler.investments_queue.add(url)

    crawler.investment_processor.process_queue(crawler.investments_queue)

    timestamp = int(time.time())
    csv_filename = f"listings_{timestamp}.csv"
    excel_filename = f"listings_{timestamp}.xlsx"
    listings = crawler.listings
    export_service = ExportService()

    export_service.to_csv_file(listings,csv_filename)
    export_service.to_excel_file(excel_filename)

if __name__ == "__main__":
    main()