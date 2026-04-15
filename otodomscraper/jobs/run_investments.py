import sys
import json
from crawler.crawler import Crawler
import logging
import time
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout

)


def main():
    if len(sys.argv) < 2:
        return

    urls = json.loads(sys.argv[1])
    if not urls: return

    crawler = Crawler()
    for url in urls:
        crawler.investments_queue.add(url)

    crawler.process_investment_queue()

    timestamp = int(time.time())
    csv_filename = f"listings_{timestamp}.csv"
    excel_filename = f"listings_{timestamp}.xlsx"

    crawler.to_csv_file(csv_filename)
    from pandas import read_csv
    if os.path.exists(csv_filename):
        df = read_csv(csv_filename)
        df.to_excel(excel_filename, index=False)
    else:
        print(f"No CSV generated (0 new listings found). Skipping read.")


if __name__ == "__main__":
    main()