import sys
import json
from crawler.crawler import Crawler
import logging

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

    import time
    timestamp = int(time.time())
    crawler.to_csv_file(f"investments_{timestamp}.csv")
    from pandas import read_csv
    df = read_csv(f"listings_{timestamp}.csv")
    df.to_excel(f"listings_{timestamp}.xlsx", index=False)


if __name__ == "__main__":
    main()