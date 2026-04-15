import sys
import json
from crawler.crawler import Crawler


def main():
    if len(sys.argv) < 2:
        return

    urls = json.loads(sys.argv[1])
    if not urls: return

    crawler = Crawler()
    for url in urls:
        crawler.investments_queue.add(url)

    crawler.process_investment_queue()


if __name__ == "__main__":
    main()