import json
import os
from crawler.crawler import Crawler
from common import PropertyType


def discover():
    crawler = Crawler()
    # Force search to look only at the investments category
    crawler.settings.property_type = PropertyType.INVESTMENT

    total_pages, _ = crawler.count_pages()
    investment_urls = set()

    for page in range(1, (total_pages or 1) + 1):
        items = crawler.extract_listings_from_page(page)
        for item in items:
            slug = item.get("slug")
            if slug:
                investment_urls.add(f"https://www.otodom.pl/pl/oferta/{slug}")

    urls = list(investment_urls)
    chunk_size = 5
    chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
    if not chunks: chunks = [[]]

    with open(os.environ['GITHUB_OUTPUT'], 'a') as env:
        env.write(f'matrix={json.dumps(chunks)}\n')


if __name__ == "__main__":
    discover()