import logging
import sys
from crawler.crawler import Crawler

# 1. Setup logging so you can see the terminal output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)


def run_test():
    print("Initializing Crawler...")
    crawler = Crawler()

    # 2. Hardcode an investment URL you know has multiple pages
    test_url = "https://www.otodom.pl/pl/oferta/piasta-towers-ID4uatL"
    # Or test the one that was failing:
    # test_url = "https://www.otodom.pl/pl/oferta/kombinat-mieszkan-ID4znLZ"

    # 3. Add it to the queue
    crawler.investments_queue.add(test_url)

    print(f"\n--- STARTING TEST ---")
    # 4. Trigger the queue processor directly
    crawler.process_investment_queue()

    print(f"--- TEST FINISHED ---")


if __name__ == "__main__":
    run_test()