from crawler import Crawler
import time
import random

import sys
import datetime

class DualLogger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log_file = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)
        self.log_file.flush()  # Instantly saves to the file

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()

# Generate a filename with the current date/time
log_filename = datetime.datetime.now().strftime("scraper_log_%Y-%m-%d_%H-%M-%S.txt")

# Redirect all print statements and errors to our DualLogger
sys.stdout = DualLogger(log_filename)
sys.stderr = sys.stdout  # This captures crash errors too!


def main():
    base_crawler = Crawler()
    original_min = base_crawler.settings.price_min
    original_max = base_crawler.settings.price_max

    CHUNK_STEP = 300000

    # Create a master list to hold ALL apartments across all chunks
    all_listings = []

    try:
        for p_type in base_crawler.settings.property_types:
            print(f"\n{'=' * 60}")
            print(f"Starting property type: {p_type.value.upper()}")
            print(f"{'=' * 60}")

            current_min = original_min

            while current_min < original_max:
                current_max = current_min + CHUNK_STEP
                if current_max > original_max:
                    current_max = original_max

                print(f"\n---> Scraping chunk: {current_min} PLN to {current_max} PLN")

                chunk_crawler = Crawler()
                chunk_crawler.settings.property_type = p_type
                chunk_crawler.settings.price_min = current_min
                chunk_crawler.settings.price_max = current_max

                # ADD THIS LINE: Force it to rebuild the URL parameters!
                chunk_crawler.params = chunk_crawler.generate_params()

                chunk_crawler.start()

                # After the chunk finishes, grab its scraped data and add it to our master list
                if hasattr(chunk_crawler, 'listings'):
                    all_listings.extend(chunk_crawler.listings)

                current_min = current_max + 1
                print("Waiting a few seconds before the next chunk...")
                time.sleep(random.uniform(5.0, 10.0))

    except KeyboardInterrupt:
        print("\nManually stopped by user!")
    except Exception as e:
        print(f"\nBLOCK DETECTED OR CRITICAL ERROR: {e}")

    finally:
        print(f"\nScript finished! Gathered {len(all_listings)} total listings.")
        print("Saving gathered data to CSV...")

        # Give all the gathered data back to the base crawler so it can export it
        if hasattr(base_crawler, 'listings'):
            base_crawler.listings = all_listings
            base_crawler.to_csv_file("listings.csv")
        else:
            print("Could not find the listings list to save the CSV.")


if __name__ == "__main__":
    main()