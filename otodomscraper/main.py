from crawler import Crawler
import time
import random
import re
import sys
import datetime

class TerminalLogger:
    def __init__(self, filename, stream):
        self.terminal = stream
        self.log_file = open(filename, "a", encoding="utf-8")
        # This removes the ugly color codes from the text file
        self.ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

    def write(self, message):
        # 1. Print to the terminal normally (keeps red colors intact!)
        self.terminal.write(message)

        # 2. Save a clean, color-free version to the text file
        clean_msg = self.ansi_escape.sub('', message)
        self.log_file.write(clean_msg)
        self.log_file.flush()

    def flush(self):
        self.terminal.flush()
        self.log_file.flush()


# Generate the log file name
log_filename = datetime.datetime.now().strftime("scraper_log_%Y-%m-%d_%H-%M-%S.txt")

# Intercept BOTH standard prints and error/warning messages
sys.stdout = TerminalLogger(log_filename, sys.stdout)
sys.stderr = TerminalLogger(log_filename, sys.stderr)


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