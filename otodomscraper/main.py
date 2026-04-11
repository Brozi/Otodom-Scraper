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

    # 3. Base case: 0 pages (Skip)
    if pages == 0:
        print(f"Skipping chunk {current_min} - {current_max} PLN (0 listings)")
        return

    # 4. Recursive case: Over 100 pages (Split in half)
    if pages > 100:
        print(f"Chunk {current_min} - {current_max} has {pages} pages (>100 limit), {total_listings} listings. Splitting in half...")
        mid_price = (current_min + current_max) // 2

        # Run first half
        scrape_dynamic_chunk(crawler, current_min, mid_price, master_list)

        time.sleep(random.uniform(3.0, 5.0))  # Small delay between splits

        # Run second half
        scrape_dynamic_chunk(crawler, mid_price + 1, current_max, master_list)

    # 5. Base case: Safe to scrape (1 to 100 pages)
    else:
        print(f"Scraping SAFE chunk: {current_min} - {current_max} PLN ({pages} pages, {total_listings} listings)")

        # Pass the pre-counted pages directly to start()!
        crawler.start(pages)

        # Store results and clear memory
        if hasattr(crawler, 'listings'):
            master_list.extend(crawler.listings)
            crawler.listings = []

        print("Waiting a few seconds before the next chunk...")
        time.sleep(random.uniform(5.0, 10.0))


def main():
    base_crawler = Crawler()
    original_min = base_crawler.settings.price_min
    original_max = base_crawler.settings.price_max
    CHUNK_STEP = 300000

    all_listings = []

    try:
        for p_type in base_crawler.settings.property_types:
            print(f"\n{'=' * 60}")
            print(f"Starting property type: {p_type.value.upper()}")
            print(f"{'=' * 60}")

            base_crawler.settings.property_type = p_type

            # Reset current_min for each property type
            current_min = original_min

            # Loop through the base 300k chunks
            while current_min < original_max:
                current_max = current_min + CHUNK_STEP
                if current_max > original_max:
                    current_max = original_max

                print(f"\n---> Starting Base Chunk: {current_min} PLN to {current_max} PLN")

                fresh_crawler = Crawler()

                # Pass the 300k chunk into the dynamic splitter.
                # If it's < 100 pages, it scrapes it immediately.
                # If it's > 100 pages, it splits it down into 150k chunks automatically!
                scrape_dynamic_chunk(fresh_crawler, current_min, current_max, all_listings)

                current_min = current_max + 1

    except KeyboardInterrupt:
        print("\nManually stopped by user!")
    except Exception as e:
        print(f"\nBLOCK DETECTED OR CRITICAL ERROR: {e}")

    finally:
        print(f"\nScript finished! Gathered {len(all_listings)} total listings.")
        print("Saving gathered data to CSV...")

        if hasattr(base_crawler, 'listings'):
            base_crawler.listings = all_listings
            base_crawler.to_csv_file("listings.csv")
            from pandas import DataFrame, read_csv
            df = read_csv("listings.csv")
            df.to_excel("listings.xlsx", index=False)
        else:
            print("Could not find the listings list to save the CSV.")


if __name__ == "__main__":
    main()