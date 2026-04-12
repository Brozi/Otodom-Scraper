import time
import random
from typing import TYPE_CHECKING

# This tells Python to only import Crawler for code editors (like VS Code),
# but completely ignore it when actually running the script!
if TYPE_CHECKING:
    from crawler import Crawler

class RangeDiscoverer:
    def __init__(self, max_listings_per_chunk=2800, global_max=14000000):
        """
        Initializes the discoverer.
        :param max_listings_per_chunk: The safe limit before we split the range (Otodom max is ~3000).
        :param global_max: The price cap before we just dump everything into a "14m+" bucket.
        """
        self.min_range_name = "min"
        self.max_range_name = "max"
        self.max_listings_per_chunk = max_listings_per_chunk
        self.global_max = global_max
        self.discovered_ranges = []
    def discover(self, crawler: 'Crawler', current_min: int, current_max: int):
        """Recursively checks price ranges and splits them if they are too large."""

        if current_min > current_max:
            return

        print(f"Checking range: {current_min} to {current_max} PLN")

        # Configure crawler for this specific range
        crawler.settings.price_min = current_min
        crawler.settings.price_max = current_max
        crawler.params = crawler.generate_params()

        # Count pages using the existing crawler logic
        pages, total_listings = crawler.count_pages()

        if total_listings == 0:
            print(f"  -> Empty range (0 listings). Skipping.")
            return

        if total_listings > self.max_listings_per_chunk:
            print(f"  -> Too many listings ({total_listings}). Splitting range in half...")
            mid_price = (current_min + current_max) // 2

            # Anti-bot delay
            time.sleep(random.uniform(2.0, 4.0))

            # Recurse on both halves
            self.discover(crawler, current_min, mid_price)
            self.discover(crawler, mid_price + 1, current_max)
        else:
            print(f"  -> Safe range found! {current_min} - {current_max} PLN ({total_listings} listings)")
            self.discovered_ranges.append({self.min_range_name: current_min, self.max_range_name: current_max})

    def get_final_matrix(self):
        """Sorts the ranges and adds the final infinite catch-all range."""
        # Sort sequentially by minimum price
        sorted_ranges = sorted(self.discovered_ranges, key=lambda x: x[self.min_range_name])

        # Add the final 5m+ range
        sorted_ranges.append({
            self.min_range_name: self.global_max + 1,
            self.max_range_name: 99999999
        })

        return sorted_ranges