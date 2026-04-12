import sys
import os
import json

# Add the parent directory to the path so it can import your modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawler import Crawler
from services.discovery import RangeDiscoverer


def export_to_github_actions(ranges: list):
    matrix_json = json.dumps(ranges)
    print(f"\nFinal Matrix JSON: {matrix_json}")

    if "GITHUB_OUTPUT" in os.environ:
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(f"matrix={matrix_json}\n")


def main():
    crawler = Crawler()
    discoverer = RangeDiscoverer()

    discoverer.discover(crawler, 0, discoverer.global_max)
    final_ranges = discoverer.get_final_matrix()

    export_to_github_actions(final_ranges)


if __name__ == "__main__":
    main()