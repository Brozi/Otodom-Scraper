import sys
import os

# Add the parent directory to the path so it can import your modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.merge import DataMerger


def main():
    # Calculate absolute paths to ensure it works regardless of where it's executed from
    current_dir = os.path.dirname(os.path.abspath(__file__))  # path/to/jobs
    scraper_dir = os.path.dirname(current_dir)  # path/to/otodomscraper
    repo_root = os.path.dirname(scraper_dir)  # path/to/Otodom-Scraper

    # The GitHub Action downloads chunks into the "all_data" folder in the repo root
    input_directory = os.path.join(repo_root, "all_data")

    # We want to save the final merged files inside the otodomscraper folder
    output_directory = scraper_dir

    print("Initializing DataMerger...")
    merger = DataMerger(input_dir=input_directory, output_dir=output_directory)
    merger.merge()


if __name__ == "__main__":
    main()