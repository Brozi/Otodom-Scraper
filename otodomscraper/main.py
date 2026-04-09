from crawler import Crawler


def main():
    crawler = Crawler()

    # Save the original min and max prices from your settings.json
    original_min = crawler.settings.price_min
    original_max = crawler.settings.price_max

    # Define how big each price chunk should be (300,000 PLN is very safe)
    CHUNK_STEP = 300000

    # Loop through property types (flats, houses)
    for p_type in crawler.settings.property_types:
        crawler.settings.property_type = p_type

        print(f"\n{'=' * 60}")
        print(f" Starting property type: {p_type.value.upper()}")
        print(f"{'=' * 60}")

        # Start the chunking loop!
        current_min = original_min

        while current_min < original_max:
            # Calculate the max price for this specific chunk
            current_max = current_min + CHUNK_STEP

            # Make sure the chunk doesn't exceed your overall max price
            if current_max > original_max:
                current_max = original_max

            print(f"\n---> Scraping chunk: {current_min} PLN to {current_max} PLN")

            # Temporarily trick the crawler's settings
            crawler.settings.price_min = current_min
            crawler.settings.price_max = current_max

            # Run the scraper for just this small price range
            crawler.start()

            # Move the minimum up for the next loop (+1 to avoid overlapping prices)
            current_min = current_max + 1

    # After ALL types and ALL price chunks are done, save to one big CSV!
    print("\nAll chunks finished! Saving to CSV...")
    crawler.to_csv_file("listings.csv")


if "__main__" == __name__:
    main()