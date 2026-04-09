from crawler import Crawler


def main():
    crawler = Crawler()

    # Loop through all the property types defined in settings.json
    for p_type in crawler.settings.property_types:
        print(f"\n{'=' * 50}")
        print(f"Scraping for property type: {p_type.value.upper()}")
        print(f"{'=' * 50}\n")

        # Update the active property type for the crawler
        crawler.settings.property_type = p_type

        # Start the scraping process for this type
        crawler.start()

    # After ALL types are scraped, save everything to one CSV
    crawler.to_csv_file("listings.csv")


if "__main__" == __name__:
    main()