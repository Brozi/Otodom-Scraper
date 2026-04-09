import concurrent.futures
import csv
import logging
import json
import re

import requests
from bs4 import BeautifulSoup
from bs4 import ResultSet
from common import Constans
from common import OfferedBy
from crawler.exceptions import DataExtractionError
from crawler.listing import Listing
from models import AgencyDocument
from models import PropertyDocument
from services import AgencyService
from services import connect_to_database
from services import PropertyService
from settings import Settings

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


class Crawler:
    """
    A crawler for the otodom.pl website.

    The crawler is responsible for crawling the website and extracting the data
    and updating the database.
    """

    def __init__(self):
        """
        Initialize the crawler.
        """
        self.settings: Settings = Settings()
        self.params: dict = self.generate_params()
        self.listings: list[Listing] = []
        connect_to_database(host=self.settings.mongo_db_host)

    def generate_search_url(self) -> str:
        """
        Generate the URL to crawl.

        :return: The URL to crawl
        """
        url = self.settings.base_url
        url += "/pl/wyniki/"
        url += f"{self.settings.auction_type.value}/"
        url += f"{self.settings.property_type.value}/"
        url += f"{self.settings.province}/"
        url += f"{self.settings.city}/"

        if self.settings.district:
            url += f"{self.settings.district}/"

        return url

    def generate_params(self) -> dict:
        """
        Generate the parameters for the URL.

        :return: The parameters for the URL
        """
        return {
            "priceMin": self.settings.price_min,
            "priceMax": self.settings.price_max,
        }

    def count_pages(self) -> int:
        """
        Count the number of pages to crawl using Regex to bypass HTML parser limits.
        """
        max_retries = 3
        while max_retries > 0:
            logger.info(f"Counting pages to crawl, try: {4 - max_retries}/3")

            search_url = self.generate_search_url()
            response = requests.get(
                url=search_url, params=self.params, headers=HEADERS, timeout=20
            )
            html = response.text
            print(f"Status: {response.status_code}, Length: {len(html)}")
            # --- DEBUG: SAVE RAW HTML ---
            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("Saved raw HTML to 'debug_page.html'")
            # ----------------------------

            # This regex perfectly matches your exact tag and ignores the extra attributes
            match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text, re.DOTALL)

            if match:
                try:
                    json_text = match.group(1)
                    data = json.loads(json_text)

                    page_count = data["props"]["pageProps"]["tracking"]["listing"]["page_count"]

                    logger.info(f"Found {page_count} pages to crawl (from JSON data)")
                    return int(page_count)

                except (KeyError, TypeError, ValueError) as e:
                    logger.warning(f"Error extracting page_count from JSON: {e}")
            else:
                logger.warning("__NEXT_DATA__ script tag not found in the raw HTML string.")

            max_retries -= 1

        logger.warning("No listings found with given parameters. Exiting...")
        exit(1)

    def extract_listings_from_page(self, page: int) -> set:
        """
        Crawl the given page.

        :param page: The page number to crawl
        :return: The listings on the page
        """
        params = self.params.copy()
        params["page"] = page
        response = requests.get(
            url=self.generate_search_url(), params=params, headers=HEADERS, timeout=10
        )
        logger.info(f"Extracting listings from page {page}")
        soup = BeautifulSoup(response.content, "html.parser")
        listings = soup.select("div[data-cy=listing-item]")
        return listings

    def extract_listing_data(self, listing_data: ResultSet) -> None:
        """
        Extract the data from the given listing.

        At this point, the data is being extracted from the listing page,
        and as the function is executed, the data is being saved to the database.

        It scrapes both the property and the agency data.

        At the end if the the listing is unique (wasn't found earlier)
        it is added to the self.listing list

        :param listing_data: The HTML part of the listing at the search page.
        """
        listing = Listing()
        property_ = PropertyDocument()
        property_.set_link(listing_data)
        property_.set_promoted(listing_data)
        try:
            soup = self.try_get_listing_page(url=property_.link)
        except DataExtractionError as e:
            logger.exception(
                f"Failed to extract data from {property_.link}, Error: {e}"
            )
            return
        property_.extract_data(soup)
        if property_.offered_by == OfferedBy.ESTATE_AGENCY:
            agency = AgencyDocument()
            agency.extract_data(soup)
            agency_doc = AgencyService.get_by_otodom_id(agency.otodom_id)
            if agency_doc is None:
                agency_doc = AgencyService.put(agency)
            property_.estate_agency = agency_doc.to_dbref()
            listing.agency = agency_doc
        if PropertyService.get_by_otodom_id(property_.otodom_id) is None:
            logger.info(f"Adding new property {property_.link} to database")
            property_ = PropertyService.put(property_)
            listing.property_ = property_
            self.listings.append(listing)

    def try_get_listing_page(self, url: str) -> BeautifulSoup:
        """
        Tries to get the listing page.

        After 3 failures raises DataExtractionError

        :param url: The URL of the listing page
        :raises DataExtractionError: If the data extraction fails
        :return: The data from the listing
        """
        max_retries = 3
        while max_retries > 0:
            response = requests.get(url=url, headers=HEADERS)
            soup = BeautifulSoup(response.content, "html.parser")
            if not PropertyDocument.informational_json_exists(soup):
                max_retries -= 1
                continue
            return soup
        raise DataExtractionError(url=url)

    def to_csv_file(self, filename: str) -> None:
        """
        Saves the listings to a json file.

        :param filename: The name of the file
        """
        logger.info(f"Saving listings to {filename}. Format: csv")
        data = [listing.to_dict() for listing in self.listings]

        with open(filename, "w", newline="", encoding="utf-8") as file:
            dict_writer = csv.DictWriter(file, Constans.CSV_KEYS)
            dict_writer.writeheader()
            dict_writer.writerows(data)

    def to_json_file(self, filename: str) -> None:
        """
        Saves the listings to a csv file.

        :param filename: The name of the file
        """
        logger.info(f"Saving listings to {filename}. Format: json")
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(
                [listing.to_dict() for listing in self.listings],
                file,
                ensure_ascii=False,
                default=str,
                indent=4,
            )

    def start(self) -> None:
        """
        Starts the crawler.

        The crawler starts crawling the website and extracting the data.
        """
        pages = self.count_pages()
        with concurrent.futures.ThreadPoolExecutor(max_workers=25) as executor:
            listings = list(
                executor.map(self.extract_listings_from_page, range(1, pages + 1))
            )

        existing_links = PropertyService.get_all_links()
        listings = {
            listing_data
            for sublist in listings
            for listing_data in sublist
            if Constans.DEFAULT_URL + PropertyDocument.extract_link(listing_data)
            not in existing_links
        }
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.extract_listing_data, listings)
