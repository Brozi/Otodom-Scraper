import concurrent.futures
import csv
import logging
import json
import re
import random

from curl_cffi import requests
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
    # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    # "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    # "Accept-Encoding": "gzip, deflate, br",
    # "Connection": "keep-alive",
    # "Upgrade-Insecure-session": "1",
    # "Sec-Fetch-Dest": "document",
    # "Sec-Fetch-Mode": "navigate",
    # "Sec-Fetch-Site": "none",
    # "Sec-Fetch-User": "?1",
    # "Cache-Control": "max-age=0",
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
        self.session = requests.Session(impersonate="chrome120")
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

    def count_pages(self) -> tuple[int, int] | None:
        """
        Count the number of pages to crawl using Regex to bypass HTML parser limits.
        """
        max_retries = 3
        while max_retries > 0:
            delay = random.uniform(6.0, 10.0)
            print(f"Delaying page count request by {delay:.2f} seconds...")
            import time
            time.sleep(delay)

            logger.info(f"Counting pages to crawl, try: {4 - max_retries}/3")
            search_url = self.generate_search_url()
            response = self.session.get(url=search_url, params=self.params, timeout=20)
            html = response.text
            print(f"Status: {response.status_code}, Length: {len(html)}")
            if response.status_code in [403, 405, 429]:
                cooldown = random.uniform(600.0, 660.0)
                print(f"\nDATADOME BLOCK DETECTED! Sleeping {cooldown/60:.2f}min to clear the pentalty box... ")
                import time
                time.sleep(cooldown)
                self.session = requests.Session(impersonate="chrome120")  # Get fresh browser
                max_retries -= 1
                continue

            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(html)

            match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))
                    ad_data = data["props"]["pageProps"].get("ad", {})
                    if "paginatedUnits" in ad_data:
                        listing_data = ad_data["paginatedUnits"]
                        page_count = listing_data["pagination"]["totalPages"]
                        item_count = listing_data["pagination"].get("totalResults", 0)
                    else:
                        page_count = data["props"]["pageProps"]["tracking"]["listing"]["page_count"]
                        listing_data = data["props"]["pageProps"]["tracking"]["listing"]
                        item_count = listing_data.get("result_count", 0)

                    return int(page_count), int(item_count)

                except (KeyError, TypeError, ValueError) as e:
                    logger.warning(f"Error extracting JSON: {e}")

            import time
            time.sleep(5)
            max_retries -= 1

        logger.warning("No listings found with given parameters or blocked.")
        # 2. Stop the script so we don't lose data.
        raise Exception("CRITICAL: Failed to count pages 3 times. IP is temporarily blocked.")

    def extract_listings_from_page(self, page: int, override_url: str = None) -> list:
        """
        Crawl the given page and extract listings from the Next.js JSON.
        """
        params = self.params.copy()
        params["page"] = page
        url = override_url if override_url else self.generate_search_url()

        import time, random
        # Change max_retries to 1. If it blocks us, try ONE more time, then abandon the page.
        max_retries = 3

        while max_retries >= 0:
            page_delay = random.uniform(6.0, 10.0)
            print(f" Delaying page {page} request by {page_delay:.2f} seconds...")
            time.sleep(page_delay)

            try:
                response = self.session.get(
                    url=url, params=params, timeout=15)

                if response.status_code in [403, 405, 429]:
                    # We now know the penalty box is roughly 10 minutes.
                    # Let's just wait it out completely so we don't lose ANY pages!
                    cooldown = random.uniform(600.0, 660.0)  # 10 to 11 minutes
                    logger.warning(
                        f"DATADOME BLOCK on page {page}! Sleeping {cooldown / 60:.2f} minutes to clear the penalty box...")
                    time.sleep(cooldown)
                    self.session = requests.Session(impersonate="chrome120")
                    max_retries -= 1
                    continue

                logger.info(f"Extracting listings from page {page}")
                html = response.text
                marker = 'id="__NEXT_DATA__"'

                if marker in html:
                    tag_start = html.find(marker)
                    json_start = html.find('>', tag_start) + 1
                    json_end = html.find('</script>', json_start)

                    json_text = html[json_start:json_end].strip()
                    data = json.loads(json_text)

                    ad_data = data["props"]["pageProps"].get("ad", {})
                    if "paginatedUnits" in ad_data:
                        items = ad_data["paginatedUnits"]["items"]
                        return items
                    else:
                        items = data["props"]["pageProps"]["data"]["searchAds"]["items"]
                        return items


                else:
                    logger.warning(f"__NEXT_DATA__ not found on page {page}. Status Code: {response.status_code}")
                    time.sleep(5)
                    max_retries -= 1

            except Exception as e:
                logger.warning(f"Error extracting items on page {page}: {e}")
                time.sleep(5)
                max_retries -= 1

        logger.error(f"CRITICAL: Failed to extract page {page} after 3 retries. Skipping page.")
        return []

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
        import time
        time.sleep(random.uniform(1.5, 4.0))
        listing = Listing()
        property_ = PropertyDocument()

        # Read directly from the JSON dictionary we passed!
        property_.link = listing_data["full_url"]
        property_.is_promoted = listing_data.get("isPromoted", False)
        print(f" Found apartment! Visiting: {property_.link}")

        try:
            soup = self.try_get_listing_page(url=property_.link)
        except DataExtractionError as e:
            logger.exception(f"Failed to extract HTML from {property_.link}, Error: {e}")
            return

        # --- NEW SAFETY NET START ---
        try:
            property_.extract_data(soup)

            if property_.offered_by == OfferedBy.ESTATE_AGENCY:
                agency = AgencyDocument()
                agency.extract_data(soup)
                agency_doc = AgencyService.get_by_otodom_id(agency.otodom_id)

                if agency_doc is None:
                    agency_doc = AgencyService.put(agency)
                property_.estate_agency = agency_doc.to_dbref()
                listing.agency = agency_doc

            if property_.offered_by == OfferedBy.DEVELOPER:
                self.process_investment(property_.link)
                return
            if PropertyService.get_by_otodom_id(property_.otodom_id) is None:
                logger.info(f" Saved to Database: {property_.link}")
                property_ = PropertyService.put(property_)
                listing.property_ = property_
                self.listings.append(listing)

        except Exception as e:
            # If the apartment has weird JSON (like a developer project), just skip it!
            print(f"Failed to parse data for {property_.link} (Error: {e}). Skipping apartment.")
            return
        # --- NEW SAFETY NET END ---

    def try_get_listing_page(self, url: str) -> BeautifulSoup:
        import time
        import random
        """
        Tries to get the listing page.

        After 3 failures raises DataExtractionError

        :param url: The URL of the listing page
        :raises DataExtractionError: If the data extraction fails
        :return: The data from the listing
        """
        max_retries = 3
        while max_retries > 0:
            # 1. Add a random delay before opening the apartment page
            time.sleep(random.uniform(1.0,  2.5))

            try:
                response = self.session.get(
                    url=url,
                    timeout=15)

                soup = BeautifulSoup(response.content, "html.parser")

                if not PropertyDocument.informational_json_exists(soup):
                    logger.warning(f"Blocked or missing JSON on {url}. Retrying...")
                    time.sleep(3)  # Wait longer if we got blocked
                    max_retries -= 1
                    continue

                return soup

            except Exception as e:
                logger.warning(f"Connection error on {url}: {e}")
                time.sleep(3)
                max_retries -= 1

        raise DataExtractionError(url=url)

    def process_investment(self, investment_url: str):
        """
        Runs a nested crawl for an investment by reusing search-result logic.
        """
        original_base_url = self.settings.base_url
        original_params = self.params

        try:
            self.settings.base_url = investment_url
            self.params = {}

            # Reuse count_pages to find out how many pages of units exist
            total_pages, _ = self.count_pages()
            print(f"[INVESTMENT] Scaling out to {total_pages} pages of units...")

            for page in range(1, total_pages + 1):
                # Fetch unit items from the current investment page
                units = self.extract_listings_from_page(page, override_url=investment_url)

                formatted_units = []
                for u in units:
                    if u.get("url"):
                        # FIX: Use Constans.DEFAULT_URL to build the correct link
                        path = u['url']
                        full_url = f"{Constans.DEFAULT_URL}{path}" if path.startswith('/') else path
                        u["full_url"] = full_url
                        formatted_units.append(u)

                # FIX: Process the NEWLY found units, not the session history
                if formatted_units:
                    print(f"  -> Page {page}: Processing {len(formatted_units)} units...")
                    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                        list(executor.map(self.extract_listing_data, formatted_units))

                import time
                time.sleep(random.uniform(3.0, 7.0))

        finally:
            self.settings.base_url = original_base_url
            self.params = original_params
            print(f"[INVESTMENT] Completed sub-crawl. Returning to main search.")

    def to_csv_file(self, filename: str) -> None:
        """
        Saves the listings to a CSV file.

        :param filename: The name of the file
        """
        valid_listings = [listing for listing in self.listings if listing.property_ is not None]

        if not valid_listings:
            print("No new valid listings to save to CSV in this chunk.")
            return

        # UPDATE THIS: Use the filtered valid_listings instead of self.listings

        logger.info(f"Saving listings to {filename}. Format: csv")
        data = [listing.to_dict() for listing in valid_listings]

        with open(filename, "w", newline="", encoding="utf-8") as file:
            dict_writer = csv.DictWriter(file, Constans.CSV_KEYS)
            dict_writer.writeheader()
            dict_writer.writerows(data)

    def to_json_file(self, filename: str) -> None:
        """
        Saves the listings to a JSON file.

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

    def start(self, pages: int) -> None:
        """
        Starts the crawler by fetching one page, reading its apartments,
        and then moving to the next page.
        """
        existing_links = PropertyService.get_all_links()

        for page in range(1, pages + 1):
            if page % 15 == 0:
                self.rotate_session()
            # 1. Fetch ONE search page
            page_items = self.extract_listings_from_page(page)

            # 2. Filter the links for just this page
            valid_listings = []
            for item in page_items:
                slug = item.get("slug")
                if not slug:
                    continue
                full_url = f"{Constans.DEFAULT_URL}/pl/oferta/{slug}"

                if full_url not in existing_links:
                    item["full_url"] = full_url
                    valid_listings.append(item)

            if not valid_listings:
                print(f"Page {page} had no new listings. Moving to next page...")
                continue

            # 3. Read the apartments for just this page (This takes several minutes!)
            # This natural break completely refills the DataDome token bucket for the next search page.
            print(f"Processing {len(valid_listings)} new apartments from Page {page}...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                # We use list() to force the executor to finish before looping to the next search page
                list(executor.map(self.extract_listing_data, valid_listings))

            print(f"Finished Page {page}. Moving to next page...")
            delay = random.uniform(8.0, 15.0)
            print(f"Sleeping {delay:.2f}s before loading the next search page...")
            import time
            time.sleep(delay)

    def rotate_session(self):
        """Drops the current session cookies and generates a fresh browser fingerprint."""
        print("\n[ANTI-BOT] Rotating main crawler session to clear velocity history...")

        # Close the existing session
        if hasattr(self, 'session') and self.session:
            self.session.close()

        # Take a long breather to reset the IP trust score
        cooldown = random.uniform(35.0, 60.0)
        print(f"[ANTI-BOT] IP cooling down for {cooldown:.2f} seconds...")
        import time
        time.sleep(cooldown)

        # Start a brand new session with a modern browser profile
        self.session = requests.Session(impersonate="chrome120")
        print("[ANTI-BOT] New session acquired. Resuming scrape...\n")
