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
        self.investments_queue: set[str] = set()
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

    # UPDATE the function signature to accept override_url
    def count_pages(self, override_url: str = None) -> tuple[int, int] | None:
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

            # USE the override_url if provided, otherwise generate the normal search URL
            search_url = override_url if override_url else self.generate_search_url()

            response = self.session.get(url=search_url, params=self.params, timeout=20)
            html = response.text
            print(f"Status: {response.status_code}, Length: {len(html)}")
            if response.status_code in [403, 405, 429]:
                cooldown = random.uniform(600.0, 660.0)
                print(f"\nDATADOME BLOCK DETECTED! Sleeping {cooldown / 60:.2f}min to clear the penalty box... ")
                import time
                time.sleep(cooldown)
                self.session = requests.Session(impersonate="chrome120")  # Get fresh browser
                max_retries -= 1
                continue

            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(html)

            import re, json
            match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
            if match:
                try:
                    data = json.loads(match.group(1))

                    # Check if this is an investment page with paginatedUnits
                    ad_data = data["props"]["pageProps"].get("ad", {})
                    if "paginatedUnits" in ad_data:
                        listing_data = ad_data["paginatedUnits"]
                        page_count = listing_data["pagination"]["totalPages"]
                        item_count = listing_data["pagination"].get("totalResults", 0)
                    else:
                        # Standard search results
                        page_count = data["props"]["pageProps"]["tracking"]["listing"]["page_count"]
                        listing_data = data["props"]["pageProps"]["tracking"]["listing"]
                        item_count = listing_data.get("result_count", 0)

                    return int(page_count), int(item_count)

                except (KeyError, TypeError, ValueError) as e:
                    logger.error(f"Error parsing pagination JSON: {e}")
                    return 0, 0
            else:
                logger.warning(f"Could not find __NEXT_DATA__ script tag.")
                return 0, 0

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
                logger.info(f" Found hidden investment: {property_.link}")
                with open("found_investments.txt", "a", encoding="utf-8") as f:
                    f.write(property_.link + "\n")
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

    def process_investment_queue(self):
        """
        Processes all queued investments. Extracts Page 1 from the HTML JSON,
        and remaining pages via the Next.js Data API to bypass 404s.
        """
        if not self.investments_queue:
            return

        print(f"\n[INVESTMENT] Processing {len(self.investments_queue)} queued investments...")
        original_base_url = self.settings.base_url
        original_params = self.params.copy()

        # Convert to list so we can iterate safely
        processed_count = 0
        for investment_url in list(self.investments_queue):
            if processed_count > 0 and processed_count % 5 == 0:
                print(f"\n[INVESTMENT] Processed 5 investments. Forcing session rotation to avoid blocks...")
                self.rotate_session()

            processed_count += 1
            try:
                print(f"[INVESTMENT] Scraping: {investment_url}")
                import time, random
                time.sleep(random.uniform(2.0, 4.0))

                # 1. Fetch the HTML of the investment page directly
                response = self.session.get(investment_url, timeout=15)
                if response.status_code in [403, 405, 429]:
                    cooldown = random.uniform(600.0, 660.0)
                    logger.warning(f"DATADOME BLOCK on investment {investment_url}. Sleeping {cooldown / 60:.2f}m...")
                    time.sleep(cooldown)
                    self.session = requests.Session(impersonate="chrome120")
                    continue

                html = response.text

                import re, json
                match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
                if not match:
                    logger.warning(f"Could not find __NEXT_DATA__ on {investment_url}")
                    self.investments_queue.remove(investment_url)
                    continue

                data = json.loads(match.group(1))

                ad_data = data.get("props", {}).get("pageProps", {}).get("ad", {})

                # GRAB THE MAIN LOCATION HERE
                main_location = ad_data.get("location", {})
                seller_type = ad_data.get("target", {}).get("user_type", {})
                if seller_type == "developer":
                    developer_id = ad_data.get("target", {}).get("seller_id", {})
                else:
                    developer_id = None

                if "paginatedUnits" not in ad_data:
                    logger.warning(f"No paginatedUnits found for {investment_url}. Skipping.")
                    self.investments_queue.remove(investment_url)
                    continue

                paginated_units = ad_data["paginatedUnits"]
                total_pages = paginated_units.get("pagination", {}).get("totalPages", 1)
                items_page_1 = paginated_units.get("items", [])

                # --- NEW STEALTH BLOCK CHECK ---
                if items_page_1 and any(unit.get("target") is None for unit in items_page_1):
                    logger.warning(f"Stealth block detected on {investment_url}. Sleeping 5 minutes...")
                    import time
                    time.sleep(300)
                    self.session = requests.Session(impersonate="chrome120")
                    continue  # Skips the rest of the loop, leaving the URL in the queue to try again!
                # -------------------------------

                print(f"  -> Found {total_pages} pages of units.")
                # Automatically determine the page size based on Page 1
                dynamic_page_size = len(items_page_1) if items_page_1 else 6

                # Pass main_location to Page 1 units
                for unit_dict in items_page_1:
                    self.extract_unit_from_json(unit_dict, investment_url, main_location, developer_id)

                # 3. If there are more pages, fetch them using the Apollo Persisted Query API
                # Note: We don't need build_id anymore, just the investment_id (ad_data['id'])
                investment_id = ad_data.get("id")

                if total_pages > 1 and investment_id:
                    print(f"  -> Using APQ Data API for pages 2-{total_pages}...")

                    page = 2
                    while page <= total_pages:
                        import time, random
                        import json

                        time.sleep(random.uniform(2.5, 4.5))

                        # Build the exact variables JSON required by their server
                        variables = {
                            "id": int(investment_id),
                            "lookup": {
                                "filters": {},
                                "page": page,
                                "pageSize": dynamic_page_size,  # <--- Uses the exact size from Page 1
                                "sort": {"by": "Price", "direction": "asc"},
                                "withFacets": True
                            }
                        }

                        # APQ Hash matching "PaginatedInvestmentUnits"
                        extensions = {
                            "persistedQuery": {
                                "sha256Hash": "ddc9f328a32057395caf18ef667d3ee4242ea57e73481cc8a56ee9618d0c2b31",
                                "version": 1
                            }
                        }

                        params = {
                            "operationName": "PaginatedInvestmentUnits",
                            "variables": json.dumps(variables, separators=(',', ':')),
                            "extensions": json.dumps(extensions, separators=(',', ':'))
                        }

                        headers = {
                            "Accept": "*/*",
                            "Referer": investment_url,
                        }

                        logger.info(f"Fetching API page {page} for {investment_url}")
                        next_res = self.session.get(
                            f"{self.settings.base_url}/api/query",
                            params=params,
                            headers=headers,
                            timeout=15
                        )

                        if next_res.status_code == 200:
                            try:
                                next_data = next_res.json()

                                # 1. Print GraphQL errors if the server rejected our request
                                if "errors" in next_data:
                                    logger.error(f"GraphQL Errors on page {page}: {next_data['errors']}")
                                    # --- APQ CACHE MISS RETRY LOGIC ---
                                    error_code = next_data["errors"][0].get("extensions", {}).get("code")
                                    if error_code == "PERSISTED_QUERY_NOT_FOUND":
                                        logger.warning(
                                            "Otodom server forgot the GraphQL hash. Retrying in 10 seconds...")
                                        import time
                                        time.sleep(10)
                                        continue  # Loops back to retry the EXACT SAME page number
                                    # ----------------------------------

                                # 2. Safely extract data (using 'or {}' handles null/None values)
                                data_block = next_data.get("data") or {}
                                paginated_units = data_block.get("paginatedUnits") or {}
                                next_items = paginated_units.get("items") or []

                                # --- NEW STEALTH BLOCK CHECK ---
                                if next_items and any(unit.get("target") is None for unit in next_items):
                                    logger.warning(f"Stealth API block on page {page}. Sleeping 5 minutes...")
                                    time.sleep(300)
                                    self.session = requests.Session(impersonate="chrome120")
                                    continue  # Loops back to retry the EXACT SAME page number
                                # -------------------------------

                                print(f"     API: Page {page} retrieved {len(next_items)} units.")

                                saved_count = 0
                                for unit_dict in next_items:
                                    # Your existing extract_unit_from_json takes (unit_dict, investment_url)
                                    was_saved = self.extract_unit_from_json(unit_dict, investment_url, main_location, developer_id)
                                    if was_saved:
                                        saved_count += 1

                                print(f" Page {page}: saved {saved_count}/{len(next_items)} units")
                                page += 1  # SUCCESS: move to the next page
                            except Exception as e:
                                    logger.error(f"Error parsing API JSON on page {page}: {e}")
                                    page += 1  # Skip broken page to avoid infinite loop
                        else:
                            logger.warning(f"API returned status {next_res.status_code} for page {page}.")
                            if next_res.status_code in [403, 405, 429]:
                                cooldown = random.uniform(600.0, 660.0)
                                logger.warning(f"DATADOME BLOCK on API. Sleeping {cooldown / 60:.2f}m...")
                                time.sleep(cooldown)
                                self.session = requests.Session(impersonate="chrome120")
                                continue  # Retry the EXACT SAME page number
                            page += 1

                # Remove from queue once successfully processed
                with open("scraped_investments.txt", "a", encoding="utf-8") as f:
                    f.write(investment_url + "\n")
                self.investments_queue.remove(investment_url)
                time.sleep(random.uniform(3.0, 7.0))

            except Exception as e:
                logger.error(f"[INVESTMENT] Failed to process {investment_url}: {e}")

        # Restore original crawler state
        self.settings.base_url = original_base_url
        self.params = original_params
        print(f"[INVESTMENT] Finished processing queue.\n")

    def extract_unit_from_json(self, unit_dict: dict, investment_url: str, main_location: dict = None, developer_id: int = None):
        """
        Maps a unit's JSON dictionary directly to a PropertyDocument and saves it.
        Uses main_location from the parent investment to fill in missing street/district data.
        """
        from models.property import PropertyDocument
        from services.property import PropertyService
        from common.constans import Constans, OfferedBy, PropertyType, MarketType, AuctionType
        import datetime
        import re

        path = unit_dict.get("url", "")
        full_url = f"{Constans.DEFAULT_URL}{path}" if path.startswith("/") else path

        raw_id = (
                unit_dict.get("id") or unit_dict.get("adId") or
                unit_dict.get("externalId") or unit_dict.get("target", {}).get("Id")
        )

        if not raw_id and full_url:
            m = re.search(r"(ID[0-9A-Za-z]+)$", full_url)
            raw_id = m.group(1) if m else None

        if not raw_id:
            return False

        otodom_id = str(raw_id)
        if PropertyService.get_by_otodom_id(int(otodom_id)):
            return False

        try:
            property_ = PropertyDocument()
            property_.link = full_url
            property_.otodom_id = otodom_id
            property_.created_at = datetime.datetime.now()
            property_.title = unit_dict.get('title', 'Developer Unit')

            if developer_id:
                property_.developer_id = int(developer_id)

            # --- TARGET DICT EXTRACTION ---
            target_data = unit_dict.get("target", {})

            area_val = target_data.get('Area', 0.0)
            property_.area = float(area_val) if area_val else 0.0

            rooms_list = target_data.get('Rooms_num', [])
            property_.rooms = str(rooms_list[0]) if rooms_list else ''

            property_.price = target_data.get('Price')
            property_.price_per_meter = target_data.get('Price_per_m')

            # --- EXTRAS & SECURITY ---
            extras_list = target_data.get("Extras_types", [])
            if extras_list:
                property_.extras = ", ".join(extras_list)

            security_list = target_data.get("Security_types", [])
            if security_list:
                property_.security_types = ", ".join(security_list)

            floor_list = target_data.get("Floor_no", [])
            if floor_list:
                property_.floor = str(floor_list[0]).replace("floor_", "").replace("ground_floor", "0")

            # --- BUILDING EXTRACTION ---
            from models.building import BuildingDocument
            building = BuildingDocument()
            building.build_year = target_data.get("Build_year")

            b_types = target_data.get("Building_type", [])
            building.type = b_types[0] if b_types else None  # Mapped to 'type'

            b_floors = target_data.get("Building_floors_num")
            building.floors = int(b_floors) if b_floors else None  # Mapped to 'floors'

            b_ownership = target_data.get("Building_ownership", [])
            building.ownership = b_ownership[0] if b_ownership else None

            property_.building = building

            # --- STATUS & STATIC DEFAULTS ---
            property_.offered_by = OfferedBy.DEVELOPER_UNIT
            property_.market_type = MarketType.PRIMARY
            property_.auction_type = AuctionType.SALE
            property_.property_type = PropertyType.FLAT

            status_list = target_data.get("Construction_status")
            if status_list and isinstance(status_list, list) and len(status_list) > 0:
                from common.constans import ConstructionStatus
                try:
                    property_.construction_status = ConstructionStatus(status_list[0])
                except ValueError:
                    pass

                    # --- PHOTOS ---
            images = unit_dict.get("images", [])
            photo_urls = [img.get("large") or img.get("medium") or img.get("small") for img in images]
            property_.photos = ", ".join(filter(None, photo_urls))
            property_.description = unit_dict.get("description", "Brak opisu (oferta deweloperska).")

            # --- LOCALIZATION EXTRACTION (USING PARENT INVESMENT DATA) ---
            from models.localization import LocalizationDocument
            loc = LocalizationDocument()

            # 1. Start with the main investment location if provided
            if main_location and isinstance(main_location, dict):
                address = main_location.get("address") or {}
                loc.street = (address.get("street") or {}).get("name")
                loc.district = (address.get("district") or {}).get("name")
                loc.city = (address.get("city") or {}).get("name")
                loc.county = (address.get("county") or {}).get("name")
                loc.province = (address.get("province") or {}).get("name")
            else:
                loc.city = self.settings.city
                loc.province = self.settings.province
                loc.district = self.settings.district

            # 2. Override with unit specific target data if any exists
            loc.province = target_data.get('Province', loc.province)
            loc.city = target_data.get('City', loc.city)

            # 'Subregion' in Otodom target dict is the County (e.g. 'powiat-krakow')
            county_raw = target_data.get("Subregion")
            if county_raw:
                loc.county = county_raw.replace("powiat-", "").capitalize()

            # 3. GPS Coordinates
            unit_loc = unit_dict.get('location', {})
            coordinates = unit_loc.get('coordinates', {})
            if coordinates:
                loc.latitude = float(coordinates.get('latitude', coordinates.get('lat', 0.0)))
                loc.longitude = float(coordinates.get('longitude', coordinates.get('lon', 0.0)))
            elif main_location and main_location.get("coordinates"):
                # Fallback to parent coordinates
                loc.latitude = float(main_location["coordinates"].get("latitude", 0.0))
                loc.longitude = float(main_location["coordinates"].get("longitude", 0.0))

            property_.localization = loc

            import logging
            logger.info(f" Saved Unit directly from JSON: {property_.link}")
            from crawler.listing import Listing
            listing = Listing()
            listing.property_ = property_
            self.listings.append(listing)
            PropertyService.put(property_)
            return True

        except Exception as e:
            import logging
            logger.error(f"Failed to map JSON for unit {full_url}: {e}")
            return False

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
