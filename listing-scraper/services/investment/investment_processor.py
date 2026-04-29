import time
import random
import json
import re
import logging
from crawler.exceptions import DataExtractionError
from services.investment import InvestmentMapper
from crawler.listing import Listing

logger = logging.getLogger(__name__)


class InvestmentProcessor:
    """
    Handles the orchestration of extracting bulk developer investments.

    This includes fetching the initial Next.js HTML payloads, extracting hidden JSON,
    and paginating through the Apollo Persisted Query (APQ) GraphQL API while
    navigating stealth blocks.
    """
    def __init__(self, network, settings, listings_list):
        self.network = network
        self.settings = settings
        self.listings = listings_list  # Reference to the main crawler.listings list

    def process_queue(self, investments_queue: set[str]):
        """
        Iterates through the discovered investment URLs, triggering extraction for each.

        Forces a network session rotation every 5 investments to avoid rate limits.

        Args:
            investments_queue (set[str]): A set of investment URLs to process.
        """
        if not investments_queue:
            return

        print(f"\n[INVESTMENT] Processing {len(investments_queue)} queued investments...")
        processed_count = 0

        for investment_url in list(investments_queue):
            if processed_count > 0 and processed_count % 5 == 0:
                print(f"\n[INVESTMENT] Processed 5 investments. Forcing session rotation...")
                self.network.rotate_session()

            processed_count += 1
            try:
                self._process_single_investment(investment_url, investments_queue)
                time.sleep(random.uniform(3.0, 7.0))
            except Exception as e:
                logger.error(f"[INVESTMENT] Failed to process {investment_url}: {e}")

        print(f"[INVESTMENT] Finished processing queue.\n")

    def _process_single_investment(self, investment_url: str, queue: set[str]):
        """
        Processes a single investment page by extracting the embedded Next.js JSON.

        If paginated units are found, it processes the first page and delegates
        subsequent pages to the API fetcher.

        Args:
            investment_url (str): The URL of the developer project.
            queue (set[str]): The queue containing the URL, used to remove it upon completion.
        """
        print(f"[INVESTMENT] Scraping: {investment_url}")
        time.sleep(random.uniform(2.0, 4.0))

        response = self.network.get(investment_url, timeout=15)
        if not response:
            raise DataExtractionError(url=investment_url)

        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', response.text, re.DOTALL)
        if not match:
            logger.warning(f"Could not find __NEXT_DATA__ on {investment_url}")
            queue.remove(investment_url)
            return

        data = json.loads(match.group(1))
        ad_data = data.get("props", {}).get("pageProps", {}).get("ad", {})

        main_location = ad_data.get("location", {})
        seller_type = ad_data.get("target", {}).get("user_type", {})
        developer_id = ad_data.get("target", {}).get("seller_id") if seller_type == "developer" else None

        if "paginatedUnits" not in ad_data:
            queue.remove(investment_url)
            return

        paginated_units = ad_data["paginatedUnits"]
        total_pages = paginated_units.get("pagination", {}).get("totalPages", 1)
        items_page_1 = paginated_units.get("items", [])

        if items_page_1 and any(unit.get("target") is None for unit in items_page_1):
            logger.warning(f"Stealth block detected on {investment_url}. Sleeping 5 minutes...")
            time.sleep(300)
            self.network.rotate_session()
            return

        print(f"  -> Found {total_pages} pages of units.")
        dynamic_page_size = len(items_page_1) if items_page_1 else 6

        # Process Page 1
        for unit_dict in items_page_1:
            self._save_unit(unit_dict, investment_url, main_location, developer_id)

        investment_id = ad_data.get("id")
        if total_pages > 1 and investment_id:
            self._fetch_api_pages(investment_url, investment_id, total_pages, dynamic_page_size, main_location,
                                  developer_id)

        with open("scraped_investments.txt", "a", encoding="utf-8") as f:
            f.write(investment_url + "\n")
        queue.remove(investment_url)

    def _fetch_api_pages(self, investment_url, investment_id, total_pages, page_size, main_location, developer_id):
        """
        Paginates through remaining investment units using Otodom's internal GraphQL API.

        Constructs the required APQ hashes and handles server-side cache misses
        (PERSISTED_QUERY_NOT_FOUND) and DataDome stealth blocks.

        Args:
            investment_url (str): The referer URL for headers.
            investment_id (int): The internal ID of the developer project.
            total_pages (int): The total number of pages to iterate through.
            page_size (int): The dynamic page size calculated from the first page.
            main_location (dict): The fallback location dictionary to pass to the mapper.
            developer_id (int): The Otodom seller ID.
        """
        print(f"  -> Using APQ Data API for pages 2-{total_pages}...")
        page = 2
        while page <= total_pages:
            variables = {
                "id": int(investment_id),
                "lookup": {
                    "filters": {}, "page": page, "pageSize": page_size,
                    "sort": {"by": "Price", "direction": "asc"}, "withFacets": True
                }
            }
            extensions = {
                "persistedQuery": {"sha256Hash": "ddc9f328a32057395caf18ef667d3ee4242ea57e73481cc8a56ee9618d0c2b31",
                                   "version": 1}}
            params = {
                "operationName": "PaginatedInvestmentUnits",
                "variables": json.dumps(variables, separators=(',', ':')),
                "extensions": json.dumps(extensions, separators=(',', ':'))
            }
            headers = {"Accept": "*/*", "Referer": investment_url}

            logger.info(f"Fetching API page {page} for {investment_url}")
            next_res = self.network.get(f"{self.settings.base_url}/api/query", params=params, headers=headers,
                                        delay_range=(2.5, 4.5))

            if not next_res:
                page += 1
                continue

            try:
                next_data = next_res.json()
                if "errors" in next_data:
                    error_code = next_data["errors"][0].get("extensions", {}).get("code")
                    if error_code == "PERSISTED_QUERY_NOT_FOUND":
                        time.sleep(10)
                        continue

                data_block = next_data.get("data") or {}
                paginated_units = data_block.get("paginatedUnits") or {}
                next_items = paginated_units.get("items") or []

                if next_items and any(unit.get("target") is None for unit in next_items):
                    time.sleep(300)
                    self.network.rotate_session()
                    continue

                saved_count = 0
                for unit_dict in next_items:
                    if self._save_unit(unit_dict, investment_url, main_location, developer_id):
                        saved_count += 1
                print(f" Page {page}: saved {saved_count}/{len(next_items)} units")
                page += 1
            except Exception as e:
                logger.error(f"Error parsing API JSON on page {page}: {e}")
                page += 1

    def _save_unit(self, unit_dict, investment_url, main_location, developer_id):
        """
        A helper function that passes raw unit data to the InvestmentMapper and
        appends valid results to the crawler's main listings state.

        Args:
            unit_dict (dict): Raw JSON unit dictionary.
            investment_url (str): The parent project URL.
            main_location (dict): The parent project's location.
            developer_id (int): The Otodom seller ID.

        Returns:
            bool: True if the unit was successfully mapped and saved, False otherwise.
        """
        property_ = InvestmentMapper.map_investment_unit(
            unit_dict, investment_url, main_location, developer_id,
            self.settings.city, self.settings.province, self.settings.district
        )
        if property_:
            listing = Listing()
            listing.property_ = property_
            self.listings.append(listing)
            return True
        return False