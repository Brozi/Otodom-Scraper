import logging
import json
from bs4 import BeautifulSoup
from services.property import PropertyService
from scraper.mapper import OtodomUnitMapper

logger = logging.getLogger(__name__)


class InvestmentScraper:
    """
    Handles scraping developer investment pages and fetching paginated units
    via the Otodom Apollo GraphQL API.
    """

    def __init__(self, network_service, settings):
        self.network = network_service
        self.settings = settings

    def process_queue(self, investments_queue: set):
        """Processes all queued developer investment URLs."""
        if not investments_queue:
            return

        logger.info(f"\n--- Starting Investment Processing ({len(investments_queue)} investments) ---")

        for investment_url in list(investments_queue):
            logger.info(f"Processing Investment: {investment_url}")
            try:
                # 1. Fetch Page 1 to get the initial JSON and total pages
                response = self.network.get(investment_url)
                if response.status_code != 200:
                    logger.error(f"Failed to load investment page: {investment_url}")
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                next_data_script = soup.find("script", id="__NEXT_DATA__")

                if not next_data_script:
                    logger.warning(f"No __NEXT_DATA__ found for {investment_url}")
                    continue

                data = json.loads(next_data_script.string)
                ad_data = data.get("props", {}).get("pageProps", {}).get("ad", {})

                # Extract main location for fallback
                main_location = ad_data.get("location", {})

                if "paginatedUnits" not in ad_data:
                    logger.warning(f"No paginatedUnits found. Skipping.")
                    continue

                paginated_units = ad_data["paginatedUnits"]
                total_pages = paginated_units.get("pagination", {}).get("totalPages", 1)
                items_page_1 = paginated_units.get("items", [])
                investment_id = ad_data.get("id")

                logger.info(f"  -> Found {total_pages} pages of units.")

                # 2. Process Page 1 Units
                self._map_and_save_units(items_page_1, main_location)

                # 3. Fetch Pages 2+ using GraphQL APQ
                if total_pages > 1 and investment_id:
                    self._fetch_remaining_pages(investment_id, total_pages, main_location)

            except Exception as e:
                logger.error(f"Error processing investment {investment_url}: {e}")

        logger.info("--- Finished Investment Processing ---\n")

    def _fetch_remaining_pages(self, investment_id, total_pages, main_location):
        """Fetches units from page 2 onwards using Otodom's GraphQL API."""
        graphql_url = "https://www.otodom.pl/graphql"

        for page in range(2, total_pages + 1):
            variables = {
                "id": int(investment_id),
                "lookup": {
                    "filters": {"numberOfRooms": []},
                    "page": page,
                    "pageSize": 36
                }
            }

            params = {
                "operationName": "PaginatedInvestmentUnits",
                "variables": json.dumps(variables),
                "extensions": json.dumps({
                    "persistedQuery": {
                        "version": 1,
                        "sha256Hash": "f8a7ab2913b8dd42ec5d2b77cfc709e3bb8df74dd7e7b51b2eab91e13abcb0bc"
                    }
                }),
                "page": page  # Custom parameter for logging in NetworkService
            }

            headers = {
                "accept": "*/*",
                "content-type": "application/json",
            }

            try:
                # Our NetworkService automatically handles 403s and retries!
                next_res = self.network.get(graphql_url, params=params, headers=headers)

                if next_res.status_code == 200:
                    next_data = next_res.json()

                    if "errors" in next_data:
                        logger.error(f"GraphQL Errors on page {page}: {next_data['errors']}")
                        continue

                    data_block = next_data.get("data") or {}
                    paginated_units = data_block.get("paginatedUnits") or {}
                    next_items = paginated_units.get("items") or []

                    logger.info(f"     API: Page {page} retrieved {len(next_items)} units.")
                    self._map_and_save_units(next_items, main_location)

            except Exception as e:
                logger.error(f"Error parsing API JSON on page {page}: {e}")

    def _map_and_save_units(self, items, main_location):
        """Passes raw dicts to the Mapper and saves the resulting PropertyDocuments."""
        saved_count = 0
        for unit_dict in items:
            # 1. Map JSON to Domain Model
            property_doc = OtodomUnitMapper.map_investment_unit(unit_dict, main_location, self.settings)

            # 2. Check if valid and not a duplicate, then save
            if property_doc and not PropertyService.get_by_otodom_id(int(property_doc.otodom_id)):
                PropertyService.put(property_doc)
                saved_count += 1

        if items:
            logger.info(f"     Saved {saved_count}/{len(items)} units")