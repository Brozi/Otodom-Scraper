import re
import json
import logging

logger = logging.getLogger(__name__)


class OtodomParser:
    """Handles all extraction of data from Otodom HTML and JSON."""

    @staticmethod
    def parse_page_count(html: str) -> tuple[int, int]:
        """Extracts the total pages and total listings from search results HTML."""
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if not match:
            logger.warning("Could not find __NEXT_DATA__ script tag.")
            return 0, 0

        try:
            data = json.loads(match.group(1))
            ad_data = data.get("props", {}).get("pageProps", {}).get("ad", {})

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
            logger.error(f"Error parsing pagination JSON: {e}")
            return 0, 0

    @staticmethod
    def parse_listings(html: str) -> list[dict]:
        """Extracts the list of apartment dictionaries from search results HTML."""
        marker = 'id="__NEXT_DATA__"'
        if marker not in html:
            return []

        try:
            tag_start = html.find(marker)
            json_start = html.find('>', tag_start) + 1
            json_end = html.find('</script>', json_start)

            json_text = html[json_start:json_end].strip()
            data = json.loads(json_text)

            ad_data = data.get("props", {}).get("pageProps", {}).get("ad", {})
            if "paginatedUnits" in ad_data:
                return ad_data["paginatedUnits"]["items"]
            else:
                return data["props"]["pageProps"]["data"]["searchAds"]["items"]

        except Exception as e:
            logger.warning(f"Error extracting items from JSON: {e}")
            return []