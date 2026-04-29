import time
import random
import logging
from bs4 import BeautifulSoup
from crawler.exceptions import DataExtractionError
from models import PropertyDocument, AgencyDocument
from services import PropertyService, AgencyService
from crawler.listing import Listing
from common import OfferedBy

logger = logging.getLogger(__name__)

class ListingProcessor:
    """
    Handles the orchestration of standard individual property listings found
    on the main search results pages.
    """
    def __init__(self, network, listings_list):
        """
        Initializes the listing processor with its parameters
        :param network: NetworkService instance
        :param listings_list: listings page list
        """
        self.network = network
        self.listings = listings_list

    def extract_listing_data(self, listing_data: dict) -> None:
        """
        Visits an individual listing page, parses the HTML, and categorizes it.

        If the listing belongs to an agency, it links the agency data. If the listing
        is a hidden developer investment, it adds the URL to found_investments.txt file
        for later processing with run_investments.py

        Args:
            listing_data (dict): The standard listing dictionary from the search page.
        """
        time.sleep(random.uniform(1.5, 4.0))
        listing = Listing()
        property_ = PropertyDocument()

        property_.link = listing_data["full_url"]
        property_.is_promoted = listing_data.get("isPromoted", False)
        print(f" Found apartment! Visiting: {property_.link}")

        try:
            soup = self.try_get_listing_page(url=property_.link)
            property_.extract_data(soup)

            if property_.offered_by == OfferedBy.ESTATE_AGENCY:
                self._process_agency(property_, soup, listing)

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

        except DataExtractionError as e:
            logger.exception(f"Failed to extract HTML from {property_.link}, Error: {e}")
        except Exception as e:
            print(f"Failed to parse data for {property_.link} (Error: {e}). Skipping apartment.")

    def try_get_listing_page(self, url: str) -> BeautifulSoup:
        """
        Attempts to fetch and parse the HTML of a listing page.

        Includes validation to ensure the page contains the expected informational JSON
        payload, raising an error if the page is malformed or blocked.

        Args:
            url (str): The specific listing URL to fetch.

        Returns:
            BeautifulSoup: The parsed HTML tree of the page.

        Raises:
            DataExtractionError: If the page cannot be fetched after retries or is missing expected JSON.
        """
        response = self.network.get(url=url, delay_range=(1.0, 2.5))
        if not response:
            raise DataExtractionError(url=url)

        soup = BeautifulSoup(response.content, "html.parser")
        if not PropertyDocument.informational_json_exists(soup):
            logger.warning(f"Missing JSON on {url}.")
            raise DataExtractionError(url=url)

        return soup
    @staticmethod
    def _process_agency(property_, soup, listing):
        """
        Extracts real estate agency data from a property page and links it to the property.

        Checks the database to prevent duplicate agency entries before establishing
        the Document Reference (DBRef).

        Args:
            property_ (PropertyDocument): The current property document being populated.
            soup (BeautifulSoup): The parsed HTML tree of the listing.
            listing (Listing): The local Listing object keeping track of crawler state.
        """
        agency = AgencyDocument()
        agency.extract_data(soup)
        agency_doc = AgencyService.get_by_otodom_id(agency.otodom_id)

        if agency_doc is None:
            agency_doc = AgencyService.put(agency)
        property_.estate_agency = agency_doc.to_dbref()
        listing.agency = agency_doc