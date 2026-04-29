from models.property import PropertyDocument
from models.building import BuildingDocument
from models.localization import LocalizationDocument
from common.constans import Constans, OfferedBy, PropertyType, MarketType, AuctionType, ConstructionStatus
from services.property import PropertyService
import logging
import re
import datetime

logger = logging.getLogger(__name__)


class InvestmentMapper:
    """
    Responsible for mapping raw JSON dictionary payloads from the Next.js API
    into application-specific MongoDB Document models.

    This class is pure and does not make network requests or maintain state.
    """
    @staticmethod
    def map_investment_unit(unit_dict: dict, investment_url: str, main_location: dict = None, developer_id: int = None,
                            default_city: str = "", default_province: str = "",
                            default_district: str = "") -> PropertyDocument | None:
        """
        Maps a single unit's JSON dictionary to a PropertyDocument.

        Uses the main_location from the parent investment to fill in missing
        street or district data if the unit lacks specific localization.

        Args:
            unit_dict (dict): The raw JSON dictionary representing the apartment unit.
            investment_url (str): The URL of the parent developer investment.
            main_location (dict, optional): The overarching location dict of the developer project.
            developer_id (int, optional): The Otodom seller/developer ID.
            default_city (str, optional): Fallback city from crawler settings.
            default_province (str, optional): Fallback province from crawler settings.
            default_district (str, optional): Fallback district from crawler settings.

        Returns:
            PropertyDocument | None: The fully mapped MongoDB document, or None if mapping fails or document already exists.
        """
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
            return None

        otodom_id = str(raw_id)
        if PropertyService.get_by_otodom_id(int(otodom_id)):
            return None

        try:
            property_ = PropertyDocument()
            property_.link = full_url
            property_.otodom_id = otodom_id
            property_.created_at = datetime.datetime.now()
            property_.title = unit_dict.get('title', 'Developer Unit')

            if developer_id:
                property_.developer_id = int(developer_id)

            target_data = unit_dict.get("target", {})
            area_val = target_data.get('Area', 0.0)
            property_.area = float(area_val) if area_val else 0.0

            rooms_list = target_data.get('Rooms_num', [])
            property_.rooms = str(rooms_list[0]) if rooms_list else ''

            property_.price = target_data.get('Price')
            property_.price_per_meter = target_data.get('Price_per_m')

            extras_list = target_data.get("Extras_types", [])
            if extras_list: property_.extras = ", ".join(extras_list)

            security_list = target_data.get("Security_types", [])
            if security_list: property_.security_types = ", ".join(security_list)

            heating_list = target_data.get("Heating", [])
            if heating_list: property_.heating = ", ".join(heating_list)

            floor_list = target_data.get("Floor_no", [])
            if floor_list: property_.floor = str(floor_list[0]).replace("floor_", "").replace("ground_floor", "0")

            property_.building = InvestmentMapper._map_building(target_data)

            property_.offered_by = OfferedBy.DEVELOPER_UNIT
            property_.market_type = MarketType.PRIMARY
            property_.auction_type = AuctionType.SALE
            property_.property_type = PropertyType.FLAT

            status_list = target_data.get("Construction_status")
            if status_list and isinstance(status_list, list) and len(status_list) > 0:
                try:
                    property_.construction_status = ConstructionStatus(status_list[0])
                except ValueError:
                    pass

            images = unit_dict.get("images", [])
            photo_urls = [img.get("large") or img.get("medium") or img.get("small") for img in images]
            property_.photos = ", ".join(filter(None, photo_urls))
            property_.description = unit_dict.get("description", "Brak opisu (oferta deweloperska).")

            property_.localization = InvestmentMapper._map_localization(
                target_data, unit_dict, main_location, default_city, default_province, default_district
            )

            logger.info(f" Saved Unit directly from JSON: {property_.link}")
            PropertyService.put(property_)
            return property_

        except Exception as e:
            logger.error(f"Failed to map JSON for unit {full_url}: {e}")
            return None

    @staticmethod
    def _map_building(target_data: dict) -> BuildingDocument:
        """
        Extracts building-specific metadata from the target dictionary.

        Args:
            target_data (dict): The 'target' dictionary from the Otodom unit JSON.

        Returns:
            BuildingDocument: A populated document containing building year, type, floors, etc.
        """
        building = BuildingDocument()
        building.build_year = target_data.get("Build_year")
        b_types = target_data.get("Building_type", [])
        building.type = b_types[0] if b_types else None
        b_floors = target_data.get("Building_floors_num")
        building.floors = int(b_floors) if b_floors else None
        b_ownership = target_data.get("Building_ownership", [])
        building.ownership = b_ownership[0] if b_ownership else None
        return building

    @staticmethod
    def _map_localization(target_data: dict, unit_dict: dict, main_location: dict, default_city: str,
                          default_province: str, default_district: str) -> LocalizationDocument:
        """
        Calculates the most accurate geographical location for a unit.

        Prioritizes exact unit coordinates, falls back to the main investment project's
        coordinates, and lastly falls back to the crawler's default search settings.

        Args:
            target_data (dict): The 'target' dictionary from the Otodom unit JSON.
            unit_dict (dict): The root unit JSON dictionary containing coordinates.
            main_location (dict): The main investment's location dictionary.
            default_city (str): Fallback city from crawler settings.
            default_province (str): Fallback province from crawler settings.
            default_district (str): Fallback district from crawler settings.

        Returns:
            LocalizationDocument: The fully resolved location with GPS coordinates.
        """

        loc = LocalizationDocument()
        if main_location and isinstance(main_location, dict):
            address = main_location.get("address") or {}
            loc.street = (address.get("street") or {}).get("name")
            loc.district = (address.get("district") or {}).get("name")
            loc.city = (address.get("city") or {}).get("name")
            loc.county = (address.get("county") or {}).get("name")
            loc.province = (address.get("province") or {}).get("name")
        else:
            loc.city = default_city
            loc.province = default_province
            loc.district = default_district

        loc.province = target_data.get('Province', loc.province)
        loc.city = target_data.get('City', loc.city)

        county_raw = target_data.get("Subregion")
        if county_raw: loc.county = county_raw.replace("powiat-", "").capitalize()

        unit_loc = unit_dict.get('location', {})
        coordinates = unit_loc.get('coordinates', {})
        if coordinates:
            loc.latitude = float(coordinates.get('latitude', coordinates.get('lat', 0.0)))
            loc.longitude = float(coordinates.get('longitude', coordinates.get('lon', 0.0)))
        elif main_location and main_location.get("coordinates"):
            loc.latitude = float(main_location["coordinates"].get("latitude", 0.0))
            loc.longitude = float(main_location["coordinates"].get("longitude", 0.0))

        if loc.longitude and loc.latitude:
            loc.location = [loc.longitude, loc.latitude]

        return loc
