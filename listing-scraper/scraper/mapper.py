import re
import datetime
from models.property import PropertyDocument
from models.building import BuildingDocument
from models.localization import LocalizationDocument
from common.constans import Constans, OfferedBy, PropertyType, MarketType, AuctionType, ConstructionStatus


class OtodomUnitMapper:
    """
    Translates raw Otodom API JSON into clean PropertyDocument domain models.
    """

    @staticmethod
    def map_investment_unit(unit_dict: dict, main_location: dict, settings) -> PropertyDocument:
        # 1. Resolve URL and ID
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

        # 2. Initialize the Domain Model
        property_ = PropertyDocument()
        property_.link = full_url
        property_.otodom_id = str(raw_id)
        property_.created_at = datetime.datetime.now()
        property_.title = unit_dict.get('title', 'Developer Unit')

        target_data = unit_dict.get("target", {})

        # 3. Map basic specs
        area_val = target_data.get('Area', 0.0)
        property_.area = float(area_val) if area_val else 0.0

        rooms_list = target_data.get('Rooms_num', [])
        property_.rooms = str(rooms_list[0]) if rooms_list else ''

        property_.price = target_data.get('Price')
        property_.price_per_meter = target_data.get('Price_per_m')

        floor_list = target_data.get("Floor_no", [])
        if floor_list:
            property_.floor = str(floor_list[0]).replace("floor_", "").replace("ground_floor", "0")

        # 4. Map extras
        extras_list = target_data.get("Extras_types", [])
        if extras_list:
            property_.extras = ", ".join(extras_list)

        security_list = target_data.get("Security_types", [])
        if security_list:
            property_.security_types = ", ".join(security_list)

        # 5. Map Building
        building = BuildingDocument()
        building.build_year = target_data.get("Build_year")
        b_types = target_data.get("Building_type", [])
        building.type = b_types[0] if b_types else None
        b_floors = target_data.get("Building_floors_num")
        building.floors = int(b_floors) if b_floors else None
        b_ownership = target_data.get("Building_ownership", [])
        building.ownership = b_ownership[0] if b_ownership else None
        property_.building = building

        # 6. Map constants and status
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

                # 7. Map media
        images = unit_dict.get("images", [])
        photo_urls = [img.get("large") or img.get("medium") or img.get("small") for img in images]
        property_.photos = ", ".join(filter(None, photo_urls))
        property_.description = unit_dict.get("description", "Brak opisu (oferta deweloperska).")

        # 8. Map Localization
        loc = LocalizationDocument()
        if main_location and isinstance(main_location, dict):
            address = main_location.get("address", {})
            loc.street = address.get("street", {}).get("name")
            loc.district = address.get("district", {}).get("name")
            loc.city = address.get("city", {}).get("name")
            loc.county = address.get("county", {}).get("name")
            loc.province = address.get("province", {}).get("name")
        else:
            loc.city = settings.city
            loc.province = settings.province
            loc.district = settings.district

        loc.province = target_data.get('Province', loc.province)
        loc.city = target_data.get('City', loc.city)

        county_raw = target_data.get("Subregion")
        if county_raw:
            loc.county = county_raw.replace("powiat-", "").capitalize()

        unit_loc = unit_dict.get('location', {})
        coordinates = unit_loc.get('coordinates', {})
        if coordinates:
            loc.latitude = float(coordinates.get('latitude', coordinates.get('lat', 0.0)))
            loc.longitude = float(coordinates.get('longitude', coordinates.get('lon', 0.0)))
        elif main_location and main_location.get("coordinates"):
            loc.latitude = float(main_location["coordinates"].get("latitude", 0.0))
            loc.longitude = float(main_location["coordinates"].get("longitude", 0.0))

        property_.localization = loc

        return property_