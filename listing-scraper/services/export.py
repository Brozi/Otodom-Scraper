import csv
import json
import logging
from common import Constans
from common import flatten_dict

logger = logging.getLogger(__name__)

class ExportService:
    @staticmethod
    def to_csv_file(listings: list, filename: str) -> None:
        """
        Saves the listings to a CSV file.

        :param filename: The name of the file
        :param listings: Listings to save
        """
        valid_listings = [listing for listing in listings if listing.property_ is not None]
        if not valid_listings:
            print("No new valid listings to save to CSV in this chunk.")
            return

        logger.info(f"Saving listings to {filename}. Format: csv")
        data = [listing.to_dict() for listing in valid_listings]

        with open(filename, "w", newline="", encoding="utf-8") as file:
            dict_writer = csv.DictWriter(file, Constans.CSV_KEYS)
            dict_writer.writeheader()
            dict_writer.writerows(data)

    @staticmethod
    def to_json_file(listings: list, filename: str) -> None:
        """
        Saves the listings to a JSON file.

        :param filename: The name of the file
        :param listings: Listings to save
        """
        logger.info(f"Saving listings to {filename}. Format: json")
        with open(filename, "w", encoding="utf-8") as file:
            json.dump(
                [listing.to_dict() for listing in listings],
                file, ensure_ascii=False, default=str, indent=4
            )
    @staticmethod
    def to_excel_file(filename: str) -> None:
        from pandas import read_csv
        """
        Saves the listings to a JSON file.

        :param filename: The name of the file
        :param listings: Listings to save
        """

        logger.info(f"Saving listings to {filename}. Format: xlsx")
        try:

            df = read_csv(filename,encoding="utf-8")
            df.to_excel(filename, index=False)
        except FileNotFoundError:
            filename = filename.strip(".xlsx")
            logger.error(f"Cannot export to Excel: The root file {filename} was not found.")
            print(f"Error: The file {filename} was not found.")
    @staticmethod
    def db_to_json_file(filename: str, include_agencies: bool = False) -> None:
        """
        Saves the properties in the database to a json file.

        :param filename: The name of the file
        :param include_agencies: Whether to include agencies in the json file
        """
        from services.property import PropertyService
        from services.agency import AgencyService
        from common.utils import flatten_dict
        logger.info(f"Saving properties to {filename}. Format: json")
        properties = PropertyService.get_all()
        properties = [property_.to_mongo().to_dict() for property_ in properties]

        if include_agencies:
            logger.info("Including agencies in the json file")
            agencies = AgencyService.get_all()
            agencies = [agency.to_mongo().to_dict() for agency in agencies]
            for property_ in properties:
                estate_agency = property_.get("estate_agency")
                if estate_agency is not None:
                    agency_id = str(estate_agency)
                    for agency in agencies:
                        if str(agency["_id"]) == agency_id:
                            property_["agency"] = agency
                            property_.pop("estate_agency")
                            break

        with open(filename, "w", encoding="utf-8") as file:
            json.dump(
                [flatten_dict(property_) for property_ in properties],
                file,
                ensure_ascii=False,
                default=str,
                indent=4,
            )

    @classmethod
    def db_to_csv_file(cls, filename: str, include_agencies: bool = False) -> None:
        """
        Saves the properties in the database to a csv file.

        :param filename: The name of the file
        :param include_agencies: Whether to include agencies in the csv file
        """
        from services.property import PropertyService
        from services.agency import AgencyService
        from common.constans import Constans
        from common.utils import flatten_dict
        logger.info(f"Saving properties to {filename}. Format: csv")
        properties = PropertyService.get_all()
        properties = [property_.to_mongo().to_dict() for property_ in properties]

        if include_agencies:
            logger.info("Including agencies in the csv file")
            agencies = AgencyService.get_all()
            agencies = [agency.to_mongo().to_dict() for agency in agencies]
            for property_ in properties:
                estate_agency = property_.get("estate_agency")
                if estate_agency is not None:
                    agency_id = str(estate_agency)
                    for agency in agencies:
                        if str(agency["_id"]) == agency_id:
                            property_["agency"] = agency
                            property_.pop("estate_agency")
                            break

        with open(filename, "w", newline="", encoding="utf-8") as output_file:
            dict_writer = csv.DictWriter(output_file, Constans.CSV_KEYS)
            dict_writer.writeheader()
            dict_writer.writerows([flatten_dict(property_) for property_ in properties])