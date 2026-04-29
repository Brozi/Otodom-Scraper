import logging
from models import PropertyDocument
from mongoengine import QuerySet

logger = logging.getLogger(__name__)
PropertyLink = str

class PropertyService:
    """
    Service responsible for interacting with the property documents in the database.
    """

    @classmethod
    def get_all(cls) -> list[PropertyDocument]:
        """
        :return: All the properties in the database
        """
        logger.info("Getting all properties from database")
        return PropertyDocument.objects.all()

    @classmethod
    def get_by_otodom_id(cls, otodom_id: int) -> PropertyDocument | None:
        """
        :param otodom_id: The otodom id of the property

        :return: The property document with the given otodom id
            or None if there is no property with the given otodom id
        """
        return PropertyDocument.objects(otodom_id=otodom_id).first()

    @classmethod
    def get_all_links(cls) -> set[PropertyLink]:
        """
        :return: All the links of the properties in the database
        """
        logger.info("Getting all property links from database")
        properties: QuerySet = PropertyDocument.objects.all()
        return {property_.link for property_ in properties}

    @classmethod
    def put(cls, property_: PropertyDocument) -> PropertyDocument | None:
        """
        Inserts the property into the database.
        """
        try:
            property_.validate()
            property_ = property_.save()
            return property_
        except Exception as e:
            error_msg = str(e)
            # Catch the specific duplicate key error gracefully
            if "E11000 duplicate key error" in error_msg or "NotUniqueError" in error_msg:
                logger.warning(
                    f"Duplicate property ignored (otodom_id: {property_.otodom_id}). Link was new, but ID already exists.")
                return None
            else:
                logger.error(f"Failed to insert property {property_.otodom_id}. Error: {error_msg}")
                return None
