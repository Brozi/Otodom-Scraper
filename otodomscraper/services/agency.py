import logging
from mongoengine.errors import NotUniqueError
from models import AgencyDocument

logger = logging.getLogger(__name__)


class AgencyService:
    """
    Service responsible for interacting with the agency documents in the database.
    """

    @classmethod
    def get_all(cls) -> list[AgencyDocument]:
        """
        :return: All the agencies in the database
        """
        logger.info("Getting all agencies from database")
        return AgencyDocument.objects.all()

    @classmethod
    def get_by_otodom_id(cls, otodom_id: int) -> AgencyDocument | None:
        """
        :param otodom_id: The otodom id of the agency

        :return: The agency document with the given otodom id
            or None if there is no agency with the given otodom id
        """
        return AgencyDocument.objects(otodom_id=otodom_id).first()

    @staticmethod
    def put(agency):
        try:
            agency = agency.save()
            return agency
        except NotUniqueError:
            # The agency is already in the database!
            # We just catch the error quietly and move on.
            logger.debug(f"Agency {agency.name} already exists. Skipping.")
            return None
        except Exception as e:
            logger.error(f"Failed to insert agency {agency.name} to database: {e}")
            return None
