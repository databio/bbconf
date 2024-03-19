from bbconf.db_utils import BaseEngine


class BedAgentBedSet:
    """
    Class that represents Bedset in Database.

    This class has method to add, delete, get files and metadata from the database.
    """

    def __init__(self, db_engine: BaseEngine):
        """
        :param db_engine: pepdbengine object with sa engine
        """
        self._sa_engine = db_engine.engine
        self._db_engine = db_engine

    def get(self, identifier: str) -> dict:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :return: project metadata
        """
        ...

    def create(self): ...

    def delete(self, identifier: str) -> None:
        """
        Delete bed file from the database.

        :param identifier: bed file identifier
        :return: None
        """
        ...

    def add_bedfile(self, identifier: str, bedfile: str) -> None: ...

    def delete_bedfile(self, identifier: str, bedfile: str) -> None: ...
