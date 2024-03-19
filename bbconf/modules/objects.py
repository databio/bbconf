from bbconf.db_utils import BaseEngine


class BBObjects:
    """ """

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
