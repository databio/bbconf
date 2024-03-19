from pathlib import Path
from typing import Union, List

from bbconf.config_parser.bedbaseconfig import BedBaseConfig

from bbconf.db_utils import POSTGRES_DIALECT, BaseEngine
from bbconf.modules.bedfiles import BedAgentBedFile
from bbconf.modules.bedsets import BedAgentBedSet
from bbconf.modules.objects import BBObjects


class BedBaseAgent(object):
    def __init__(
        self,
        config: Union[Path, str],
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.

        """

        self.config = BedBaseConfig(config)

        self.__bed = BedAgentBedFile(self.config)

        # ff = self.__bed.add("test", {"number_of_regions": 44})

        # ff

        # self.__bedset = BedAgentBedSet(self.config)
        # self.__objects = BBObjects(self.config)

    @property
    def bed(self) -> BedAgentBedFile:
        return self.__bed

    # @property
    # def bedset(self):
    #     return self.__bedset
    # #
    # @property
    # def objects(self):
    #     return self.__objects

