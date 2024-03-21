from pathlib import Path
from typing import Union, List

from sqlalchemy.orm import Session
from sqlalchemy.sql import select, func, distinct

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.db_utils import Bed, BedSets

from bbconf.modules.bedfiles import BedAgentBedFile
from bbconf.modules.bedsets import BedAgentBedSet
from bbconf.modules.objects import BBObjects
from bbconf.models.base_models import StatsReturn


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
        self.__objects = BBObjects(self.config)

    @property
    def bed(self) -> BedAgentBedFile:
        return self.__bed

    # @property
    # def bedset(self):
    #     return self.__bedset

    @property
    def objects(self):
        return self.__objects

    def get_stats(self, bedfile: str) -> StatsReturn:
        """
        Get statistics for a bed file

        :param bedfile: bed file name
        :return: statistics
        """
        with Session(self.config.db_engine.engine) as session:
            number_of_bed = session.execute(select(func.count(Bed.id))).one()[0]
            number_of_bedset = session.execute(select(func.count(BedSets.id))).one()[0]

            number_of_genomes = session.execute(
                select(func.count(distinct(Bed.genome_alias)))
            ).one()[0]

        return StatsReturn(
            number_of_bed=number_of_bed,
            number_of_bedset=number_of_bedset,
            number_of_genomes=number_of_genomes,
        )
