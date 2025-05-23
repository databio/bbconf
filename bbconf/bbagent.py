import logging
from functools import cached_property
from pathlib import Path
from typing import List, Union

from sqlalchemy.orm import Session
from sqlalchemy.sql import distinct, func, select

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.db_utils import (
    Bed,
    BedMetadata,
    BedSets,
    License,
    UsageBedSetMeta,
    UsageBedMeta,
    UsageFiles,
    UsageSearch,
)
from bbconf.models.base_models import StatsReturn, UsageModel, FileStats
from bbconf.modules.bedfiles import BedAgentBedFile
from bbconf.modules.bedsets import BedAgentBedSet
from bbconf.modules.objects import BBObjects

from .const import PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


class BedBaseAgent(object):
    def __init__(
        self,
        config: Union[Path, str],
        init_ml: bool = True,
    ):
        """
        Initialize connection to the pep_db database. You can use the basic connection parameters
        or libpq connection string.

        :param config: path to the configuration file
        :param init_ml: initialize ML models for search (default: True)
        """

        self.config = BedBaseConfig(config, init_ml)

        self._bed = BedAgentBedFile(self.config, self)
        self._bedset = BedAgentBedSet(self.config)
        self._objects = BBObjects(self.config)

    @property
    def bed(self) -> BedAgentBedFile:
        return self._bed

    @property
    def bedset(self) -> BedAgentBedSet:
        return self._bedset

    @property
    def objects(self) -> BBObjects:
        return self._objects

    def __repr__(self) -> str:
        repr = f"BedBaseAgent(config={self.config})"
        repr += f"\n{self.bed}"
        repr += f"\n{self.bedset}"
        repr += f"\n{self.objects}"
        return repr

    def get_stats(self) -> StatsReturn:
        """
        Get statistics for a bed file

        :return: statistics
        """
        with Session(self.config.db_engine.engine) as session:
            number_of_bed = session.execute(select(func.count(Bed.id))).one()[0]
            number_of_bedset = session.execute(select(func.count(BedSets.id))).one()[0]

            number_of_genomes = session.execute(
                select(func.count(distinct(Bed.genome_alias)))
            ).one()[0]

        return StatsReturn(
            bedfiles_number=number_of_bed,
            bedsets_number=number_of_bedset,
            genomes_number=number_of_genomes,
        )

    def get_detailed_stats(self) -> FileStats:
        """
        Get comprehensive statistics for all bed files

        """

        _LOGGER.info("Getting detailed statistics for all bed files")

        with Session(self.config.db_engine.engine) as session:
            file_types = {
                f[0]: f[1]
                for f in session.execute(
                    select(Bed.bed_compliance, func.count(Bed.bed_compliance)).group_by(
                        Bed.bed_compliance
                    )
                ).all()
            }
            file_formats = {
                f[0]: f[1]
                for f in session.execute(
                    select(Bed.data_format, func.count(Bed.data_format)).group_by(
                        Bed.data_format
                    )
                ).all()
            }
            file_genomes = {
                f[0]: f[1]
                for f in session.execute(
                    select(Bed.genome_alias, func.count(Bed.genome_alias)).group_by(
                        Bed.genome_alias
                    )
                ).all()
            }
            file_organism = {
                f[0]: f[1]
                for f in session.execute(
                    select(
                        BedMetadata.species_name, func.count(BedMetadata.species_name)
                    )
                    .group_by(BedMetadata.species_name)
                    .order_by(func.count(BedMetadata.species_name).desc())
                ).all()
            }

        return FileStats(
            file_type=file_types,
            file_format=file_formats,
            file_genome=file_genomes,
            file_organism=file_organism,
        )

    def get_list_genomes(self) -> List[str]:
        """
        Get list of genomes from the database

        :return: list of genomes
        """
        statement = select(distinct(Bed.genome_alias))
        with Session(self.config.db_engine.engine) as session:
            genomes = session.execute(statement).all()
        return [result[0] for result in genomes]

    @cached_property
    def list_of_licenses(self) -> List[str]:
        """
        Get list of licenses from the database

        :return: list of licenses
        """
        statement = select(License.id)
        with Session(self.config.db_engine.engine) as session:
            licenses = session.execute(statement).all()
        return [result[0] for result in licenses]

    def add_usage(self, stats: UsageModel) -> None:

        with Session(self.config.db_engine.engine) as session:
            for key, value in stats.files.items():
                new_stats = UsageFiles(
                    file_path=key,
                    count=value,
                    date_from=stats.date_from,
                    date_to=stats.date_to,
                )
                session.add(new_stats)

            for key, value in stats.bed_meta.items():
                new_stats = UsageBedMeta(
                    bed_id=key,
                    count=value,
                    date_from=stats.date_from,
                    date_to=stats.date_to,
                )
                session.add(new_stats)

            for key, value in stats.bedset_meta.items():
                new_stats = UsageBedSetMeta(
                    bedset_id=key,
                    count=value,
                    date_from=stats.date_from,
                    date_to=stats.date_to,
                )
                session.add(new_stats)

            for key, value in stats.bed_search.items():
                new_stats = UsageSearch(
                    query=key,
                    count=value,
                    type="bed",
                    date_from=stats.date_from,
                    date_to=stats.date_to,
                )
                session.add(new_stats)

            for key, value in stats.bedset_search.items():
                new_stats = UsageSearch(
                    query=key,
                    count=value,
                    type="bedset",
                    date_from=stats.date_from,
                    date_to=stats.date_to,
                )
                session.add(new_stats)

            session.commit()
