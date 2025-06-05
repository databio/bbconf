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
from bbconf.models.base_models import StatsReturn, UsageModel, FileStats, UsageStats
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

    def get_detailed_stats(self, concise: bool = False) -> FileStats:
        """
        Get comprehensive statistics for all bed files

        :param concise: if True, return only top 20 items for each category
        :return: FileStats object containing detailed statistics
        """

        _LOGGER.info("Getting detailed statistics for all bed files")

        with Session(self.config.db_engine.engine) as session:
            bed_compliance = {
                f[0]: f[1]
                for f in session.execute(
                    select(Bed.bed_compliance, func.count(Bed.bed_compliance))
                    .group_by(Bed.bed_compliance)
                    .order_by(func.count(Bed.bed_compliance).desc())
                ).all()
            }
            data_format = {
                f[0]: f[1]
                for f in session.execute(
                    select(Bed.data_format, func.count(Bed.data_format))
                    .group_by(Bed.data_format)
                    .order_by(func.count(Bed.data_format).desc())
                ).all()
            }
            file_genomes = {
                f[0]: f[1]
                for f in session.execute(
                    select(Bed.genome_alias, func.count(Bed.genome_alias))
                    .group_by(Bed.genome_alias)
                    .order_by(func.count(Bed.genome_alias).desc())
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

        slice_value = 20

        if concise:
            bed_compliance_concise = dict(list(bed_compliance.items())[0:slice_value])
            bed_compliance_concise["other"] = sum(
                list(bed_compliance.values())[slice_value:]
            )

            file_genomes_concise = dict(list(file_genomes.items())[0:slice_value])
            file_genomes_concise["other"] = sum(
                list(file_genomes.values())[slice_value:]
            ) + file_genomes.get("other", 0)

            file_organism_concise = dict(list(file_organism.items())[0:slice_value])
            file_organism_concise["other"] = sum(
                list(file_organism.values())[slice_value:]
            ) + file_organism.get("other", 0)

            return FileStats(
                data_format=data_format,
                bed_compliance=bed_compliance_concise,
                file_genome=file_genomes_concise,
                file_organism=file_organism_concise,
            )

        return FileStats(
            data_format=data_format,
            bed_compliance=bed_compliance,
            file_genome=file_genomes,
            file_organism=file_organism,
        )

    def get_detailed_usage(self) -> UsageStats:
        """
        Get detailed usage statistics for the bedbase platform.
        This method will only return top 20 items for each category.

        :return: UsageStats object containing detailed usage statistics
        """

        _LOGGER.info("Getting detailed usage statistics.")

        with Session(self.config.db_engine.engine) as session:
            bed_metadata = {
                f[0]: f[1]
                for f in session.execute(
                    select(UsageBedMeta.bed_id, func.sum(UsageBedMeta.count))
                    .group_by(UsageBedMeta.bed_id)
                    .order_by(func.sum(UsageBedMeta.count).desc())
                    .limit(20)
                ).all()
            }
            bedset_metadata = {
                f[0]: f[1]
                for f in session.execute(
                    select(UsageBedSetMeta.bedset_id, func.sum(UsageBedSetMeta.count))
                    .group_by(UsageBedSetMeta.bedset_id)
                    .order_by(func.sum(UsageBedSetMeta.count).desc())
                    .limit(20)
                ).all()
            }

            bed_search_terms = {
                f[0]: f[1]
                for f in session.execute(
                    select(UsageSearch.query, func.sum(UsageSearch.count))
                    .where(UsageSearch.type == "bed")
                    .group_by(UsageSearch.query)
                    .order_by(func.sum(UsageSearch.count).desc())
                    .limit(20)
                ).all()
            }

            bedset_search_terms = {
                f[0]: f[1]
                for f in session.execute(
                    select(UsageSearch.query, func.sum(UsageSearch.count))
                    .where(UsageSearch.type == "bedset")
                    .group_by(UsageSearch.query)
                    .order_by(func.sum(UsageSearch.count).desc())
                    .limit(20)
                ).all()
            }

        return UsageStats(
            bed_metadata=bed_metadata,
            bedset_metadata=bedset_metadata,
            bed_search_terms=bed_search_terms,
            bedset_search_terms=bedset_search_terms,
        )

    def get_list_genomes(self) -> List[str]:
        """
        Get list of genomes from the database

        :return: list of genomes
        """
        statement = (
            select(Bed.genome_alias)
            .group_by(Bed.genome_alias)
            .order_by(func.count(Bed.genome_alias).desc())
        )
        with Session(self.config.db_engine.engine) as session:
            genomes = session.execute(statement).all()
        return [result[0] for result in genomes if result[0]]

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

            # FILES USAGE
            reported_items_files = session.scalars(
                select(UsageFiles).where(UsageFiles.date_to > func.now())
            )
            reported_dict_files = {
                item.file_path: item for item in reported_items_files
            }

            for key, value in stats.files.items():
                if key in reported_dict_files:
                    reported_dict_files[key].count += value
                else:
                    new_stats = UsageFiles(
                        file_path=key,
                        count=value,
                        date_from=stats.date_from,
                        date_to=stats.date_to,
                    )
                    session.add(new_stats)

            # METADATA USAGE
            reported_items_metadata = session.scalars(
                select(UsageBedMeta).where(UsageBedMeta.date_to > func.now())
            )

            reported_dict_metadata = {
                item.bed_id: item for item in reported_items_metadata
            }

            for key, value in stats.bed_meta.items():
                if key in reported_dict_metadata:
                    reported_dict_metadata[key].count += value
                else:
                    new_stats = UsageBedMeta(
                        bed_id=key,
                        count=value,
                        date_from=stats.date_from,
                        date_to=stats.date_to,
                    )
                    session.add(new_stats)

            # BEDSET METADATA USAGE
            reported_items_bedset_metadata = session.scalars(
                select(UsageBedSetMeta).where(UsageBedSetMeta.date_to > func.now())
            )

            reported_dict_bedset_metadata = {
                item.bedset_id: item for item in reported_items_bedset_metadata
            }

            for key, value in stats.bedset_meta.items():
                if key in reported_dict_bedset_metadata:
                    reported_dict_bedset_metadata[key].count += value
                else:
                    new_stats = UsageBedSetMeta(
                        bedset_id=key,
                        count=value,
                        date_from=stats.date_from,
                        date_to=stats.date_to,
                    )
                    session.add(new_stats)

            # SEARCH USAGE

            reported_items_bed_search = session.scalars(
                select(UsageSearch).where(
                    UsageSearch.type == "bed", UsageSearch.date_to > func.now()
                )
            )
            reported_dict_bed_search = {
                item.query: item for item in reported_items_bed_search
            }

            for key, value in stats.bed_search.items():
                if key in reported_dict_bed_search:
                    reported_dict_bed_search[key].count += value
                else:
                    new_stats = UsageSearch(
                        query=key,
                        count=value,
                        type="bed",
                        date_from=stats.date_from,
                        date_to=stats.date_to,
                    )
                    session.add(new_stats)

            # SEARCH BEDSET USAGE
            reporeted_items_bedset_search = session.scalars(
                select(UsageSearch).where(
                    UsageSearch.type == "bedset", UsageSearch.date_to > func.now()
                )
            )
            reported_dict_bedset_search = {
                item.query: item for item in reporeted_items_bedset_search
            }

            for key, value in stats.bedset_search.items():
                if key in reported_dict_bedset_search:
                    reported_dict_bedset_search[key].count += value
                else:
                    new_stats = UsageSearch(
                        query=key,
                        count=value,
                        type="bedset",
                        date_from=stats.date_from,
                        date_to=stats.date_to,
                    )
                    session.add(new_stats)

            session.commit()
