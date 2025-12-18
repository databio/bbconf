import logging
import statistics
from functools import cached_property
from pathlib import Path
from typing import Dict, List, Union

import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy.sql import and_, distinct, func, or_, select

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.db_utils import (
    Bed,
    BedMetadata,
    BedSets,
    BedStats,
    Files,
    GeoGsmStatus,
    License,
    UsageBedMeta,
    UsageBedSetMeta,
    UsageFiles,
    UsageSearch,
)
from bbconf.models.base_models import (
    AllFilesInfo,
    BinValues,
    FileInfo,
    FileStats,
    GEOStatistics,
    StatsReturn,
    UsageModel,
    UsageStats,
)
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
            file_assay = {
                f[0]: f[1]
                for f in session.execute(
                    select(BedMetadata.assay, func.count(BedMetadata.assay))
                    .group_by(BedMetadata.assay)
                    .order_by(func.count(BedMetadata.assay).desc())
                ).all()
            }

        slice_value = 20

        bed_comments = self._stats_comments(session)
        geo_status = self._stats_geo_status(session)

        bedfiles_info = self.bed_files_info()

        number_of_regions = [bed.number_of_regions for bed in bedfiles_info.files]
        list_mean_width = [bed.mean_region_width for bed in bedfiles_info.files]
        list_file_size = [bed.file_size for bed in bedfiles_info.files]

        number_of_regions_bins = self._bin_number_of_regions(number_of_regions)
        list_mean_width_bins = self._bin_mean_region_width(list_mean_width)
        list_file_size_bins = self._bin_file_size(list_file_size)

        geo_stats = self._get_geo_stats(session)

        if concise:
            bed_compliance_concise = dict(list(bed_compliance.items())[0:slice_value])
            bed_compliance_concise["other"] = sum(
                list(bed_compliance.values())[slice_value:]
            )
            if "" in bed_compliance_concise:
                bed_compliance_concise["other"] = (
                    bed_compliance_concise["other"] + bed_compliance_concise[""]
                )
                bed_compliance_concise.pop("")

            file_genomes_concise = dict(list(file_genomes.items())[0:slice_value])
            file_genomes_concise["other"] = sum(
                list(file_genomes.values())[slice_value:]
            ) + file_genomes.get("other", 0)
            if "" in file_genomes_concise:
                file_genomes_concise["other"] = (
                    file_genomes_concise["other"] + file_genomes_concise[""]
                )
                file_genomes_concise.pop("")

            file_organism_concise = dict(list(file_organism.items())[0:slice_value])
            file_organism_concise["other"] = sum(
                list(file_organism.values())[slice_value:]
            ) + file_organism.get("other", 0)
            if "" in file_organism_concise:
                file_organism_concise["other"] = (
                    file_organism_concise["other"] + file_organism_concise[""]
                )
                file_organism_concise.pop("")
            file_assay_concise = dict(list(file_assay.items())[0:slice_value])
            file_assay_concise["other"] = sum(
                list(file_assay.values())[slice_value:]
            ) + file_assay.get("other", 0)
            if "" in file_assay_concise:
                file_assay_concise["other"] = (
                    file_assay_concise["other"] + file_assay_concise[""]
                )
                file_assay_concise.pop("")
            if "OTHER" in file_assay_concise:
                file_assay_concise["other"] = (
                    file_assay_concise["other"] + file_assay_concise["OTHER"]
                )
                file_assay_concise.pop("OTHER")

            return FileStats(
                data_format=data_format,
                bed_compliance=bed_compliance_concise,
                file_genome=file_genomes_concise,
                file_organism=file_organism_concise,
                file_assay=file_assay_concise,
                bed_comments=bed_comments,
                geo_status=geo_status,
                mean_region_width=list_mean_width_bins,
                file_size=list_file_size_bins,
                number_of_regions=number_of_regions_bins,
                geo=geo_stats,
            )

        return FileStats(
            data_format=data_format,
            bed_compliance=bed_compliance,
            file_genome=file_genomes,
            file_organism=file_organism,
            file_assay=file_assay,
            bed_comments=bed_comments,
            geo_status=geo_status,
            mean_region_width=list_mean_width_bins,
            file_size=list_file_size_bins,
            number_of_regions=number_of_regions_bins,
            geo=geo_stats,
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
                if f[0]
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
                if f[0]
            }

            bed_downloads = {
                f[0].split("/")[-1].split(".")[0]: f[1]
                for f in session.execute(
                    select(UsageFiles.file_path, func.sum(UsageFiles.count))
                    .group_by(UsageFiles.file_path)
                    .order_by(func.sum(UsageFiles.count).desc())
                    .limit(20)
                ).all()
            }

        return UsageStats(
            bed_metadata=bed_metadata,
            bedset_metadata=bedset_metadata,
            bed_search_terms=bed_search_terms,
            bedset_search_terms=bedset_search_terms,
            bed_downloads=bed_downloads,
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

    def get_list_assays(self):
        """
        Get list of genomes from the database

        :return: list of genomes
        """

        with Session(self.config.db_engine.engine) as session:
            statement = (
                select(BedMetadata.assay)
                .group_by(BedMetadata.assay)
                .order_by(func.count(BedMetadata.assay).desc())
            )
            results = session.execute(statement).all()
        return [result[0] for result in results if result[0]]

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

    def _stats_comments(self, sa_session: Session) -> Dict[str, int]:
        """
        Get statistics about comments that are present in bed files.

        :param sa_session: SQLAlchemy session

        :return: Dict[str, int]
        """
        _LOGGER.info("Analyzing bed table for comments in bed files...")

        total_statement = select(func.count(Bed.id)).where(Bed.header.is_not(None))
        correct_statement = select(func.count(Bed.id)).where(Bed.header.like("#%"))
        track_name_statement = select(func.count(Bed.id)).where(
            Bed.header.like("track name%")
        )
        track_type_statement = select(func.count(Bed.id)).where(
            Bed.header.like("track type%")
        )
        browser_statement = select(func.count(Bed.id)).where(
            Bed.header.like("browser%")
        )

        total_bed_comments = (
            sa_session.execute(total_statement).one_or_none() or (0,)
        )[0]
        correct_bed_comments = (
            sa_session.execute(correct_statement).one_or_none() or (0,)
        )[0]
        track_name_comments = (
            sa_session.execute(track_name_statement).one_or_none() or (0,)
        )[0]
        track_type_comments = (
            sa_session.execute(track_type_statement).one_or_none() or (0,)
        )[0]
        browser_comments = (
            sa_session.execute(browser_statement).one_or_none() or (0,)
        )[0]

        header_comments = (
            total_bed_comments
            - correct_bed_comments
            - track_name_comments
            - track_type_comments
            - browser_comments
        )

        return {
            "correct_bed_comments": correct_bed_comments,
            "track_name_comments": track_name_comments,
            "track_type_comments": track_type_comments,
            "browser_comments": browser_comments,
            "header_comments": header_comments,
        }

    def _stats_geo_status(self, sa_session: Session) -> Dict[str, int]:
        """
        Get statistics about status of GEO bed file processing.

        :param sa_session: SQLAlchemy session
        :return Dict[str, int]
        """

        success_statement = select(
            func.count(distinct(GeoGsmStatus.sample_name))
        ).where(GeoGsmStatus.status == "SUCCESS")

        failed_count_statement = select(
            func.count(distinct(GeoGsmStatus.sample_name))
        ).where(and_(GeoGsmStatus.status == "FAIL"))
        mean_region_width_statement = select(
            func.count(distinct(GeoGsmStatus.sample_name))
        ).where(
            or_(
                GeoGsmStatus.error.like("%Initial QC failed for%"),
                GeoGsmStatus.error.like("%Quality control failed%"),
            )
        )
        size_greater_then_statement = select(
            func.count(distinct(GeoGsmStatus.sample_name))
        ).where(GeoGsmStatus.error.like("%File size is too big.%"))

        success_count = (sa_session.execute(success_statement).one_or_none() or (0,))[0]
        failed_count = (
            sa_session.execute(failed_count_statement).one_or_none() or (0,)
        )[0]

        mean_region_width_count = (
            sa_session.execute(mean_region_width_statement).one_or_none() or (0,)
        )[0]
        size_greater_than_count = (
            sa_session.execute(size_greater_then_statement).one_or_none() or (0,)
        )[0]
        corrupted_file_count = (
            failed_count - mean_region_width_count - size_greater_than_count
        )

        return {
            "success": success_count,
            "mean_region_width_lower_10": mean_region_width_count,
            "size_greater_20": size_greater_than_count,
            "corrupted_files": corrupted_file_count,
        }

    def bed_files_info(self) -> AllFilesInfo:
        """
        Get information about all bed files in bedbase.

        :param sa_session: SQLAlchemy session
        :return AllFilesInfo:
            {
            "total": int,"
            "files": [
                {   id: str
                    bed_compliance: str
                    data_format: str
                    mean_region_width: float
                    file_size: int
                    number_of_regions: int
                },
                ... ]
            }
        """

        all_files_statement = (
            select(
                Bed.id,
                Bed.bed_compliance,
                Bed.data_format,
                BedStats.mean_region_width,
                Files.size,
                BedStats.number_of_regions,
            )
            .join(BedStats, BedStats.id == Bed.id)  # Explicit join condition
            .join(Files, Files.bedfile_id == Bed.id)  # Explicit join condition
            .where(Files.name == "bed_file")
        )

        results = []
        error_list = []
        with Session(self.config.db_engine.engine) as session:
            sql_result = session.execute(all_files_statement).all()

            for single_bed in sql_result:
                try:
                    results.append(
                        FileInfo(
                            id=single_bed[0],
                            bed_compliance=single_bed[1],
                            data_format=single_bed[2],
                            mean_region_width=single_bed[3],
                            file_size=single_bed[4],
                            number_of_regions=single_bed[5],
                        )
                    )
                except Exception:
                    error_list.append(single_bed[0])

        _LOGGER.info(
            f"Number of bed records with unknown regions and region width {len(error_list)}"
        )
        return AllFilesInfo(
            total=len(results),
            files=results,
        )

    def _bin_number_of_regions(self, number_of_regions: list) -> BinValues:
        """
        Create bins for number of regions in bed files

        :param number_of_regions: list of number of regions in bed files
        :return: BinValues object containing bins and values
        """

        max_value_threshold = 400_000  # set a threshold for maximum value to avoid outliers in the histogram

        filtered_number_of_regions = [
            x if x <= max_value_threshold else max_value_threshold + 1
            for x in number_of_regions
        ]

        n_region_counts, n_region_bin_edges = np.histogram(
            filtered_number_of_regions, bins=100
        )
        n_region_counts = n_region_counts.astype(int).tolist()
        n_region_bin_edges = n_region_bin_edges.astype(int).tolist()

        return BinValues(
            bins=n_region_bin_edges,
            counts=n_region_counts,
            mean=round(statistics.mean(number_of_regions), 2),
            median=round(statistics.median(number_of_regions), 2),
        )

    def _bin_mean_region_width(self, mean_region_widths: list) -> BinValues:
        """
        Create bins for number of regions in bed files

        :param mean_region_widths: list of mean region widths in bed files
        :return: BinValues object containing bins and values
        """

        max_value_threshold = 5_000  # set a threshold for maximum value to avoid outliers in the histogram

        filtered_mean_region_widths = [
            x if x <= max_value_threshold else max_value_threshold + 1
            for x in mean_region_widths
        ]

        mean_reg_width_counts, mean_reg_width_bin_edges = np.histogram(
            filtered_mean_region_widths, bins=100
        )
        mean_reg_width_counts = mean_reg_width_counts.tolist()
        mean_reg_width_bin_edges = mean_reg_width_bin_edges.tolist()

        return BinValues(
            bins=mean_reg_width_bin_edges,
            counts=mean_reg_width_counts,
            mean=round(statistics.mean(mean_region_widths), 2),
            median=round(statistics.median(mean_region_widths), 2),
        )

    def _bin_file_size(self, list_file_size: list) -> BinValues:
        """
        Create bins for number of regions in bed files

        :param list_file_size: list of bed file sizes in bytes
        :return: BinValues object containing bins and values
        """

        max_value_threshold = 10 * 1024 * 1024

        filtered_list_file_size = [
            x for x in list_file_size if x <= max_value_threshold
        ]

        filtered_list_file_size = [x / (1024 * 1024) for x in filtered_list_file_size]

        file_size_counts, file_size_bin_edges = np.histogram(
            filtered_list_file_size, bins=100
        )
        file_size_counts = file_size_counts.astype(int).tolist()
        file_size_bin_edges = file_size_bin_edges.astype(float).tolist()

        return BinValues(
            bins=file_size_bin_edges,
            counts=file_size_counts,
            mean=round(statistics.mean(filtered_list_file_size), 2),
            median=round(statistics.median(filtered_list_file_size), 2),
        )

    def _get_geo_stats(self, sa_session: Session) -> GEOStatistics:
        """
        Get GEO statistics for the bedbase platform.

        :return: GEOStatistics
        """

        _LOGGER.info("Getting GEO statistics.")

        statement = select(
            GeoGsmStatus.bed_id,
            GeoGsmStatus.source_submission_date,
            GeoGsmStatus.file_size,
        ).distinct(GeoGsmStatus.sample_name)

        results = sa_session.execute(statement).all()

        years = []
        file_sizes = []

        for result in results:
            if result[1]:
                years.append(result[1].year)
            if result[2]:
                file_sizes.append(result[2])

        years_array = np.array([y for y in years if y is not None])
        unique_years, counts = np.unique(years_array, return_counts=True)

        cumulative_counts = np.cumsum(counts)
        unique_years = unique_years.astype(str).tolist()

        years_cumulative = dict(zip(unique_years, cumulative_counts))
        years_numbers = dict(zip(unique_years, counts))

        MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
        file_size_filter = [
            x if x <= MAX_FILE_SIZE else MAX_FILE_SIZE + 1 for x in file_sizes
        ]

        file_size_filter = [x / (1024 * 1024) for x in file_size_filter]
        file_size_counts, file_size_bin_edges = np.histogram(file_size_filter, bins=100)

        return GEOStatistics(
            number_of_files=years_numbers,
            cumulative_number_of_files=years_cumulative,
            file_sizes=BinValues(
                bins=list(file_size_bin_edges),
                counts=file_size_counts.astype(int).tolist(),
                mean=round(statistics.mean(file_sizes), 2),
                median=round(statistics.median(file_sizes), 2),
            ),
        )
