from typing import List
import logging
from hashlib import md5

from sqlalchemy import select, func, Numeric, Float, or_
from sqlalchemy.orm import Session

from bbconf.config_parser import BedBaseConfig
from bbconf.db_utils import BedFileBedSetRelation, Bed, BedSets

from bbconf.models.bed_models import BedStats
from bbconf.models.bedset_models import (
    BedSetStats,
    BedSetMetadata,
    BedSetListResult,
    FileModel,
    BedSetBedFiles,
)
from bbconf.modules.bedfiles import BedAgentBedFile
from bbconf.const import PKG_NAME
from bbconf.exceptions import BedSetNotFoundError, BEDFileNotFoundError


_LOGGER = logging.getLogger(PKG_NAME)


class BedAgentBedSet:
    """
    Class that represents Bedset in Database.

    This class has method to add, delete, get files and metadata from the database.
    """

    def __init__(self, config: BedBaseConfig):
        """
        :param config: config object
        """
        self.config = config
        self._db_engine = self.config.db_engine

    def get(self, identifier: str) -> BedSetMetadata:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :return: project metadata
        """

        statement = select(BedSets).where(BedSets.id == identifier)

        with Session(self._db_engine.engine) as session:
            bedset_obj = session.execute(statement).one()
            if not bedset_obj:
                raise BedSetNotFoundError(identifier)
            else:
                bedset_obj = bedset_obj[0]
            list_of_bedfiles = [
                bedset_obj.bedfile_id for bedset_obj in bedset_obj.bedfiles
            ]

            bedset_metadata = BedSetMetadata(
                id=bedset_obj.id,
                name=bedset_obj.name,
                description=bedset_obj.description,
                md5sum=bedset_obj.md5sum,
                statistics=BedSetStats(
                    mean=BedStats(**bedset_obj.bedset_means),
                    sd=BedStats(**bedset_obj.bedset_standard_deviation),
                ).model_dump(),
                plots=[FileModel(**plot.__dict__) for plot in bedset_obj.files],
                bed_ids=list_of_bedfiles,
            )

        return bedset_metadata

    def create(
        self,
        identifier: str,
        name: str,
        description: str = None,
        bedid_list: List[str] = None,
        statistics: bool = False,
        plots: dict = None,
        upload_pephub: bool = False,
        no_fail: bool = False,
    ) -> None:
        """
        Create bedset in the database.

        :param identifier: bedset identifier
        :param description: bedset description
        :param bedid_list: list of bed file identifiers
        :param statistics: calculate statistics for bedset
        :param plots: dictionary with plots
        :param upload_pephub: upload bedset to pephub (create view in pephub)
        :param no_fail: do not raise an error if bedset already exists
        :return: None
        """
        _LOGGER.info(f"Creating bedset '{identifier}'")

        if statistics:
            stats = self._calculate_statistics(bedid_list)
        else:
            stats = None

        if upload_pephub:
            try:
                self._create_pephub_view(identifier, description, bedid_list, no_fail)
            except Exception as e:
                _LOGGER.error(f"Failed to create view in pephub: {e}")
                if not no_fail:
                    raise e

        new_bedset = BedSets(
            id=identifier,
            name=name,
            description=description,
            bedset_means=stats.mean.model_dump() if stats else None,
            bedset_standard_deviation=stats.sd.model_dump() if stats else None,
            md5sum=md5(";".join(sorted(bedid_list)).encode("utf-8")).hexdigest(),
        )
        # TODO: use md5sum from geniml.io

        # TODO: upload plots! We don't have them now

        with Session(self._db_engine.engine) as session:
            session.add(new_bedset)

            for bedfile in bedid_list:
                session.add(
                    BedFileBedSetRelation(bedset_id=identifier, bedfile_id=bedfile)
                )

            session.commit()

        _LOGGER.info(f"Bedset '{identifier}' was created successfully")
        return None

    def _calculate_statistics(self, bed_ids: List[str]) -> BedSetStats:
        """
        Calculate statistics for bedset.

        :param bed_ids: list of bed file identifiers
        :return: statistics
        """

        _LOGGER.info(f"Calculating bedset statistics")
        numeric_columns = BedStats.model_fields

        bedset_sd = {}
        bedset_mean = {}
        with Session(self._db_engine.engine) as session:
            for column_name in numeric_columns:
                mean_bedset_statement = select(
                    func.round(
                        func.avg(getattr(Bed, column_name)).cast(Numeric), 4
                    ).cast(Float)
                ).where(Bed.id.in_(bed_ids))

                sd_bedset_statement = select(
                    func.round(
                        func.stddev(getattr(Bed, column_name)).cast(Numeric),
                        4,
                    ).cast(Float)
                ).where(Bed.id.in_(bed_ids))

                bedset_sd[column_name] = session.execute(mean_bedset_statement).one()[0]
                bedset_mean[column_name] = session.execute(sd_bedset_statement).one()[0]

            bedset_stats = BedSetStats(
                mean=bedset_mean,
                sd=bedset_sd,
            )

        _LOGGER.info("Bedset statistics were calculated successfully")
        return bedset_stats

    def _create_pephub_view(
        self,
        bedset_id: str,
        description: str = None,
        bed_ids: list = None,
        nofail: bool = False,
    ) -> None:
        """
        Create view in pephub for bedset.

        :param bedset_id: bedset identifier
        :param description: bedset description
        :param bed_ids: list of bed file identifiers
        :param nofail: do not raise an error if sample not found

        :return: None
        """

        _LOGGER.info(f"Creating view in pephub for bedset '{bedset_id}'")
        self.config.phc.view.create(
            namespace=self.config.config.phc.namespace,
            name=self.config.config.phc.name,
            tag=self.config.config.phc.tag,
            view_name=bedset_id,
            # description=description,
            sample_list=bed_ids,
        )
        return None

    def get_ids_list(self, limit: int = 10, offset: int = 0) -> BedSetListResult:
        """
        Get list of bedsets from the database.

        :param limit: limit of results
        :param offset: offset of results
        :return: list of bedsets
        """
        # TODO: add search and some metadata here
        statement = select(BedSets).limit(limit).offset(offset)

        with Session(self._db_engine.engine) as session:
            bedset_list = session.execute(statement).all()

        results = [self.get(bedset[0].id) for bedset in bedset_list]

        return BedSetListResult(
            count=len(results),
            limit=limit,
            offset=offset,
            results=results,
        )

    def get_bedset_bedfiles(
        self, identifier: str, full: bool = False, limit: int = 100, offset: int = 0
    ) -> BedSetBedFiles:
        """
        Get list of bedfiles in bedset.

        :param identifier: bedset identifier
        :param full: return full records with stats, plots, files and metadata
        :param limit: limit of results
        :param offset: offset of results

        :return: list of bedfiles
        """
        bed_object = BedAgentBedFile(self.config)

        statement = (
            select(BedFileBedSetRelation)
            .where(BedFileBedSetRelation.bedset_id == identifier)
            .limit(limit)
            .offset(offset)
        )

        with Session(self._db_engine.engine) as session:
            bedfiles = session.execute(statement).all()
        results = []
        for bedfile in bedfiles:
            try:
                results.append(bed_object.get(bedfile[0].bedfile_id, full=full))
            except BEDFileNotFoundError as _:
                _LOGGER.error(f"Bedfile {bedfile[0].bedfile_id} not found")

        return BedSetBedFiles(
            count=len(results),
            limit=limit,
            offset=offset,
            results=results,
        )

    def search(self, query: str, limit: int = 10, offset: int = 0) -> BedSetListResult:
        """
        Search bedsets in the database.

        :param query: search query
        :param limit: limit of results
        :param offset: offset of results
        :return: list of bedsets
        """
        statement = select(BedSets.id)
        if query:
            sql_search_str = f"%{query}%"
            statement = statement.where(
                or_(
                    BedSets.name.ilike(sql_search_str),
                    BedSets.description.ilike(sql_search_str),
                )
            )
        with Session(self._db_engine.engine) as session:
            bedset_list = session.execute(statement.limit(limit).offset(offset))

        result_list = []
        for bedset_id in bedset_list:
            result_list.append(self.get(bedset_id[0]))
        return BedSetListResult(
            count=len(result_list),
            limit=limit,
            offset=offset,
            results=result_list,
        )

    def delete(self) -> None:
        """
        Delete bed file from the database.

        :param identifier: bed file identifier
        :return: None
        """
        raise NotImplementedError

    def add_bedfile(self, identifier: str, bedfile: str) -> None:
        raise NotImplementedError

    def delete_bedfile(self, identifier: str, bedfile: str) -> None:
        raise NotImplementedError
