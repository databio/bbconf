from typing import List
import logging

from sqlalchemy import select, func, Numeric, Float, or_
from sqlalchemy.orm import Session

# TODO: will be available in the next geniml release
# from geniml.io.utils import compute_md5sum_bedset
from hashlib import md5

from bbconf.config_parser import BedBaseConfig
from bbconf.db_utils import BedFileBedSetRelation, Bed, BedSets, Files

from bbconf.models.bed_models import BedStats
from bbconf.models.bedset_models import (
    BedSetStats,
    BedSetMetadata,
    BedSetListResult,
    FileModel,
    BedSetBedFiles,
    BedSetPlots,
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

    def get(self, identifier: str, full: bool = False) -> BedSetMetadata:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :param full: return full record with stats, plots, files and metadata
        :return: project metadata
        """

        statement = select(BedSets).where(BedSets.id == identifier)

        with Session(self._db_engine.engine) as session:
            bedset_obj = session.scalar(statement)
            if not bedset_obj:
                raise BedSetNotFoundError(identifier)
            list_of_bedfiles = [
                bedset_obj.bedfile_id for bedset_obj in bedset_obj.bedfiles
            ]
            if full:
                plots = BedSetPlots()
                for plot in bedset_obj.files:
                    setattr(plots, plot.name, FileModel(**plot.__dict__))

                stats = BedSetStats(
                    mean=BedStats(**bedset_obj.bedset_means),
                    sd=BedStats(**bedset_obj.bedset_standard_deviation),
                ).model_dump()
            else:
                plots = None
                stats = None

            bedset_metadata = BedSetMetadata(
                id=bedset_obj.id,
                name=bedset_obj.name,
                description=bedset_obj.description,
                md5sum=bedset_obj.md5sum,
                statistics=stats,
                plots=plots,
                bed_ids=list_of_bedfiles,
            )

        return bedset_metadata

    def get_plots(self, identifier: str) -> BedSetPlots:
        """
        Get plots for bedset by identifier.

        :param identifier: bedset identifier
        :return: bedset plots
        """
        statement = select(Files).where(Files.bedset_id == identifier)

        with Session(self._db_engine.engine) as session:
            plots = session.execute(statement).all()

        return BedSetPlots(
            **{plot[0].name: FileModel(**plot[0].model_dump()) for plot in plots}
        )

    def create(
        self,
        identifier: str,
        name: str,
        bedid_list: List[str],
        description: str = None,
        statistics: bool = False,
        plots: dict = None,
        upload_pephub: bool = False,
        upload_s3: bool = False,
        local_path: str = "",
        no_fail: bool = False,
    ) -> None:
        """
        Create bedset in the database.

        :param identifier: bedset identifier
        :param name: bedset name
        :param description: bedset description
        :param bedid_list: list of bed file identifiers
        :param statistics: calculate statistics for bedset
        :param plots: dictionary with plots
        :param upload_pephub: upload bedset to pephub (create view in pephub)
        :param upload_s3: upload bedset to s3
        :param local_path: local path to the output files
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
            # md5sum=compute_md5sum_bedset(bedid_list),
            md5sum=md5("".join(bedid_list).encode()).hexdigest(),
        )

        if upload_s3:
            plots = BedSetPlots(**plots)
            plots = self.config.upload_files_s3(
                identifier, files=plots, base_path=local_path, type="bedsets"
            )

        with Session(self._db_engine.engine) as session:
            session.add(new_bedset)

            for bedfile in bedid_list:
                session.add(
                    BedFileBedSetRelation(bedset_id=identifier, bedfile_id=bedfile)
                )
            if upload_s3:
                for k, v in plots:
                    if v:
                        new_file = Files(
                            **v.model_dump(exclude_none=True, exclude_unset=True),
                            bedset_id=identifier,
                            type="plot",
                        )
                        session.add(new_file)

            session.commit()

        _LOGGER.info(f"Bedset '{identifier}' was created successfully")
        return None

    def _calculate_statistics(self, bed_ids: List[str]) -> BedSetStats:
        """
        Calculate statistics for bedset.

        :param bed_ids: list of bed file identifiers
        :return: statistics
        """

        _LOGGER.info("Calculating bedset statistics")
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
        try:
            self.config.phc.view.create(
                namespace=self.config.config.phc.namespace,
                name=self.config.config.phc.name,
                tag=self.config.config.phc.tag,
                view_name=bedset_id,
                # description=description,
                sample_list=bed_ids,
            )
        except Exception as e:
            _LOGGER.error(f"Failed to create view in pephub: {e}")
            if not nofail:
                raise e
        return None

    def get_ids_list(
        self, query: str = None, limit: int = 10, offset: int = 0
    ) -> BedSetListResult:
        """
        Get list of bedsets from the database.

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

    def delete(self, identifier: str) -> None:
        """
        Delete bed file from the database.

        :param identifier: bedset identifier
        :return: None
        """
        if not self.exists(identifier):
            raise BedSetNotFoundError(identifier)

        _LOGGER.info(f"Deleting bedset '{identifier}'")

        with Session(self._db_engine.engine) as session:
            statement = select(BedSets).where(BedSets.id == identifier)

            bedset_obj = session.scalar(statement)
            files = [FileModel(**k.__dict__) for k in bedset_obj.files]

            session.delete(bedset_obj)
            session.commit()

        self.delete_phc_view(identifier, nofail=True)
        if files:
            self.config.delete_files_s3(files)

    def delete_phc_view(self, identifier: str, nofail: bool = False) -> None:
        """
        Delete view in pephub.

        :param identifier: bedset identifier
        :param nofail: do not raise an error if view not found
        :return: None
        """
        _LOGGER.info(f"Deleting view in pephub for bedset '{identifier}'")
        try:
            self.config.phc.view.delete(
                namespace=self.config.config.phc.namespace,
                name=self.config.config.phc.name,
                tag=self.config.config.phc.tag,
                view_name=identifier,
            )
        except Exception as e:
            _LOGGER.error(f"Failed to delete view in pephub: {e}")
            if not nofail:
                raise e
        return None

    def exists(self, identifier: str) -> bool:
        """
        Check if bedset exists in the database.

        :param identifier: bedset identifier
        :return: True if bedset exists, False otherwise
        """
        statement = select(BedSets).where(BedSets.id == identifier)
        with Session(self._db_engine.engine) as session:
            result = session.execute(statement).one_or_none()
        if result:
            return True
        return False

    def add_bedfile(self, identifier: str, bedfile: str) -> None:
        raise NotImplementedError

    def delete_bedfile(self, identifier: str, bedfile: str) -> None:
        raise NotImplementedError
