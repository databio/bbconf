import logging
from typing import Dict, List

from geniml.io.utils import compute_md5sum_bedset
from sqlalchemy import Float, Numeric, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from bbconf.config_parser import BedBaseConfig
from bbconf.const import PKG_NAME
from bbconf.db_utils import Bed, BedFileBedSetRelation, BedSets, BedStats, Files
from bbconf.exceptions import (
    BedBaseConfError,
    BEDFileNotFoundError,
    BedSetExistsError,
    BedSetNotFoundError,
    BedSetTrackHubLimitError,
)
from bbconf.models.bed_models import BedStatsModel, StandardMeta
from bbconf.models.bedset_models import (
    BedMetadataBasic,
    BedSetBedFiles,
    BedSetListResult,
    BedSetMetadata,
    BedSetPEP,
    BedSetPlots,
    BedSetStats,
    FileModel,
)

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
                    mean=BedStatsModel(**bedset_obj.bedset_means),
                    sd=BedStatsModel(**bedset_obj.bedset_standard_deviation),
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
                submission_date=bedset_obj.submission_date,
                last_update_date=bedset_obj.last_update_date,
                author=bedset_obj.author,
                source=bedset_obj.source,
            )

        return bedset_metadata

    def get_plots(self, identifier: str) -> BedSetPlots:
        """
        Get plots for bedset by identifier.

        :param identifier: bedset identifier
        :return: bedset plots
        """
        statement = select(BedSets).where(BedSets.id == identifier)

        with Session(self._db_engine.engine) as session:
            bedset_object = session.scalar(statement)
            if not bedset_object:
                raise BedSetNotFoundError(f"Bed file with id: {identifier} not found.")
            bedset_files = BedSetPlots()
            for result in bedset_object.files:
                if result.name in bedset_files.model_fields:
                    setattr(
                        bedset_files,
                        result.name,
                        FileModel(
                            **result.__dict__,
                            object_id=f"bed.{identifier}.{result.name}",
                            access_methods=self.config.construct_access_method_list(
                                result.path
                            ),
                        ),
                    )
        return bedset_files

    def get_objects(self, identifier: str) -> Dict[str, FileModel]:
        """
        Get objects for bedset by identifier.

        :param identifier: bedset identifier
        :return: bedset objects
        """
        statement = select(BedSets).where(BedSets.id == identifier)
        return_dict = {}

        with Session(self._db_engine.engine) as session:
            bedset_object = session.scalar(statement)
            if not bedset_object:
                raise BedSetNotFoundError(f"Bedset with id: {identifier} not found.")
            for result in bedset_object.files:
                return_dict[result.name] = FileModel(
                    **result.__dict__,
                    object_id=f"bed.{identifier}.{result.name}",
                    access_methods=self.config.construct_access_method_list(
                        result.path
                    ),
                )

        return return_dict

    def get_statistics(self, identifier: str) -> BedSetStats:
        """
        Get statistics for bedset by identifier.

        :param identifier: bedset identifier
        :return: bedset statistics
        """
        statement = select(BedSets).where(BedSets.id == identifier)
        with Session(self._db_engine.engine) as session:
            bedset_object = session.scalar(statement)
            if not bedset_object:
                raise BedSetNotFoundError(f"Bedset with id: {identifier} not found.")
            return BedSetStats(
                mean=BedStatsModel(**bedset_object.bedset_means),
                sd=BedStatsModel(**bedset_object.bedset_standard_deviation),
            )

    def get_bedset_pep(self, identifier: str) -> dict:
        """
        Create pep file for a bedset.

        :param identifier: bedset identifier
        :return: pep dict
        """

        statement = select(BedFileBedSetRelation).where(
            BedFileBedSetRelation.bedset_id == identifier
        )

        with Session(self._db_engine.engine) as session:
            bedfile_bedset = session.scalars(statement)
            bedfiles = [res.bedfile for res in bedfile_bedset]

            if len(bedfiles) == 0:
                raise BedSetNotFoundError(identifier)
            else:
                bedfile_meta_list = []

                for bedfile in bedfiles:

                    try:
                        annotation = bedfile.annotations.__dict__
                    except AttributeError:
                        annotation = {}

                    bedfile_metadata = BedSetPEP(
                        sample_name=bedfile.id,
                        original_name=bedfile.name,
                        genome_alias=bedfile.genome_alias,
                        genome_digest=bedfile.genome_digest,
                        bed_compliance=bedfile.bed_compliance,
                        data_format=bedfile.data_format,
                        description=bedfile.description,
                        url=f"https://data2.bedbase.org/files/{bedfile.id[0]}/{bedfile.id[1]}/{bedfile.id}.bed.gz",
                        **annotation,
                    )
                    bedfile_meta_list.append(bedfile_metadata.model_dump())

                bedset = session.scalar(select(BedSets).where(BedSets.id == identifier))

                pep_config = {
                    "pep_version": "2.1.0",
                    "name": bedset.id,
                    "description": bedset.description,
                    "md5sum": bedset.md5sum,
                    "author": bedset.author,
                    "source": bedset.source,
                }

        return {
            "_config": pep_config,
            "_sample_dict": bedfile_meta_list,
            "_subsample_list": [],
        }

    def get_track_hub_file(self, identifier: str) -> str:
        """
        Get track hub file for bedset.

        :param identifier: bedset identifier
        :return: track hub file
        """
        statement = select(BedFileBedSetRelation).where(
            BedFileBedSetRelation.bedset_id == identifier
        )

        trackDb_txt = ""

        with Session(self._db_engine.engine) as session:
            bs2bf_objects = session.scalars(statement)
            if not bs2bf_objects:
                raise BedSetNotFoundError(f"Bedset with id: {identifier} not found.")

            relationship_objects = [relationship for relationship in bs2bf_objects]

            if len(relationship_objects) > 20:
                raise BedSetTrackHubLimitError(
                    "Number of bedfiles exceeds 20. Unable to process request for track hub."
                )

            for bs2bf_obj in relationship_objects:
                bed_obj = bs2bf_obj.bedfile

                try:
                    bigbed_url = None
                    for bedfile in bed_obj.files:
                        if bedfile.name == "bigbed_file":
                            bigbed_url = self.config.get_prefixed_uri(
                                postfix=bedfile.path, access_id="http"
                            )
                            break
                    if not bigbed_url:
                        _LOGGER.debug(
                            f"BigBed file for bedfile {bs2bf_obj.bedfile_id} not found."
                        )
                        continue
                except AttributeError:
                    _LOGGER.debug(
                        f"BigBed file for bedfile {bs2bf_obj.bedfile_id} not found."
                    )
                    continue
                trackDb_txt = (
                    trackDb_txt + f"track\t {bed_obj.name}\n"
                    "type\t bigBed\n"
                    f"bigDataUrl\t {bigbed_url} \n"
                    f"shortLabel\t {bed_obj.name}\n"
                    f"longLabel\t {bed_obj.description}\n"
                    "visibility\t full\n\n"
                )
        return trackDb_txt

    def create(
        self,
        identifier: str,
        name: str,
        bedid_list: List[str],
        description: str = None,
        statistics: bool = False,
        annotation: dict = None,
        plots: dict = None,
        upload_pephub: bool = False,
        upload_s3: bool = False,
        local_path: str = "",
        no_fail: bool = False,
        overwrite: bool = False,
        processed: bool = True,
    ) -> None:
        """
        Create bedset in the database.

        :param identifier: bedset identifier
        :param name: bedset name
        :param description: bedset description
        :param bedid_list: list of bed file identifiers
        :param statistics: calculate statistics for bedset
        :param annotation: bedset annotation (author, source)
        :param plots: dictionary with plots
        :param upload_pephub: upload bedset to pephub (create view in pephub)
        :param upload_s3: upload bedset to s3
        :param local_path: local path to the output files
        :param no_fail: do not raise an error if bedset already exists
        :param overwrite: overwrite the record in the database
        :param processed: flag to indicate that bedset is processed. [Default: True]
        :return: None
        """
        _LOGGER.info(f"Creating bedset '{identifier}'")

        if statistics:
            stats = self._calculate_statistics(bedid_list)
        else:
            stats = None
        if self.exists(identifier):
            if no_fail and not overwrite:
                _LOGGER.warning(
                    f"Bedset '{identifier}' already exists. no_fail=True. Skipping updating bedset."
                )
                return None

            if not overwrite:
                raise BedSetExistsError(
                    f"BEDset already exist in the database: {identifier}"
                )

            self.delete(identifier)

        if not isinstance(annotation, dict):
            annotation = {}

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
            summary=annotation.get("summary"),
            bedset_means=stats.mean.model_dump() if stats else None,
            bedset_standard_deviation=stats.sd.model_dump() if stats else None,
            md5sum=compute_md5sum_bedset(bedid_list),
            author=annotation.get("author"),
            source=annotation.get("source"),
            processed=processed,
        )

        if upload_s3:
            plots = BedSetPlots(**plots) if plots else BedSetPlots()
            plots = self.config.upload_files_s3(
                identifier, files=plots, base_path=local_path, type="bedsets"
            )

        try:
            with Session(self._db_engine.engine) as session:
                session.add(new_bedset)

                if no_fail:
                    bedid_list = list(set(bedid_list))
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
        except IntegrityError as _:
            raise BEDFileNotFoundError(
                "Failed to create bedset. One of the bedfiles does not exist."
            )
        except Exception as _:
            if not no_fail:
                raise BedBaseConfError("Failed to create bedset. SQL error.")

        _LOGGER.info(f"Bedset '{identifier}' was created successfully")
        return None

    def _calculate_statistics(self, bed_ids: List[str]) -> BedSetStats:
        """
        Calculate statistics for bedset.

        :param bed_ids: list of bed file identifiers
        :return: statistics
        """

        _LOGGER.info("Calculating bedset statistics")
        numeric_columns = BedStatsModel.model_fields

        bedset_sd = {}
        bedset_mean = {}
        with Session(self._db_engine.engine) as session:
            for column_name in numeric_columns:
                mean_bedset_statement = select(
                    func.round(
                        func.avg(getattr(BedStats, column_name)).cast(Numeric), 4
                    ).cast(Float)
                ).where(BedStats.id.in_(bed_ids))

                sd_bedset_statement = select(
                    func.round(
                        func.stddev(getattr(BedStats, column_name)).cast(Numeric),
                        4,
                    ).cast(Float)
                ).where(BedStats.id.in_(bed_ids))

                bedset_sd[column_name] = session.execute(sd_bedset_statement).one()[0]
                bedset_mean[column_name] = session.execute(mean_bedset_statement).one()[
                    0
                ]

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
        count_statement = select(func.count(BedSets.id))
        if query:
            query = query.strip()
            sql_search_str = f"%{query}%"
            statement = statement.where(
                or_(
                    BedSets.name.ilike(sql_search_str),
                    BedSets.description.ilike(sql_search_str),
                )
            )
            count_statement = count_statement.where(
                or_(
                    BedSets.name.ilike(sql_search_str),
                    BedSets.description.ilike(sql_search_str),
                )
            )

        with Session(self._db_engine.engine) as session:
            bedset_list = session.execute(statement.limit(limit).offset(offset))
            bedset_count = session.execute(count_statement).one()

        result_list = []
        for bedset_id in bedset_list:
            result_list.append(self.get(bedset_id[0]))
        return BedSetListResult(
            count=bedset_count[0],
            limit=limit,
            offset=offset,
            results=result_list,
        )

    def get_bedset_bedfiles(self, identifier: str) -> BedSetBedFiles:
        """
        Get list of bedfiles in bedset.

        :param identifier: bedset identifier

        :return: list of bedfiles
        """
        sub_statement = select(BedFileBedSetRelation.bedfile_id).where(
            BedFileBedSetRelation.bedset_id == identifier
        )
        statement = select(Bed).where(Bed.id.in_(sub_statement))

        with Session(self._db_engine.engine) as session:
            bedfiles_list = session.scalars(statement)
            results = [
                BedMetadataBasic(
                    **bedfile_obj.__dict__,
                    annotation=StandardMeta(
                        **(
                            bedfile_obj.annotations.__dict__
                            if bedfile_obj.annotations
                            else {}
                        )
                    ),
                )
                for bedfile_obj in bedfiles_list
            ]

        return BedSetBedFiles(
            count=len(results),
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

    def get_unprocessed(self, limit: int = 100, offset: int = 0) -> BedSetListResult:
        """
        Get unprocessed bedset from the database.

        :param limit: limit of results
        :param offset: offset of results

        :return: bedset metadata
        """

        with Session(self._db_engine.engine) as session:

            statement = (
                select(BedSets)
                .where(BedSets.processed.is_(False))
                .limit(limit)
                .offset(offset)
            )
            count_statement = select(func.count()).where(BedSets.processed.is_(False))

            count = session.execute(count_statement).one()[0]

            bedset_object_list = session.scalars(statement)

            results = []

            for bedset_obj in bedset_object_list:
                list_of_bedfiles = [
                    bedset_obj.bedfile_id for bedset_obj in bedset_obj.bedfiles
                ]

                results.append(
                    BedSetMetadata(
                        id=bedset_obj.id,
                        name=bedset_obj.name,
                        description=bedset_obj.description,
                        md5sum=bedset_obj.md5sum,
                        statistics=None,
                        plots=None,
                        bed_ids=list_of_bedfiles,
                        submission_date=bedset_obj.submission_date,
                        last_update_date=bedset_obj.last_update_date,
                        author=bedset_obj.author,
                        source=bedset_obj.source,
                    )
                )

        return BedSetListResult(
            count=count,
            limit=limit,
            offset=offset,
            results=results,
        )

    def add_bedfile(self, identifier: str, bedfile: str) -> None:
        raise NotImplementedError

    def delete_bedfile(self, identifier: str, bedfile: str) -> None:
        raise NotImplementedError
