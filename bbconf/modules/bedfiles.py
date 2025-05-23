import datetime
import os
from logging import getLogger
from typing import Dict, List, Union

import numpy as np
from geniml.bbclient import BBClient
from geniml.io import RegionSet
from geniml.search.backends import QdrantBackend
from gtars.models import RegionSet as GRegionSet
from pephubclient.exceptions import ResponseError
from pydantic import BaseModel
from qdrant_client.http.models import PointStruct
from qdrant_client.models import Distance, PointIdsList, VectorParams
from sqlalchemy import and_, delete, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased
from sqlalchemy.orm.attributes import flag_modified
from tqdm import tqdm

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.const import DEFAULT_LICENSE, PKG_NAME, ZARR_TOKENIZED_FOLDER
from bbconf.db_utils import (
    Bed,
    BedMetadata,
    BedStats,
    Files,
    GenomeRefStats,
    TokenizedBed,
    Universes,
)
from bbconf.exceptions import (
    BedBaseConfError,
    BedFIleExistsError,
    BEDFileNotFoundError,
    QdrantInstanceNotInitializedError,
    TokenizeFileExistsError,
    TokenizeFileNotExistError,
    UniverseNotFoundError,
)
from bbconf.models.bed_models import (
    BedClassification,
    BedEmbeddingResult,
    BedFiles,
    BedListResult,
    BedListSearchResult,
    BedMetadataAll,
    BedMetadataBasic,
    BedPEPHub,
    BedPEPHubRestrict,
    BedPlots,
    BedSetMinimal,
    BedStatsModel,
    FileModel,
    QdrantSearchResult,
    RefGenValidModel,
    RefGenValidReturnModel,
    StandardMeta,
    TokenizedBedResponse,
    TokenizedPathResponse,
    UniverseMetadata,
)

_LOGGER = getLogger(PKG_NAME)

QDRANT_GENOME = "hg38"


class BedAgentBedFile:
    """
    Class that represents a BED file in the Database.

    Provides methods to add, delete, get BED files and metadata from the database.
    """

    def __init__(self, config: BedBaseConfig, bbagent_obj=None):
        """
        :param config: config object with database and qdrant engine and credentials
        :param bbagent_obj: BedBaseAgent object (Parent object)
        """
        self._sa_engine = config.db_engine.engine
        self._db_engine = config.db_engine
        self._qdrant_engine = config.qdrant_engine
        self._boto3_client = config.boto3_client
        self._config = config
        self.bb_agent = bbagent_obj

    def get(self, identifier: str, full: bool = False) -> BedMetadataAll:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :param full: if True, return full metadata, including statistics, files, and raw metadata from pephub
        :return: project metadata
        """
        statement = select(Bed).where(and_(Bed.id == identifier))

        bed_plots = BedPlots()
        bed_files = BedFiles()

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")

            if full:
                for result in bed_object.files:
                    # PLOTS
                    if result.name in BedPlots.model_fields:
                        setattr(
                            bed_plots,
                            result.name,
                            FileModel(
                                **result.__dict__,
                                object_id=f"bed.{identifier}.{result.name}",
                                access_methods=self._config.construct_access_method_list(
                                    result.path
                                ),
                            ),
                        )
                    # FILES
                    elif result.name in BedFiles.model_fields:
                        (
                            setattr(
                                bed_files,
                                result.name,
                                FileModel(
                                    **result.__dict__,
                                    object_id=f"bed.{identifier}.{result.name}",
                                    access_methods=self._config.construct_access_method_list(
                                        result.path
                                    ),
                                ),
                            ),
                        )

                    else:
                        _LOGGER.error(
                            f"Unknown file type: {result.name}. And is not in the model fields. Skipping.."
                        )
                bed_stats = BedStatsModel(**bed_object.stats.__dict__)
                bed_bedsets = []
                for relation in bed_object.bedsets:
                    bed_bedsets.append(
                        BedSetMinimal(
                            id=relation.bedset.id,
                            description=relation.bedset.description,
                            name=relation.bedset.name,
                        )
                    )

                if bed_object.universe:
                    universe_meta = UniverseMetadata(**bed_object.universe.__dict__)
                else:
                    universe_meta = UniverseMetadata()
            else:
                bed_plots = None
                bed_files = None
                bed_stats = None
                universe_meta = None
                bed_bedsets = []

        try:
            if full:
                bed_metadata = BedPEPHubRestrict(
                    **self._config.phc.sample.get(
                        namespace=self._config.config.phc.namespace,
                        name=self._config.config.phc.name,
                        tag=self._config.config.phc.tag,
                        sample_name=identifier,
                    )
                )
            else:
                bed_metadata = None
        except Exception as e:
            _LOGGER.warning(f"Could not retrieve metadata from pephub. Error: {e}")
            bed_metadata = None

        return BedMetadataAll(
            id=bed_object.id,
            name=bed_object.name,
            stats=bed_stats,
            plots=bed_plots,
            files=bed_files,
            description=bed_object.description,
            submission_date=bed_object.submission_date,
            last_update_date=bed_object.last_update_date,
            raw_metadata=bed_metadata,
            genome_alias=bed_object.genome_alias,
            genome_digest=bed_object.genome_digest,
            bed_compliance=bed_object.bed_compliance,
            data_format=bed_object.data_format,
            is_universe=bed_object.is_universe,
            compliant_columns=bed_object.compliant_columns,
            non_compliant_columns=bed_object.non_compliant_columns,
            license_id=bed_object.license_id or DEFAULT_LICENSE,
            universe_metadata=universe_meta,
            bedsets=bed_bedsets,
            annotation=StandardMeta(
                **bed_object.annotations.__dict__ if bed_object.annotations else {}
            ),
        )

    def get_stats(self, identifier: str) -> BedStatsModel:
        """
        Get file statistics by identifier.

        :param identifier: bed file identifier

        :return: project statistics as BedStats object
        """
        statement = select(BedStats).where(and_(BedStats.id == identifier))

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
            bed_stats = BedStatsModel(**bed_object.__dict__)

        return bed_stats

    def get_plots(self, identifier: str) -> BedPlots:
        """
        Get file plots by identifier.

        :param identifier: bed file identifier
        :return: project plots
        """
        statement = select(Bed).where(and_(Bed.id == identifier))

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
            bed_plots = BedPlots()
            for result in bed_object.files:
                if result.name in BedPlots.model_fields:
                    setattr(
                        bed_plots,
                        result.name,
                        FileModel(
                            **result.__dict__,
                            object_id=f"bed.{identifier}.{result.name}",
                            access_methods=self._config.construct_access_method_list(
                                result.path
                            ),
                        ),
                    )
        return bed_plots

    def get_neighbours(
        self, identifier: str, limit: int = 10, offset: int = 0
    ) -> BedListSearchResult:
        """
        Get nearest neighbours of bed file from qdrant.

        :param identifier: bed file identifier
        :param limit: number of results to return
        :param offset: offset to start from

        :return: list of nearest neighbours
        """
        if not self.exists(identifier):
            raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
        s = identifier
        results = self._qdrant_engine.qd_client.query_points(
            collection_name=self._config.config.qdrant.file_collection,
            query="-".join([s[:8], s[8:12], s[12:16], s[16:20], s[20:]]),
            limit=limit,
            offset=offset,
        )
        result_list = []
        for result in results.points:
            result_id = result.id.replace("-", "")
            result_list.append(
                QdrantSearchResult(
                    id=result_id,
                    payload=result.payload,
                    score=result.score,
                    metadata=self.get(result_id, full=False),
                )
            )
        return BedListSearchResult(
            count=self.bb_agent.get_stats().bedfiles_number,
            limit=limit,
            offset=offset,
            results=result_list,
        )

    def get_files(self, identifier: str) -> BedFiles:
        """
        Get file files by identifier.

        :param identifier: bed file identifier
        :return: project files
        """
        statement = select(Bed).where(and_(Bed.id == identifier))

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
            bed_files = BedFiles()
            for result in bed_object.files:
                if result.name in BedFiles.model_fields:
                    name = result.name
                    setattr(
                        bed_files,
                        name,
                        FileModel(
                            **result.__dict__,
                            object_id=f"bed.{identifier}.{result.name}",
                            access_methods=self._config.construct_access_method_list(
                                result.path
                            ),
                        ),
                    )
        return bed_files

    def get_raw_metadata(self, identifier: str) -> BedPEPHub:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :return: project metadata
        """
        try:
            bed_metadata = self._config.phc.sample.get(
                namespace=self._config.config.phc.namespace,
                name=self._config.config.phc.name,
                tag=self._config.config.phc.tag,
                sample_name=identifier,
            )
        except Exception as e:
            _LOGGER.warning(f"Could not retrieve metadata from pephub. Error: {e}")
            bed_metadata = {}
        return BedPEPHubRestrict(**bed_metadata)

    def get_classification(self, identifier: str) -> BedClassification:
        """
        Get file classification by identifier.

        :param identifier: bed file identifier
        :return: project classification
        """
        statement = select(Bed).where(and_(Bed.id == identifier))

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
            bed_classification = BedClassification(**bed_object.__dict__)

        return bed_classification

    def get_objects(self, identifier: str) -> Dict[str, FileModel]:
        """
        Get all object related to bedfile

        :param identifier:  bed file identifier
        :return: project objects dict
        """
        statement = select(Bed).where(and_(Bed.id == identifier))
        return_dict = {}

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
            for result in bed_object.files:
                return_dict[result.name] = FileModel(**result.__dict__)

        return return_dict

    def get_embedding(self, identifier: str) -> BedEmbeddingResult:
        """
        Get bed file embedding of bed file from qdrant.

        :param identifier: bed file identifier
        :return: bed file embedding
        """
        if not self.exists(identifier):
            raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
        result = self._qdrant_engine.qd_client.retrieve(
            collection_name=self._config.config.qdrant.file_collection,
            ids=[identifier],
            with_vectors=True,
            with_payload=True,
        )
        if not result:
            raise BEDFileNotFoundError(
                f"Bed file with id: {identifier} not found in qdrant database."
            )
        return BedEmbeddingResult(
            identifier=identifier, embedding=result[0].vector, payload=result[0].payload
        )

    def get_ids_list(
        self,
        limit: int = 100,
        offset: int = 0,
        genome: str = None,
        bed_compliance: str = None,
    ) -> BedListResult:
        """
        Get list of bed file identifiers.

        :param limit: number of results to return
        :param offset: offset to start from
        :param genome: filter by genome
        :param bed_compliance: filter by bed type. e.g. 'bed6+4'

        :return: list of bed file identifiers
        """
        statement = select(Bed)
        count_statement = select(func.count(Bed.id))

        # TODO: make it generic, like in PEPhub
        if genome:
            statement = statement.where(and_(Bed.genome_alias == genome))
            count_statement = count_statement.where(and_(Bed.genome_alias == genome))

        if bed_compliance:
            statement = statement.where(and_(Bed.bed_compliance == bed_compliance))
            count_statement = count_statement.where(
                and_(Bed.bed_compliance == bed_compliance)
            )

        statement = statement.limit(limit).offset(offset)

        result_list = []
        with Session(self._sa_engine) as session:
            bed_ids = session.scalars(statement)
            count = session.execute(count_statement).one()

            for result in bed_ids:
                annotation = StandardMeta(
                    **result.annotations.__dict__ if result.annotations else {}
                )
                result_list.append(
                    BedMetadataBasic(**result.__dict__, annotation=annotation)
                )

        return BedListResult(
            count=count[0],
            limit=limit,
            offset=offset,
            results=result_list,
        )

    def get_reference_validation(self, identifier: str) -> RefGenValidReturnModel:
        """
        Get results of reference genome validation for the bed file.

        :param identifier: bed file identifier
        :return: reference genome validation results
        """

        if not self.exists(identifier):
            raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")

        with Session(self._sa_engine) as session:
            statement = select(GenomeRefStats).where(
                GenomeRefStats.bed_id == identifier
            )

            results = session.scalars(statement)

            result_list = []

            for result in results:
                result_list.append(
                    RefGenValidModel(
                        provided_genome=result.provided_genome,
                        compared_genome=result.compared_genome,
                        xs=result.xs,
                        oobr=result.oobr,
                        sequence_fit=result.sequence_fit,
                        assigned_points=result.assigned_points,
                        tier_ranking=result.tier_ranking,
                    )
                )

        return RefGenValidReturnModel(
            id=identifier,
            provided_genome=result.provided_genome,
            compared_genome=result_list,
        )

    def add(
        self,
        identifier: str,
        stats: dict,
        metadata: dict = None,
        plots: dict = None,
        files: dict = None,
        classification: dict = None,
        ref_validation: Union[Dict[str, BaseModel], None] = None,
        license_id: str = DEFAULT_LICENSE,
        upload_qdrant: bool = False,
        upload_pephub: bool = False,
        upload_s3: bool = False,
        local_path: str = None,
        overwrite: bool = False,
        nofail: bool = False,
        processed: bool = True,
    ) -> None:
        """
        Add bed file to the database.

        :param identifier: bed file identifier
        :param stats: bed file results {statistics, plots, files, metadata}
        :param metadata: bed file metadata (will be saved in pephub)
        :param plots: bed file plots
        :param files: bed file files
        :param classification: bed file classification
        :param ref_validation: reference validation data.  RefGenValidModel
        :param license_id: bed file license id (default: 'DUO:0000042'). Full list of licenses:
            https://raw.githubusercontent.com/EBISPOT/DUO/master/duo.csv
        :param upload_qdrant: add bed file to qdrant indexs
        :param upload_pephub: add bed file to pephub
        :param upload_s3: upload files to s3
        :param local_path: local path to the output files
        :param overwrite: overwrite bed file if it already exists
        :param nofail: do not raise an error for error in pephub/s3/qdrant or record exsist and not overwrite
        :param processed: true if bedfile was processed and statistics and plots were calculated
        :return: None
        """
        _LOGGER.info(f"Adding bed file to database. bed_id: {identifier}")

        if self.exists(identifier):
            _LOGGER.warning(f"Bed file with id: {identifier} exists in the database.")
            if not overwrite:
                bed_metadata = StandardMeta(**metadata)

                ## OLD:
                # self._update_sources(
                #     identifier=identifier,
                #     global_sample_id=metadata_standard.global_sample_id,
                #     global_experiment_id=metadata_standard.global_experiment_id,
                # )

                with Session(self._sa_engine) as session:
                    statement = select(Bed).where(Bed.id == identifier)
                    bed_object = session.scalar(statement)
                    self._update_metadata(
                        sa_session=session,
                        bed_object=bed_object,
                        bed_metadata=bed_metadata,
                    )
                if not nofail:
                    raise BedFIleExistsError(
                        f"Bed file with id: {identifier} already exists in the database."
                    )
                _LOGGER.warning("Overwrite set to False. Skipping..")
                return None
            else:
                self.delete(identifier)

        if license_id not in self.bb_agent.list_of_licenses:
            raise BedBaseConfError(
                f"License: {license_id} is not in the list of licenses. Please provide a valid license."
                f"List of licenses: {self.bb_agent.list_of_licenses}"
            )

        stats = BedStatsModel(**stats)
        # TODO: we should not check for specific keys, of the plots!
        plots = BedPlots(**plots)
        files = BedFiles(**files)
        bed_metadata = StandardMeta(**metadata)

        classification = BedClassification(**classification)
        if upload_pephub:
            pephub_metadata = BedPEPHub(**metadata)
            try:
                self.upload_pephub(
                    identifier,
                    pephub_metadata.model_dump(exclude=set("input_file")),
                    overwrite,
                )
            except Exception as e:
                _LOGGER.warning(
                    f"Could not upload to pephub. Error: {e}. nofail: {nofail}"
                )
                upload_pephub = False
                if not nofail:
                    raise e
        else:
            _LOGGER.info("upload_pephub set to false. Skipping pephub..")

        if upload_qdrant:
            if classification.genome_alias == "hg38":
                _LOGGER.info(f"Uploading bed file to qdrant.. [{identifier}]")
                self.upload_file_qdrant(
                    identifier,
                    files.bed_file.path,
                    bed_metadata.model_dump(exclude_none=False),
                )
                _LOGGER.info(f"File uploaded to qdrant. {identifier}")
            else:
                _LOGGER.warning(
                    f"Could not upload to qdrant. Genome: {classification.genome_alias} is not supported."
                )
        else:
            _LOGGER.info("upload_qdrant set to false. Skipping qdrant..")

        # Upload files to s3
        if upload_s3:
            if files:
                files = self._config.upload_files_s3(
                    identifier, files=files, base_path=local_path, type="files"
                )

            if plots:
                plots = self._config.upload_files_s3(
                    identifier, files=plots, base_path=local_path, type="plots"
                )
        with Session(self._sa_engine) as session:
            new_bed = Bed(
                id=identifier,
                **classification.model_dump(),
                description=bed_metadata.description,
                license_id=license_id,
                indexed=upload_qdrant,
                pephub=upload_pephub,
                processed=processed,
            )
            session.add(new_bed)
            if upload_s3:
                for k, v in files:
                    if v:
                        new_file = Files(
                            **v.model_dump(
                                exclude_none=True,
                                exclude_unset=True,
                                exclude={"object_id", "access_methods"},
                            ),
                            bedfile_id=identifier,
                            type="file",
                        )
                        session.add(new_file)
                for k, v in plots:
                    if v:
                        new_plot = Files(
                            **v.model_dump(
                                exclude_none=True,
                                exclude_unset=True,
                                exclude={"object_id", "access_methods"},
                            ),
                            bedfile_id=identifier,
                            type="plot",
                        )
                        session.add(new_plot)

            new_bedstat = BedStats(**stats.model_dump(), id=identifier)
            new_metadata = BedMetadata(
                **bed_metadata.model_dump(exclude={"description"}), id=identifier
            )

            session.add(new_bedstat)
            session.add(new_metadata)

            if ref_validation:
                for ref_gen_check, data in ref_validation.items():
                    new_gen_ref = GenomeRefStats(
                        **RefGenValidModel(
                            **data.model_dump(),
                            provided_genome=classification.genome_alias,
                            compared_genome=ref_gen_check,
                        ).model_dump(),
                        bed_id=identifier,
                    )
                    session.add(new_gen_ref)
            session.commit()

        return None

    def update(
        self,
        identifier: str,
        stats: Union[dict, None] = None,
        metadata: Union[dict, None] = None,
        plots: Union[dict, None] = None,
        files: Union[dict, None] = None,
        classification: Union[dict, None] = None,
        ref_validation: Union[Dict[str, BaseModel], None] = None,
        license_id: str = DEFAULT_LICENSE,
        upload_qdrant: bool = True,
        upload_pephub: bool = True,
        upload_s3: bool = True,
        local_path: str = None,
        overwrite: bool = False,
        nofail: bool = False,
        processed: bool = True,
    ) -> None:
        """
        Update bed file to the database.

        :param identifier: bed file identifier
        :param stats: bed file results {statistics, plots, files, metadata}
        :param metadata: bed file metadata (will be saved in pephub)
        :param plots: bed file plots
        :param files: bed file files
        :param classification: bed file classification
        :param ref_validation: reference validation data.  RefGenValidModel
        :param license_id: bed file license id (default: 'DUO:0000042').
        :param upload_qdrant: add bed file to qdrant indexs
        :param upload_pephub: add bed file to pephub
        :param upload_s3: upload files to s3
        :param local_path: local path to the output files
        :param overwrite: overwrite bed file if it already exists
        :param nofail: do not raise an error for error in pephub/s3/qdrant or record exsist and not overwrite
        :param processed: true if bedfile was processed and statistics and plots were calculated
        :return: None
        """
        if not self.exists(identifier):
            raise BEDFileNotFoundError(
                f"Bed file with id: {identifier} not found. Cannot update."
            )
        _LOGGER.info(f"Updating bed file: '{identifier}'")

        if license_id not in self.bb_agent.list_of_licenses and not license_id:
            raise BedBaseConfError(
                f"License: {license_id} is not in the list of licenses. Please provide a valid license."
                f"List of licenses: {self.bb_agent.list_of_licenses}"
            )

        stats = BedStatsModel(**stats if stats else {})
        plots = BedPlots(**plots if plots else {})
        files = BedFiles(**files if files else {})
        bed_metadata = StandardMeta(**metadata if metadata else {})
        classification = BedClassification(**classification if classification else {})

        if upload_pephub:
            metadata = BedPEPHub(**metadata)
            try:
                self.update_pephub(identifier, metadata.model_dump(), overwrite)
            except Exception as e:
                _LOGGER.warning(
                    f"Could not upload to pephub. Error: {e}. nofail: {nofail}"
                )
                if not nofail:
                    raise e
        else:
            _LOGGER.info("upload_pephub set to false. Skipping pephub..")

        if upload_qdrant:
            if classification.genome_alias == "hg38":
                _LOGGER.info(f"Uploading bed file to qdrant.. [{identifier}]")
                self.upload_file_qdrant(
                    identifier,
                    files.bed_file.path,
                    bed_metadata.model_dump(exclude_none=False),
                )
                _LOGGER.info(f"File uploaded to qdrant. {identifier}")
            else:
                _LOGGER.warning(
                    f"Could not upload to qdrant. Genome: {classification.genome_alias} is not supported."
                )

        with Session(self._sa_engine) as session:
            bed_statement = select(Bed).where(and_(Bed.id == identifier))
            bed_object = session.scalar(bed_statement)

            self._update_classification(
                sa_session=session, bed_object=bed_object, classification=classification
            )

            self._update_metadata(
                sa_session=session,
                bed_object=bed_object,
                bed_metadata=bed_metadata,
            )
            self._update_stats(sa_session=session, bed_object=bed_object, stats=stats)

            if upload_s3:
                self._update_plots(
                    sa_session=session,
                    bed_object=bed_object,
                    plots=plots,
                    local_path=local_path,
                )
                self._update_files(
                    sa_session=session,
                    bed_object=bed_object,
                    files=files,
                    local_path=local_path,
                )

            self._update_ref_validation(
                sa_session=session, bed_object=bed_object, ref_validation=ref_validation
            )

            bed_object.processed = processed
            bed_object.indexed = upload_qdrant
            bed_object.last_update_date = datetime.datetime.now(datetime.timezone.utc)

            session.commit()

        return None

    @staticmethod
    def _update_classification(
        sa_session: Session, bed_object: Bed, classification: BedClassification
    ) -> None:
        """
        Update bed file classification

        :param sa_session: sqlalchemy session
        :param bed_object: bed sqlalchemy object
        :param classification: bed file classification as BedClassification object

        :return: None
        """
        classification_dict = classification.model_dump(
            exclude_defaults=True, exclude_none=True, exclude_unset=True
        )
        for k, v in classification_dict.items():
            setattr(bed_object, k, v)

        sa_session.commit()

    @staticmethod
    def _update_stats(
        sa_session: Session, bed_object: Bed, stats: BedStatsModel
    ) -> None:
        """
        Update bed file statistics

        :param sa_session: sqlalchemy session
        :param bed_object: bed sqlalchemy object
        :param stats: bed file statistics as BedStatsModel object
        :return: None
        """

        stats_dict = stats.model_dump(
            exclude_defaults=True, exclude_none=True, exclude_unset=True
        )
        if not bed_object.stats:
            new_bedstat = BedStats(**stats.model_dump(), id=bed_object.id)
            sa_session.add(new_bedstat)
        else:
            for k, v in stats_dict.items():
                setattr(bed_object.stats, k, v)

        sa_session.commit()

    def _update_metadata(
        self, sa_session: Session, bed_object: Bed, bed_metadata: StandardMeta
    ) -> None:
        """
        Update bed file metadata

        :param sa_session: sqlalchemy session
        :param bed_object: bed sqlalchemy object
        :param bed_metadata: bed file metadata as StandardMeta object

        :return: None
        """

        self._update_sources(
            identifier=bed_object.id,
            global_sample_id=bed_metadata.global_sample_id,
            global_experiment_id=bed_metadata.global_experiment_id,
        )

        bed_metadata.global_experiment_id = None
        bed_metadata.global_sample_id = None

        metadata_dict = bed_metadata.model_dump(
            exclude_defaults=True, exclude_none=True, exclude_unset=True
        )
        if not bed_object.annotations:
            new_metadata = BedMetadata(
                **bed_metadata.model_dump(exclude={"description"}), id=bed_object.id
            )
            sa_session.add(new_metadata)
        else:
            for k, v in metadata_dict.items():
                setattr(bed_object.annotations, k, v)

        sa_session.commit()

    def _update_plots(
        self,
        sa_session: Session,
        bed_object: Bed,
        plots: BedPlots,
        local_path: str = None,
    ) -> None:
        """
        Update bed file plots

        :param sa_session: sqlalchemy session
        :param bed_object: bed sqlalchemy object
        :param plots: bed file plots
        :param local_path: local path to the output files
        """

        _LOGGER.info("Updating bed file plots..")
        if plots:
            plots = self._config.upload_files_s3(
                bed_object.id, files=plots, base_path=local_path, type="plots"
            )
        plots_dict = plots.model_dump(
            exclude_defaults=True, exclude_none=True, exclude_unset=True
        )
        if not plots_dict:
            return None

        for k, v in plots:
            if v:
                new_plot = Files(
                    **v.model_dump(
                        exclude_none=True,
                        exclude_unset=True,
                        exclude={"object_id", "access_methods"},
                    ),
                    bedfile_id=bed_object.id,
                    type="plot",
                )
                try:
                    sa_session.add(new_plot)
                    sa_session.commit()
                except IntegrityError as _:
                    sa_session.rollback()
                    _LOGGER.debug(
                        f"Plot with name: {v.name} already exists. Updating.."
                    )

        return None

    def _update_files(
        self,
        sa_session: Session,
        bed_object: Bed,
        files: BedFiles,
        local_path: str = None,
    ) -> None:
        """
        Update bed files

        :param sa_session: sqlalchemy session
        :param bed_object: bed sqlalchemy object
        :param files: bed file files
        """

        _LOGGER.info("Updating bed files..")
        if files:
            files = self._config.upload_files_s3(
                bed_object.id, files=files, base_path=local_path, type="files"
            )

        files_dict = files.model_dump(
            exclude_defaults=True, exclude_none=True, exclude_unset=True
        )
        if not files_dict:
            return None

        for k, v in files:
            if v:
                new_file = Files(
                    **v.model_dump(
                        exclude_none=True,
                        exclude_unset=True,
                        exclude={"object_id", "access_methods"},
                    ),
                    bedfile_id=bed_object.id,
                    type="file",
                )

                try:
                    sa_session.add(new_file)
                    sa_session.commit()
                except IntegrityError as _:
                    sa_session.rollback()
                    _LOGGER.debug(
                        f"File with name: {v.name} already exists. Updating.."
                    )

    @staticmethod
    def _update_ref_validation(
        sa_session: Session, bed_object: Bed, ref_validation: Dict[str, BaseModel]
    ) -> None:
        """
        Update reference validation data

        ! This function won't update the reference validation data, if it exists, it will skip it.

        :param sa_session: sqlalchemy session
        :param bed_object: bed sqlalchemy object
        :param ref_validation: bed file metadata
        """

        if not ref_validation:
            return None

        _LOGGER.info("Updating reference validation data..")

        for ref_gen_check, data in ref_validation.items():
            new_gen_ref = GenomeRefStats(
                **RefGenValidModel(
                    **data.model_dump(),
                    provided_genome=bed_object.genome_alias,
                    compared_genome=ref_gen_check,
                ).model_dump(),
                bed_id=bed_object.id,
            )
            try:
                sa_session.add(new_gen_ref)
                sa_session.commit()
            except IntegrityError as _:
                sa_session.rollback()
                _LOGGER.info(
                    f"Reference validation exists for BED id: {bed_object.id} and ref_gen_check."
                )

        return None

    def delete(self, identifier: str) -> None:
        """
        Delete bed file from the database.

        :param identifier: bed file identifier
        :return: None
        """
        _LOGGER.info(f"Deleting bed file from database. bed_id: {identifier}")
        if not self.exists(identifier):
            raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")

        with Session(self._sa_engine) as session:
            statement = select(Bed).where(and_(Bed.id == identifier))
            bed_object = session.scalar(statement)

            files = [FileModel(**k.__dict__) for k in bed_object.files]
            delete_pephub = bed_object.pephub
            delete_qdrant = bed_object.indexed

            session.delete(bed_object)
            session.commit()

        if delete_pephub:
            self.delete_pephub_sample(identifier)
        if delete_qdrant:
            self.delete_qdrant_point(identifier)
        self._config.delete_files_s3(files)

    def upload_pephub(self, identifier: str, metadata: dict, overwrite: bool = False):
        if not metadata:
            _LOGGER.warning("No metadata provided. Skipping pephub upload..")
            return False
        self._config.phc.sample.create(
            namespace=self._config.config.phc.namespace,
            name=self._config.config.phc.name,
            tag=self._config.config.phc.tag,
            sample_name=identifier,
            sample_dict=metadata,
            overwrite=overwrite,
        )

    def update_pephub(
        self, identifier: str, metadata: dict, overwrite: bool = False
    ) -> None:
        try:
            if not metadata:
                _LOGGER.warning("No metadata provided. Skipping pephub upload..")
                return None
            self._config.phc.sample.update(
                namespace=self._config.config.phc.namespace,
                name=self._config.config.phc.name,
                tag=self._config.config.phc.tag,
                sample_name=identifier,
                sample_dict=metadata,
            )
        except ResponseError as e:
            _LOGGER.warning(f"Could not update pephub. Error: {e}")

    def delete_pephub_sample(self, identifier: str):
        """
        Delete sample from pephub

        :param identifier: bed file identifier
        """
        try:
            self._config.phc.sample.remove(
                namespace=self._config.config.phc.namespace,
                name=self._config.config.phc.name,
                tag=self._config.config.phc.tag,
                sample_name=identifier,
            )
        except ResponseError as e:
            _LOGGER.warning(f"Could not delete from pephub. Error: {e}")

    def upload_file_qdrant(
        self,
        bed_id: str,
        bed_file: Union[str, RegionSet],
        payload: dict = None,
    ) -> None:
        """
        Convert bed file to vector and add it to qdrant database

        !Warning: only hg38 genome can be added to qdrant!

        :param bed_id: bed file id
        :param bed_file: path to the bed file, or RegionSet object
        :param payload: additional metadata to store alongside vectors
        :return: None
        """

        _LOGGER.debug(f"Adding bed file to qdrant. bed_id: {bed_id}")

        if not isinstance(self._qdrant_engine, QdrantBackend):
            raise QdrantInstanceNotInitializedError("Could not upload file.")

        bed_embedding = self._embed_file(bed_file)

        self._qdrant_engine.load(
            ids=[bed_id],
            vectors=bed_embedding,
            payloads=[{**payload}],
        )
        return None

    def _embed_file(self, bed_file: Union[str, RegionSet]) -> np.ndarray:
        """
        Create embeding for bed file

        :param bed_file: bed file path or regionset
        :param bed_file: path to the bed file, or RegionSet object

        :return np array of embeddings
        """
        if self._qdrant_engine is None:
            raise QdrantInstanceNotInitializedError
        if not self._config.r2v:
            raise BedBaseConfError(
                "Could not add add region to qdrant. Invalid type, or path. "
            )

        if isinstance(bed_file, str):
            # Use try if file is corrupted. In Python RegionSet we have functionality to tackle this problem
            try:
                bed_region_set = GRegionSet(bed_file)
            except RuntimeError as _:
                bed_region_set = RegionSet(bed_file)
        elif isinstance(bed_file, RegionSet) or isinstance(bed_file, GRegionSet):
            bed_region_set = bed_file
        else:
            raise BedBaseConfError(
                "Could not add add region to qdrant. Invalid type, or path. "
            )
        bed_embedding = np.mean(self._config.r2v.encode(bed_region_set), axis=0)
        vec_dim = bed_embedding.shape[0]
        return bed_embedding.reshape(1, vec_dim)

    def text_to_bed_search(
        self, query: str, limit: int = 10, offset: int = 0
    ) -> BedListSearchResult:
        """
        Search for bed files by text query in qdrant database

        :param query: text query
        :param limit: number of results to return
        :param offset: offset to start from

        :return: list of bed file metadata
        """
        _LOGGER.info(f"Looking for: {query}")

        results = self._config.bivec.query_search(query, limit=limit, offset=offset)
        results_list = []
        for result in results:
            result_id = result["id"].replace("-", "")
            try:
                result_meta = self.get(result_id)
            except BEDFileNotFoundError as e:
                _LOGGER.warning(
                    f"Could not retrieve metadata for bed file: {result_id}. Error: {e}"
                )
                continue
            if result_meta:
                results_list.append(QdrantSearchResult(**result, metadata=result_meta))
        return BedListSearchResult(
            count=self.bb_agent.get_stats().bedfiles_number,
            limit=limit,
            offset=offset,
            results=results_list,
        )

    def bed_to_bed_search(
        self,
        region_set: RegionSet,
        limit: int = 10,
        offset: int = 0,
    ) -> BedListSearchResult:
        results = self._config.b2bsi.query_search(
            region_set, limit=limit, offset=offset
        )
        results_list = []
        for result in results:
            result_id = result["id"].replace("-", "")
            try:
                result_meta = self.get(result_id)
            except BEDFileNotFoundError as e:
                _LOGGER.warning(
                    f"Could not retrieve metadata for bed file: {result_id}. Error: {e}"
                )
                continue
            if result_meta:
                results_list.append(QdrantSearchResult(**result, metadata=result_meta))
        return BedListSearchResult(
            count=self.bb_agent.get_stats().bedfiles_number,
            limit=limit,
            offset=offset,
            results=results_list,
        )

    def sql_search(
        self, query: str, limit: int = 10, offset: int = 0
    ) -> BedListSearchResult:
        """
        Search for bed files by using sql exact search.
        This search will search files by id, name, and description

        :param query: text query
        :param limit: number of results to return
        :param offset: offset to start from

        :return: list of bed file metadata
        """
        _LOGGER.debug(f"Looking for: {query}")

        sql_search_str = f"%{query}%"
        with Session(self._sa_engine) as session:
            statement = (
                select(Bed)
                .where(
                    or_(
                        Bed.id.ilike(sql_search_str),
                        Bed.name.ilike(sql_search_str),
                        Bed.description.ilike(sql_search_str),
                    )
                )
                .limit(limit)
                .offset(offset)
            )
            bed_objects = session.scalars(statement)
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
                for bedfile_obj in bed_objects
            ]
            result_list = [
                QdrantSearchResult(id=result.id, score=1, metadata=result)
                for result in results
            ]

        return BedListSearchResult(
            count=self._sql_search_count(query),
            limit=limit,
            offset=offset,
            results=result_list,
        )

    def _sql_search_count(self, query: str) -> int:
        """
        Get number of total found files in the database.

        :param query: text query

        :return: number of found files
        """
        sql_search_str = f"%{query}%"
        with Session(self._sa_engine) as session:
            statement = (
                select(func.count())
                .select_from(Bed)
                .where(
                    or_(
                        Bed.id.ilike(sql_search_str),
                        Bed.name.ilike(sql_search_str),
                        Bed.description.ilike(sql_search_str),
                    )
                )
            )
            count = session.execute(statement).one()
        return count[0]

    def reindex_qdrant(self, batch: int = 100) -> None:
        """
        Re-upload all files to quadrant.
        !Warning: only hg38 genome can be added to qdrant!

        If you want to fully reindex/reupload to qdrant, first delete collection and create new one.

        Upload all files to qdrant.

        :param batch: number of files to upload in one batch
        """
        bb_client = BBClient()

        annotation_result = self.get_ids_list(
            limit=100000, genome=QDRANT_GENOME, offset=0
        )

        if not annotation_result.results:
            _LOGGER.error("No bed files found.")
            return None
        results = annotation_result.results

        with tqdm(total=len(results), position=0, leave=True) as pbar:
            points_list = []
            processed_number = 0
            for record in results:
                try:
                    bed_region_set_obj = GRegionSet(bb_client.seek(record.id))
                except FileNotFoundError:
                    bed_region_set_obj = bb_client.load_bed(record.id)

                pbar.set_description(f"Processing file: {record.id}")

                file_embedding = self._embed_file(bed_region_set_obj)
                points_list.append(
                    PointStruct(
                        id=record.id,
                        vector=file_embedding.tolist()[0],
                        payload=(
                            record.annotation.model_dump() if record.annotation else {}
                        ),
                    )
                )
                processed_number += 1
                if processed_number % batch == 0:
                    pbar.set_description(f"Uploading points to qdrant using batch...")
                    operation_info = self._config.qdrant_engine.qd_client.upsert(
                        collection_name=self._config.config.qdrant.file_collection,
                        points=points_list,
                    )
                    pbar.write("Uploaded batch to qdrant.")
                    points_list = []
                    assert operation_info.status == "completed"

                pbar.write(f"File: {record.id} successfully indexed.")
                pbar.update(1)

        _LOGGER.info(f"Uploading points to qdrant using batches...")
        operation_info = self._config.qdrant_engine.qd_client.upsert(
            collection_name=self._config.config.qdrant.file_collection,
            points=points_list,
        )
        assert operation_info.status == "completed"
        return None

    def delete_qdrant_point(self, identifier: str) -> None:
        """
        Delete bed file from qdrant.

        :param identifier: bed file identifier
        :return: None
        """

        result = self._config.qdrant_engine.qd_client.delete(
            collection_name=self._config.config.qdrant.file_collection,
            points_selector=PointIdsList(
                points=[identifier],
            ),
        )
        return result

    def create_qdrant_collection(self) -> bool:
        """
        Create qdrant collection for bed files.
        """
        return self._config.qdrant_engine.qd_client.create_collection(
            collection_name=self._config.config.qdrant.file_collection,
            vectors_config=VectorParams(size=100, distance=Distance.DOT),
        )

    def exists(self, identifier: str) -> bool:
        """
        Check if bed file exists in the database.

        :param identifier: bed file identifier
        :return: True if bed file exists, False otherwise
        """
        statement = select(Bed).where(and_(Bed.id == identifier))

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                return False
            return True

    def exists_universe(self, identifier: str) -> bool:
        """
        Check if universe exists in the database.

        :param identifier: universe identifier

        :return: True if universe exists, False otherwise
        """
        statement = select(Universes).where(and_(Universes.id == identifier))

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                return False
            return True

    def add_universe(
        self, bedfile_id: str, bedset_id: str = None, construct_method: str = None
    ) -> str:
        """
        Add universe to the database.

        :param bedfile_id: bed file identifier
        :param bedset_id: bedset identifier
        :param construct_method: method used to construct the universe

        :return: universe identifier.
        """

        if not self.exists(bedfile_id):
            raise BEDFileNotFoundError
        with Session(self._sa_engine) as session:
            new_univ = Universes(
                id=bedfile_id, bedset_id=bedset_id, method=construct_method
            )
            session.add(new_univ)
            session.commit()

        _LOGGER.info(f"Universe added to the database successfully. id: {bedfile_id}")
        return bedfile_id

    def delete_universe(self, identifier: str) -> None:
        """
        Delete universe from the database.

        :param identifier: universe identifier
        :return: None
        """
        if not self.exists_universe(identifier):
            raise UniverseNotFoundError(f"Universe not found. id: {identifier}")

        with Session(self._sa_engine) as session:
            statement = delete(Universes).where(and_(Universes.id == identifier))
            session.execute(statement)
            session.commit()

    def add_tokenized(
        self, bed_id: str, universe_id: str, token_vector: list, overwrite: bool = False
    ) -> str:
        """
        Add tokenized bed file to the database

        :param bed_id: bed file identifier
        :param universe_id: universe identifier
        :param token_vector: list of tokens
        :param overwrite: overwrite tokenized file if it already exists

        :return: token path
        """

        with Session(self._sa_engine) as session:
            if not self.exists_universe(universe_id):
                raise UniverseNotFoundError(
                    f"Universe not found in the database. id: {universe_id}"
                    f"Please add universe first."
                )

            if self.exist_tokenized(bed_id, universe_id):
                if not overwrite:
                    if not overwrite:
                        raise TokenizeFileExistsError(
                            "Tokenized file already exists in the database. "
                            "Set overwrite to True to overwrite it."
                        )
                    else:
                        self.delete_tokenized(bed_id, universe_id)

            path = self._add_zarr_s3(
                bed_id=bed_id,
                universe_id=universe_id,
                tokenized_vector=token_vector,
                overwrite=overwrite,
            )
            path = os.path.join(f"s3://{self._config.config.s3.bucket}", path)
            new_token = TokenizedBed(bed_id=bed_id, universe_id=universe_id, path=path)

            session.add(new_token)
            session.commit()
        return path

    def _add_zarr_s3(
        self,
        universe_id: str,
        bed_id: str,
        tokenized_vector: list,
        overwrite: bool = False,
    ) -> str:
        """
        Add zarr file to the database

        :param universe_id: universe identifier
        :param bed_id: bed file identifier
        :param tokenized_vector: tokenized vector

        :return: zarr path
        """
        univers_group = self._config.zarr_root.require_group(universe_id)

        if not univers_group.get(bed_id):
            _LOGGER.info("Saving tokenized vector to s3")
            path = univers_group.create_dataset(bed_id, data=tokenized_vector).path
        elif overwrite:
            _LOGGER.info("Overwriting tokenized vector in s3")
            path = univers_group.create_dataset(
                bed_id, data=tokenized_vector, overwrite=True
            ).path
        else:
            raise TokenizeFileExistsError(
                "Tokenized file already exists in the database. "
                "Set overwrite to True to overwrite it."
            )

        return str(os.path.join(ZARR_TOKENIZED_FOLDER, path))

    def get_tokenized(self, bed_id: str, universe_id: str) -> TokenizedBedResponse:
        """
        Get zarr file from the database

        :param bed_id: bed file identifier
        :param universe_id: universe identifier

        :return: zarr path
        """
        if not self.exist_tokenized(bed_id, universe_id):
            raise TokenizeFileNotExistError("Tokenized file not found in the database.")
        univers_group = self._config.zarr_root.require_group(universe_id)

        return TokenizedBedResponse(
            universe_id=universe_id,
            bed_id=bed_id,
            tokenized_bed=list(univers_group[bed_id]),
        )

    def delete_tokenized(self, bed_id: str, universe_id: str) -> None:
        """
        Delete tokenized bed file from the database

        :param bed_id: bed file identifier
        :param universe_id: universe identifier

        :return: None
        """
        if not self.exist_tokenized(bed_id, universe_id):
            raise TokenizeFileNotExistError("Tokenized file not found in the database.")
        univers_group = self._config.zarr_root.require_group(universe_id)

        del univers_group[bed_id]

        with Session(self._sa_engine) as session:
            statement = delete(TokenizedBed).where(
                and_(
                    TokenizedBed.bed_id == bed_id,
                    TokenizedBed.universe_id == universe_id,
                )
            )
            session.execute(statement)
            session.commit()

        return None

    def _get_tokenized_path(self, bed_id: str, universe_id: str) -> str:
        """
        Get tokenized path to tokenized file

        :param bed_id: bed file identifier
        :param universe_id: universe identifier

        :return: token path
        """
        if not self.exist_tokenized(bed_id, universe_id):
            raise TokenizeFileNotExistError("Tokenized file not found in the database.")

        with Session(self._sa_engine) as session:
            statement = select(TokenizedBed).where(
                and_(
                    TokenizedBed.bed_id == bed_id,
                    TokenizedBed.universe_id == universe_id,
                ),
            )
            tokenized_object = session.scalar(statement)
            return str(tokenized_object.path)

    def exist_tokenized(self, bed_id: str, universe_id: str) -> bool:
        """
        Check if tokenized bed file exists in the database

        :param bed_id: bed file identifier
        :param universe_id: universe identifier

        :return: bool
        """
        with Session(self._sa_engine) as session:
            statement = select(TokenizedBed).where(
                and_(
                    TokenizedBed.bed_id == bed_id,
                    TokenizedBed.universe_id == universe_id,
                )
            )
            tokenized_object = session.scalar(statement)
            if not tokenized_object:
                return False
            return True

    def get_tokenized_link(
        self, bed_id: str, universe_id: str
    ) -> TokenizedPathResponse:
        """
        Get tokenized link to tokenized file

        :param bed_id: bed file identifier
        :param universe_id: universe identifier

        :return: token link
        :raises: TokenizeFileNotExistError
        """
        file_path = self._get_tokenized_path(bed_id, universe_id)

        return TokenizedPathResponse(
            endpoint_url=self._config.config.s3.endpoint_url,
            file_path=file_path,
            bed_id=bed_id,
            universe_id=universe_id,
        )

    def get_missing_plots(
        self, plot_name: str, limit: int = 1000, offset: int = 0
    ) -> List[str]:
        """
        Get list of bed files that are missing plot

        :param plot_name: plot name
        :param limit: number of results to return
        :param offset: offset to start from

        :return: list of bed file identifiers
        """
        if plot_name not in list(BedPlots.model_fields.keys()):
            raise BedBaseConfError(
                f"Plot name: {plot_name} is not valid. Valid names: {list(BedPlots.model_fields.keys())}"
            )

        with Session(self._sa_engine) as session:
            # Alias for subquery
            t2_alias = aliased(Files)

            # Define the subquery
            subquery = select(t2_alias).where(t2_alias.name == plot_name).subquery()

            query = (
                select(Bed.id)
                .outerjoin(subquery, Bed.id == subquery.c.bedfile_id)
                .where(subquery.c.bedfile_id.is_(None))
                .limit(limit)
                .offset(offset)
            )

            results = session.scalars(query)

            results = [result for result in results]

        return results

    def get_missing_stats(self, limit: int = 1000, offset: int = 0) -> List[str]:
        """
        Get list of bed files that are missing statistics

        :param limit: number of results to return
        :param offset: offset to start from

        :return: list of bed file identifiers
        """

        with Session(self._sa_engine) as session:
            query = (
                select(BedStats)
                .where(BedStats.number_of_regions.is_(None))
                .limit(limit)
                .offset(offset)
            )

            results = session.scalars(query)

            results = [result.id for result in results]

        return results

    def get_missing_files(self, limit: int = 1000, offset: int = 0) -> List[str]:
        """
        Get list of bed files that are missing files (bigBed files)

        :param limit: number of results to return
        :param offset: offset to start from

        :return: list of bed file identifiers
        """

        with Session(self._sa_engine) as session:
            # Alias for subquery
            t2_alias = aliased(Files)

            # Define the subquery
            subquery = select(t2_alias).where(t2_alias.name == "bigbed_file").subquery()

            query = (
                select(Bed.id)
                .outerjoin(subquery, Bed.id == subquery.c.bedfile_id)
                .where(subquery.c.bedfile_id.is_(None))
                .limit(limit)
                .offset(offset)
            )

            results = session.scalars(query)

            results = [result for result in results]

        return results

    def get_unprocessed(self, limit: int = 1000, offset: int = 0) -> BedListResult:
        """
        Get bed files that are not processed.

        :param limit: number of results to return
        :param offset: offset to start from

        :return: list of bed file identifiers
        """
        with Session(self._sa_engine) as session:
            query = (
                select(Bed).where(Bed.processed.is_(False)).limit(limit).offset(offset)
            )
            count_query = select(func.count()).where(Bed.processed.is_(False))

            count = session.execute(count_query).one()[0]

            bed_results = session.scalars(query)

            results = []
            for bed_object in bed_results:
                results.append(
                    BedMetadataBasic(
                        id=bed_object.id,
                        name=bed_object.name,
                        genome_alias=bed_object.genome_alias,
                        genome_digest=bed_object.genome_digest,
                        bed_compliance=bed_object.bed_compliance,
                        data_format=bed_object.data_format,
                        description=bed_object.description,
                        annotation=StandardMeta(
                            **(
                                bed_object.annotations.__dict__
                                if bed_object.annotations
                                else {}
                            )
                        ),
                        last_update_date=bed_object.last_update_date,
                        submission_date=bed_object.submission_date,
                        is_universe=bed_object.is_universe,
                        license_id=bed_object.license_id,
                    )
                )

        return BedListResult(
            count=count,
            limit=limit,
            offset=offset,
            results=results,
        )

    def _update_sources(
        self,
        identifier,
        global_sample_id: List[str] = None,
        global_experiment_id: List[str] = None,
    ) -> None:
        """
        Add global sample and experiment ids to the bed file if they are missing

        :param identifier: bed file identifier
        :param global_sample_id: list of global sample ids
        :param global_experiment_id: list of global experiment ids

        :return: None
        """
        _LOGGER.info(f"Updating sources for bed file: {identifier}")

        with Session(self._sa_engine) as session:
            bed_statement = select(BedMetadata).where(
                and_(BedMetadata.id == identifier)
            )

            bedmetadata_object = session.scalar(bed_statement)

            if (
                global_sample_id
                and global_sample_id[0] not in bedmetadata_object.global_sample_id
            ):
                bedmetadata_object.global_sample_id.append(global_sample_id[0])

                flag_modified(bedmetadata_object, "global_sample_id")

            if (
                global_experiment_id
                and global_experiment_id[0]
                not in bedmetadata_object.global_experiment_id
            ):
                bedmetadata_object.global_experiment_id.append(global_experiment_id[0])
                flag_modified(bedmetadata_object, "global_experiment_id")

            session.commit()
