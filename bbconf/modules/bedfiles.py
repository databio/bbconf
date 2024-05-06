from logging import getLogger
from typing import Dict, Union

import numpy as np
import os
from geniml.bbclient import BBClient
from geniml.io import RegionSet

from pephubclient.exceptions import ResponseError

from qdrant_client.models import Distance, PointIdsList, VectorParams

from sqlalchemy import select, delete, and_, func
from sqlalchemy.orm import Session

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.const import (
    PKG_NAME,
    ZARR_TOKENIZED_FOLDER,
)
from bbconf.db_utils import Bed, BedStats, Files, Universes, TokenizedBed
from bbconf.exceptions import (
    BedBaseConfError,
    BedFIleExistsError,
    BEDFileNotFoundError,
    UniverseNotFoundError,
    TokenizeFileExistsError,
    TokenizeFileNotExistError,
)
from bbconf.models.bed_models import (
    BedClassification,
    BedFiles,
    BedListResult,
    BedListSearchResult,
    BedMetadata,
    BedPEPHub,
    BedPlots,
    BedStatsModel,
    FileModel,
    QdrantSearchResult,
    UniverseMetadata,
    BedMetadataBasic,
)

_LOGGER = getLogger(PKG_NAME)

QDRANT_GENOME = "hg38"


class BedAgentBedFile:
    """
    Class that represents Bedfile in Database.

    This class has method to add, delete, get files and metadata from the database.
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

    def get(self, identifier: str, full: bool = False) -> BedMetadata:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :param full: if True, return full metadata, including statistics, files, and raw metadata from pephub
        :return: project metadata
        """
        statement = select(Bed).where(Bed.id == identifier)

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
            else:
                bed_plots = None
                bed_files = None
                bed_stats = None

        try:
            if full:
                bed_metadata = BedPEPHub(
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

        return BedMetadata(
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
            bed_type=bed_object.bed_type,
            bed_format=bed_object.bed_format,
            full_response=full,
        )

    def get_stats(self, identifier: str) -> BedStatsModel:
        """
        Get file statistics by identifier.

        :param identifier: bed file identifier

        :return: project statistics as BedStats object
        """
        statement = select(BedStats).where(BedStats.id == identifier)

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
        statement = select(Bed).where(Bed.id == identifier)

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

    def get_files(self, identifier: str) -> BedFiles:
        """
        Get file files by identifier.

        :param identifier: bed file identifier
        :return: project files
        """
        statement = select(Bed).where(Bed.id == identifier)

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
            bed_files = BedFiles()
            for result in bed_object.files:
                if result.name in BedFiles.model_fields:
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
        return BedPEPHub(**bed_metadata)

    def get_classification(self, identifier: str) -> BedClassification:
        """
        Get file classification by identifier.

        :param identifier: bed file identifier
        :return: project classification
        """
        statement = select(Bed).where(Bed.id == identifier)

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
        statement = select(Bed).where(Bed.id == identifier)
        return_dict = {}

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                raise BEDFileNotFoundError(f"Bed file with id: {identifier} not found.")
            for result in bed_object.files:
                return_dict[result.name] = FileModel(**result.__dict__)

        return return_dict

    def get_ids_list(
        self,
        limit: int = 100,
        offset: int = 0,
        genome: str = None,
        bed_type: str = None,
    ) -> BedListResult:
        """
        Get list of bed file identifiers.

        :param limit: number of results to return
        :param offset: offset to start from
        :param genome: filter by genome
        :param bed_type: filter by bed type. e.g. 'bed6+4'
        :param full: if True, return full metadata, including statistics, files, and raw metadata from pephub

        :return: list of bed file identifiers
        """
        statement = select(Bed)
        count_statement = select(func.count(Bed.id))

        # TODO: make it generic, like in pephub
        if genome:
            statement = statement.where(Bed.genome_alias == genome)
            count_statement = count_statement.where(Bed.genome_alias == genome)

        if bed_type:
            statement = statement.where(Bed.bed_type == bed_type)
            count_statement = count_statement.where(Bed.bed_type == bed_type)

        statement = statement.limit(limit).offset(offset)

        result_list = []
        with Session(self._sa_engine) as session:
            bed_ids = session.scalars(statement)
            count = session.execute(count_statement).one()

            for result in bed_ids:
                result_list.append(BedMetadataBasic(**result.__dict__))

        return BedListResult(
            count=count[0],
            limit=limit,
            offset=offset,
            results=result_list,
        )

    def add(
        self,
        identifier: str,
        stats: dict,
        metadata: dict = None,
        plots: dict = None,
        files: dict = None,
        classification: dict = None,
        upload_qdrant: bool = False,
        upload_pephub: bool = False,
        upload_s3: bool = False,
        local_path: str = None,
        overwrite: bool = False,
        nofail: bool = False,
    ) -> None:
        """
        Add bed file to the database.

        :param identifier: bed file identifier
        :param stats: bed file results {statistics, plots, files, metadata}
        :param metadata: bed file metadata (will be saved in pephub)
        :param plots: bed file plots
        :param files: bed file files
        :param classification: bed file classification
        :param upload_qdrant: add bed file to qdrant indexs
        :param upload_pephub: add bed file to pephub
        :param upload_s3: upload files to s3
        :param local_path: local path to the output files
        :param overwrite: overwrite bed file if it already exists
        :param nofail: do not raise an error for error in pephub/s3/qdrant or record exsist and not overwrite
        :return: None
        """
        _LOGGER.info(f"Adding bed file to database. bed_id: {identifier}")

        if self.exists(identifier):
            _LOGGER.warning(f"Bed file with id: {identifier} exists in the database.")
            if not overwrite:
                if not nofail:
                    raise BedFIleExistsError(
                        f"Bed file with id: {identifier} already exists in the database."
                    )
                _LOGGER.warning("Overwrite set to False. Skipping..")
                return None
            else:
                self.delete(identifier)

        stats = BedStatsModel(**stats)
        # TODO: we should not check for specific keys, of the plots!
        plots = BedPlots(**plots)
        files = BedFiles(**files)

        classification = BedClassification(**classification)
        if upload_pephub:
            metadata = BedPEPHub(**metadata)
            try:
                self.upload_pephub(identifier, metadata.model_dump(), overwrite)
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
                self.upload_file_qdrant(
                    identifier, files.bed_file.path, {"bed_id": identifier}
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
                indexed=upload_qdrant,
                pephub=upload_pephub,
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
            session.add(new_bedstat)

            session.commit()

        return None

    def update(
        self,
        identifier: str,
        stats: dict,
        metadata: dict = None,
        plots: dict = None,
        files: dict = None,
        classification: dict = None,
        add_to_qdrant: bool = False,
        upload_pephub: bool = False,
        upload_s3: bool = False,
        local_path: str = None,
        overwrite: bool = False,
        nofail: bool = False,
    ):
        """
        Update bed file to the database.

        :param identifier: bed file identifier
        :param stats: bed file results {statistics, plots, files, metadata}
        :param metadata: bed file metadata (will be saved in pephub)
        :param plots: bed file plots
        :param files: bed file files
        :param classification: bed file classification
        :param add_to_qdrant: add bed file to qdrant indexs
        :param upload_pephub: add bed file to pephub
        :param upload_s3: upload files to s3
        :param local_path: local path to the output files
        :param overwrite: overwrite bed file if it already exists
        :param nofail: do not raise an error for error in pephub/s3/qdrant or record exsist and not overwrite
        :return: None
        """
        if not self.exists(identifier):
            raise BEDFileNotFoundError(
                f"Bed file with id: {identifier} not found. Cannot update."
            )

        stats = BedStatsModel(**stats)
        plots = BedPlots(**plots)
        files = BedFiles(**files)
        classification = BedClassification(**classification)

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

        if add_to_qdrant:
            self.upload_file_qdrant(
                identifier, files.bed_file.path, {"bed_id": identifier}
            )

        statement = select(Bed).where(Bed.id == identifier)

        if upload_s3:
            _LOGGER.warning("S3 upload is not implemented yet")
            # if files:
            #     files = self._config.upload_files_s3(
            #         identifier, files=files, base_path=local_path, type="files"
            #     )
            #
            # if plots:
            #     plots = self._config.upload_files_s3(
            #         identifier, files=plots, base_path=local_path, type="plots"
            #     )

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)

            setattr(bed_object, **stats.model_dump())
            setattr(bed_object, **classification.model_dump())

            bed_object.indexed = add_to_qdrant
            bed_object.pephub = upload_pephub

            if upload_s3:
                _LOGGER.warning("S3 upload is not implemented yet")
                # for k, v in files:
                #     if v:
                #         new_file = Files(
                #             **v.model_dump(exclude_none=True, exclude_unset=True),
                #             bedfile_id=identifier,
                #             type="file",
                #         )
                #         session.add(new_file)
                # for k, v in plots:
                #     if v:
                #         new_plot = Files(
                #             **v.model_dump(exclude_none=True, exclude_unset=True),
                #             bedfile_id=identifier,
                #             type="plot",
                #         )
                #         session.add(new_plot)

            session.commit()

        raise NotImplementedError

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
            statement = select(Bed).where(Bed.id == identifier)
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

    def update_pephub(self, identifier: str, metadata: dict, overwrite: bool = False):
        if not metadata:
            _LOGGER.warning("No metadata provided. Skipping pephub upload..")
            return False
        self._config.phc.sample.update(
            namespace=self._config.config.phc.namespace,
            name=self._config.config.phc.name,
            tag=self._config.config.phc.tag,
            sample_name=identifier,
            sample_dict=metadata,
        )

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

        _LOGGER.info(f"Adding bed file to qdrant. bed_id: {bed_id}")
        if isinstance(bed_file, str):
            bed_region_set = RegionSet(bed_file)
        elif isinstance(bed_file, RegionSet):
            bed_region_set = bed_file
        else:
            raise BedBaseConfError(
                "Could not add add region to qdrant. Invalid type, or path. "
            )
        bed_embedding = np.mean(self._config.r2v.encode(bed_region_set), axis=0)

        # Upload bed file vector to the database
        vec_dim = bed_embedding.shape[0]
        self._qdrant_engine.load(
            ids=[bed_id],
            vectors=bed_embedding.reshape(1, vec_dim),
            payloads=[{**payload}],
        )
        return None

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
        _LOGGER.info(f"Using backend: {self._config.t2bsi}")

        results = self._config.t2bsi.query_search(query, limit=limit, offset=offset)
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

    def reindex_qdrant(self) -> None:
        """
        Re-upload all files to quadrant.
        !Warning: only hg38 genome can be added to qdrant!

        If you want want to fully reindex/reupload to qdrant, first delete collection and create new one.

        Upload all files to qdrant.
        """
        bb_client = BBClient()

        statement = select(Bed.id).where(Bed.genome_alias == QDRANT_GENOME)

        with Session(self._db_engine.engine) as session:
            bed_ids = session.execute(statement).all()

        bed_ids = [bed_result[0] for bed_result in bed_ids]

        for record_id in bed_ids:
            bed_region_set_obj = bb_client.load_bed(record_id)

            self.upload_file_qdrant(
                bed_id=record_id,
                bed_file=bed_region_set_obj,
                payload={"bed_id": record_id},
            )

        return None

    def delete_qdrant_point(self, identifier: str) -> None:
        """
        Delete bed file from qdrant.

        :param identifier: bed file identifier
        :return: None
        """

        result = self._config.qdrant_engine.qd_client.delete(
            collection_name=self._config.config.qdrant.collection,
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
            collection_name=self._config.config.qdrant.collection,
            vectors_config=VectorParams(size=100, distance=Distance.DOT),
        )

    def exists(self, identifier: str) -> bool:
        """
        Check if bed file exists in the database.

        :param identifier: bed file identifier
        :return: True if bed file exists, False otherwise
        """
        statement = select(Bed).where(Bed.id == identifier)

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                return False
            return True

    def get_universe(self, identifier: str, full: bool = False) -> UniverseMetadata:
        """
        Get universe metadata

        :param identifier: universe identifier
        :param full: if True, return full metadata, including statistics, files, and raw metadata from pephub

        :return: universe metadata
        """
        if not self.exists_universe(identifier):
            raise ValueError(f"Universe with id: {identifier} not found.")

        with Session(self._sa_engine) as session:
            statement = select(Universes).where(Universes.id == identifier)
            universe_object = session.scalar(statement)

            bedset_id = universe_object.bedset_id
            method = universe_object.method

        return UniverseMetadata(
            **self.get(identifier, full=full).__dict__,
            bedset_id=bedset_id,
            method=method,
        )

    def exists_universe(self, identifier: str) -> bool:
        """
        Check if universe exists in the database.

        :param identifier: universe identifier

        :return: True if universe exists, False otherwise
        """
        statement = select(Universes).where(Universes.id == identifier)

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)
            if not bed_object:
                return False
            return True

    def add_universe(
        self, bedfile_id: str, bedset_id: str, construct_method: str
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
            statement = delete(Universes).where(Universes.id == identifier)
            session.execute(statement)
            session.commit()

    def add_tokenized(self, bed_id: str, universe_id: str, token_vector: list) -> str:
        """
        Add tokenized bed file to the database

        :param bed_id: bed file identifier
        :param universe_id: universe identifier
        :param token_vector: list of tokens

        :return: token path
        """

        path = self._add_zarr_s3(bed_id, universe_id, token_vector)

        with Session(self._sa_engine) as session:
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

        return os.path.join(ZARR_TOKENIZED_FOLDER, path)

    def get_tokenized(self, bed_id: str, universe_id: str) -> list:
        """
        Get zarr file from the database

        :param bed_id: bed file identifier
        :param universe_id: universe identifier

        :return: zarr path
        """
        if not self.exist_tokenized(bed_id, universe_id):
            raise TokenizeFileNotExistError(
                f"Tokenized file not found in the database."
            )
        univers_group = self._config.zarr_root.require_group(universe_id)

        return list(univers_group[bed_id])

    def delete_tokenized(self, bed_id: str, universe_id: str) -> None:
        """
        Delete tokenized bed file from the database

        :param bed_id: bed file identifier
        :param universe_id: universe identifier

        :return: None
        """
        if not self.exist_tokenized(bed_id, universe_id):
            raise TokenizeFileNotExistError(
                f"Tokenized file not found in the database."
            )
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

    def get_tokenized_path(self, bed_id: str, universe_id: str) -> str:
        """
        Get tokenized path to tokenized file

        :param bed_id: bed file identifier
        :param universe_id: universe identifier

        :return: token path
        """
        if not self.exist_tokenized(bed_id, universe_id):
            raise TokenizeFileNotExistError(
                f"Tokenized file not found in the database."
            )

        with Session(self._sa_engine) as session:
            statement = select(TokenizedBed).where(
                and_(
                    TokenizedBed.bed_id == bed_id,
                    TokenizedBed.universe_id == universe_id,
                ),
            )
            tokenized_object = session.scalar(statement)
            return tokenized_object.path

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
