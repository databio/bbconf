
import warnings
from logging import getLogger
from typing import List, Optional, Dict, Union, Literal
import numpy as np

from geniml.region2vec import Region2VecExModel
from geniml.io import RegionSet

from sqlalchemy.orm import Session
from sqlalchemy import select

import os

from bbconf.const import (
    PKG_NAME,
)
from bbconf.modules.models import BedMetadata, BedFiles, Files
from bbconf.exceptions import (
    BedBaseConfError,
)
from bbconf.db_utils import BaseEngine, Bed
from bbconf.config_parser.bedbaseconfig import BedBaseConfig

_LOGGER = getLogger(PKG_NAME)


class BedAgentBedFile:
    """
    Class that represents Bedfile in Database.

    This class has method to add, delete, get files and metadata from the database.
    """

    def __init__(self, config: BedBaseConfig):
        """
        :param config: config object with database and qdrant engine and credentials
        """
        self._sa_engine = config.db_engine.engine
        self._db_engine = config.db_engine
        self._qdrant_engine = config.qdrant_engine
        self._boto3_client = config.boto3_client
        self._config = config

    def get(self, identifier: str) -> BedMetadata:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :return: project metadata
        """
        statement = select(Bed).where(Bed.id == identifier)

        with Session(self._sa_engine) as session:
            bed_object = session.scalar(statement)






    def get_stats(self, identifier: str) -> dict:
        """
        Get file statistics by identifier.

        :param identifier: bed file identifier
        :return: project statistics
        """
        ...

    def get_plots(self, identifier: str) -> dict:
        """
        Get file plots by identifier.

        :param identifier: bed file identifier
        :return: project plots
        """
        ...

    def get_files(self, identifier: str) -> dict:
        """
        Get file files by identifier.

        :param identifier: bed file identifier
        :return: project files
        """
        ...

    def get_metadata(self, identifier: str) -> dict:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :return: project metadata
        """
        ...

    def get_classification(self, identifier: str) -> dict:
        """
        Get file classification by identifier.

        :param identifier: bed file identifier
        :return: project classification
        """
        ...

    def add(self,
            identifier: str,
            stats: dict,
            metadata: dict = None,
            plots: dict = None,
            files: dict = None,
            add_to_qdrant: bool = False,
            upload_pephub: bool = False,
            upload_s3: bool = False,
            overwrite: bool = False) -> None:
        """
        Add bed file to the database.

        :param identifier: bed file identifier
        :param stats: bed file results {statistics, plots, files, metadata}
        :param metadata: bed file metadata (will be saved in pephub)
        :param add_to_qdrant: add bed file to qdrant indexs
        :param upload_pephub: add bed file to pephub
        :param upload_s3: upload files to s3
        :param overwrite: overwrite bed file if it already exists
        :return: None
        """
        _LOGGER.info(f"Adding bed file to database. bed_id: {identifier}")

        if upload_pephub:
            self.upload_pephub(identifier, metadata, overwrite)
        else:
            _LOGGER.info("upload_pephub set to false. Skipping pephub..")

        if add_to_qdrant:
            # TODO: rethink it
            bed_file_path = files.get("bed_file")
            self.upload_qdrant(identifier, bed_file_path, metadata)
        else:
            _LOGGER.info("add_to_qdrant set to false. Skipping qdrant..")

        # Upload files to s3
        if files:
            self.upload_files_s3(files)

        if plots:
            self.upload_plots_s3(identifier, plots)

        stats["id"] = identifier

        new_bed = Bed(
            **stats
        )

        with Session(self._sa_engine) as session:
            session.add(new_bed)
            session.commit()

        print(new_bed)

    def delete(self, identifier: str) -> None:
        """
        Delete bed file from the database.

        :param identifier: bed file identifier
        :return: None
        """
        ...

    def upload_files_s3(self, files: List[Files]) -> BedFiles:
        """
        Upload files to s3.

        :param files: dictionary with files to upload
        :return: None
        """
        bed_files_object = BedFiles()
        for file in files:
            if file.name == "bed_file":

                file_base_name = os.path.basename(file.path)

                bed_file = file.path
                bed_s3_path = os.path.join("bed_files", file_base_name[0], file_base_name[1], os.path.basename(bed_file))
                self._upload_s3(bed_file, bed_s3_path)

                bed_files_object["bed_file"] = Files(
                    name="bedfile",
                    path=bed_s3_path,
                )

            elif file.name == "bigbed_file":
                file_base_name = os.path.basename(file.path)

                bed_file = file.path
                bed_s3_path = os.path.join("bigbed_files", file_base_name[0], file_base_name[1],
                                           os.path.basename(bed_file))
                self._upload_s3(bed_file, bed_s3_path)

                bed_files_object["bed_file"] = Files(
                    name="bigbedfile",
                    path=bed_s3_path,
                )

            else:
                warnings.warn(f"Provided file name: {file.name} is not supported. Skipping..")
        return bed_files_object

    def upload_plots_s3(self, identifier: str, plots: dict) -> dict:
        """
        Upload plots to s3.

        :param identifier: bed file identifier
        :param plots: dictionary with plots to upload
        :return: None
        """
        return_dict = {}
        _LOGGER.info(f"Uploading plots to S3...")
        for key, value in plots.items():
            self._upload_s3(value["local_path"], value["s3_path"])
            return_dict[key] = value["s3_path"]

        _LOGGER.info(f"Data for '{identifier}' uploaded to S3 successfully!")
        return return_dict

    def _upload_s3(self, file_path: str, s3_path: str) -> None:
        """
        Upload file to s3.

        :param file_path: local path to the file
        :param s3_path: path to the file in s3 with file name
        :return: None
        """
        self._config.boto3_client.upload_file(file_path,
                                              self._config.config.s3.bucket,
                                              s3_path)

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

    def upload_qdrant(
        self,
        bed_id: str,
        bed_file: Union[str, RegionSet],
        payload: dict = None,
        region_to_vec: Region2VecExModel = None,
    ) -> None:
        """
        Convert bed file to vector and add it to qdrant database

        :param bed_id: bed file id
        :param bed_file: path to the bed file, or RegionSet object
        :param payload: additional metadata to store alongside vectors
        :param region_to_vec: initiated region to vector model. If None, new object will be created.
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
        if not region_to_vec and isinstance(self._config.config.path.region2vec, str):
            reg_2_vec_obj = Region2VecExModel(self._config.config.path.region2vec)
        else:
            reg_2_vec_obj = region_to_vec
        bed_embedding = np.mean(reg_2_vec_obj.encode(bed_region_set), axis=0)

        # Upload bed file vector to the database
        vec_dim = bed_embedding.shape[0]
        self._qdrant_engine.load(
            ids=[bed_id],
            vectors=bed_embedding.reshape(1, vec_dim),
            payloads=[{**payload}],
        )
        return None

    def text_to_bed_search(self, query: str, limit: int = 10, offset: int = 0):
        """
        Search for bed files by text query in qdrant database

        :param query: text query
        :param limit: number of results to return
        :param offset: offset to start from

        :return: list of bed file metadata
        """
        _LOGGER.info(f"Looking for: {query}")
        _LOGGER.info(f"Using backend: {self._config.t2bsi}")

        # TODO: FIX it!
        results = self._config.t2bsi.nl_vec_search(query, limit=limit, offset=offset)
        for result in results:
            try:
                # qdrant automatically adds hypens to the ids. remove them.
                result["metadata"] = bbc.bed.retrieve_one(result["id"].replace("-", ""))
            except RecordNotFoundError:
                _LOGGER.info(
                    f"Couldn't find qdrant result in bedbase for id: {result['id']}"
                )
        return results

    def _verify_results(self, results: dict) -> tuple:
        """
        Verify if results are in the correct format

        :param results: results to verify
        :return: True if results are correct, False if not
        """
        table_annotations = Bed.__annotations__.keys()
        correct_results = {}
        unknown_results = {}
        for key, value in results.items():
            if key in table_annotations:
                correct_results[key] = value
            else:
                unknown_results[key] = value
        return correct_results, unknown_results
