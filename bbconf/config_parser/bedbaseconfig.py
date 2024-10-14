import logging
import os
import warnings
from pathlib import Path
from typing import List, Literal, Union

import boto3
import qdrant_client
import s3fs
import yacman
import zarr
from botocore.exceptions import BotoCoreError, EndpointConnectionError
from geniml.region2vec.main import Region2VecExModel
from geniml.search import BED2BEDSearchInterface, Text2BEDSearchInterface
from geniml.search.query2vec import BED2Vec, Text2Vec
from geniml.search.backends import BiVectorBackend, QdrantBackend
from geniml.search.interfaces import BiVectorSearchInterface
from pephubclient import PEPHubClient
from zarr import Group as Z_GROUP

from bbconf.config_parser.const import (
    S3_BEDSET_PATH_FOLDER,
    S3_FILE_PATH_FOLDER,
    S3_PLOTS_PATH_FOLDER,
)
from bbconf.config_parser.models import ConfigFile
from bbconf.const import PKG_NAME, ZARR_TOKENIZED_FOLDER
from bbconf.db_utils import BaseEngine
from bbconf.exceptions import (
    BadAccessMethodError,
    BedBaseConfError,
    BedbaseS3ConnectionError,
)
from bbconf.helpers import get_absolute_path, get_bedbase_cfg
from bbconf.models.base_models import FileModel
from bbconf.models.bed_models import BedFiles, BedPlots
from bbconf.models.bedset_models import BedSetPlots
from bbconf.models.drs_models import AccessMethod, AccessURL

_LOGGER = logging.getLogger(PKG_NAME)


class BedBaseConfig:
    def __init__(self, config: Union[Path, str]):
        self.cfg_path = get_bedbase_cfg(config)
        self._config = self._read_config_file(self.cfg_path)

        self._db_engine = self._init_db_engine()
        self._qdrant_engine = self._init_qdrant_backend()
        self._qdrant_text_engine = self._init_qdrant_text_backend()
        self._t2bsi = self._init_t2bsi_object()
        self._b2bsi = self._init_b2bsi_object()
        self._r2v = self._init_r2v_object()
        self._bivec = self._init_bivec_object()

        self._phc = self._init_pephubclient()
        self._boto3_client = self._init_boto3_client()

    @staticmethod
    def _read_config_file(config_path: str) -> ConfigFile:
        """
        Read configuration file and insert default values if not set

        :param config_path: configuration file path
        :return: None
        :raises: raise_missing_key (if config key is missing)
        """
        _config = yacman.YAMLConfigManager(filepath=config_path).exp

        config_dict = {}
        for field_name, annotation in ConfigFile.model_fields.items():
            try:
                config_dict[field_name] = annotation.annotation(
                    **_config.get(field_name)
                )
            except TypeError:
                # TODO: this should be more specific
                config_dict[field_name] = annotation.annotation()

        return ConfigFile(**config_dict)
        # return ConfigFile.from_yaml(Path(config_path))

    @property
    def config(self) -> ConfigFile:
        """
        Get configuration

        :return: configuration object
        """
        return self._config

    @property
    def db_engine(self) -> BaseEngine:
        """
        Get database engine

        :return: database engine
        """
        return self._db_engine

    # @property
    # def t2bsi(self) -> Union[Text2BEDSearchInterface, None]:
    #     """
    #     Get text2bednn object
    #
    #     :return: text2bednn object
    #     """
    #     return self._t2bsi

    @property
    def b2bsi(self) -> Union[BED2BEDSearchInterface, None]:
        """
        Get bed2bednn object

        :return: bed2bednn object
        """
        return self._b2bsi

    @property
    def r2v(self) -> Region2VecExModel:
        """
        Get region2vec object

        :return: region2vec object
        """
        return self._r2v

    @property
    def bivec(self) -> BiVectorSearchInterface:
        """
        Get region2vec object

        :return: region2vec object
        """

        return self._bivec

    @property
    def qdrant_engine(self) -> QdrantBackend:
        """
        Get qdrant engine

        :return: qdrant engine
        """
        return self._qdrant_engine

    @property
    def phc(self) -> PEPHubClient:
        """
        Get PEPHub client

        :return: PEPHub client
        """
        return self._phc

    @property
    def boto3_client(self) -> boto3.client:
        """
        Get boto3 client

        :return: boto3 client
        """
        return self._boto3_client

    @property
    def zarr_root(self) -> Union[Z_GROUP, None]:
        """
        Get zarr root object (Group)

        :return: zarr root group object
        """

        try:
            s3fc_obj = s3fs.S3FileSystem(
                endpoint_url=self._config.s3.endpoint_url,
                key=self._config.s3.aws_access_key_id,
                secret=self._config.s3.aws_secret_access_key,
            )
        except BotoCoreError as e:
            _LOGGER.error(f"Error in creating s3fs object: {e}")
            warnings.warn(f"Error in creating s3fs object: {e}", UserWarning)
            return None

        s3_path = f"s3://{self._config.s3.bucket}/{ZARR_TOKENIZED_FOLDER}"

        zarr_store = s3fs.S3Map(
            root=s3_path, s3=s3fc_obj, check=False, create=self._config.s3.modify_access
        )
        cache = zarr.LRUStoreCache(zarr_store, max_size=2**28)

        return zarr.group(store=cache, overwrite=False)

    def _init_db_engine(self) -> BaseEngine:
        return BaseEngine(
            host=self._config.database.host,
            port=self._config.database.port,
            database=self._config.database.database,
            user=self._config.database.user,
            password=self._config.database.password,
            drivername=f"{self._config.database.dialect}+{self._config.database.driver}",
        )

    def _init_qdrant_backend(self) -> QdrantBackend:
        """
        Create qdrant client object using credentials provided in config file

        :return: QdrantClient
        """
        try:
            return QdrantBackend(
                collection=self._config.qdrant.file_collection,
                qdrant_host=self._config.qdrant.host,
                qdrant_port=self._config.qdrant.port,
                qdrant_api_key=self._config.qdrant.api_key,
            )
        except qdrant_client.http.exceptions.ResponseHandlingException as err:
            _LOGGER.error(f"error in Connection to qdrant! skipping... Error: {err}")
            warnings.warn(
                f"error in Connection to qdrant! skipping... Error: {err}", UserWarning
            )

    def _init_qdrant_text_backend(self) -> QdrantBackend:
        """
        Create qdrant client text embedding object using credentials provided in config file

        :return: QdrantClient
        """

        return QdrantBackend(
            dim=384,
            collection=self.config.qdrant.text_collection,
            qdrant_host=self.config.qdrant.host,
            qdrant_api_key=self.config.qdrant.api_key,
        )

    def _init_bivec_object(self) -> Union[BiVectorSearchInterface, None]:
        """
        Create BiVectorSearchInterface object using credentials provided in config file

        :return: BiVectorSearchInterface
        """

        search_backend = BiVectorBackend(
            metadata_backend=self._qdrant_text_engine, bed_backend=self._qdrant_engine
        )
        search_interface = BiVectorSearchInterface(
            backend=search_backend, query2vec="sentence-transformers/all-MiniLM-L6-v2"
        )
        return search_interface

    def _init_t2bsi_object(self) -> Union[Text2BEDSearchInterface, None]:
        """
        Create Text 2 BED search interface and return this object

        :return: Text2BEDSearchInterface object
        """

        try:
            return Text2BEDSearchInterface(
                backend=self.qdrant_engine,
                query2vec=Text2Vec(
                    hf_repo=self._config.path.text2vec,
                    v2v=self._config.path.vec2vec,
                ),
            )
        except Exception as e:
            _LOGGER.error("Error in creating Text2BEDSearchInterface object: " + str(e))
            warnings.warn(
                "Error in creating Text2BEDSearchInterface object: " + str(e),
                UserWarning,
            )
            return None

    def _init_b2bsi_object(self) -> Union[BED2BEDSearchInterface, None]:
        """
        Create Bed 2 BED search interface and return this object

        :return: Bed2BEDSearchInterface object
        """
        try:
            return BED2BEDSearchInterface(
                backend=self.qdrant_engine,
                query2vec=BED2Vec(model=self._config.path.region2vec),
            )
        except Exception as e:
            _LOGGER.error("Error in creating BED2BEDSearchInterface object: " + str(e))
            warnings.warn(
                "Error in creating BED2BEDSearchInterface object: " + str(e),
                UserWarning,
            )
            return None

    @staticmethod
    def _init_pephubclient() -> Union[PEPHubClient, None]:
        """
        Create Pephub client object using credentials provided in config file

        :return: PephubClient
        """
        try:
            return PEPHubClient()
        except Exception as e:
            _LOGGER.error(f"Error in creating PephubClient object: {e}")
            warnings.warn(f"Error in creating PephubClient object: {e}", UserWarning)
            return None

    def _init_boto3_client(
        self,
    ) -> boto3.client:
        """
        Create Pephub client object using credentials provided in config file

        :return: PephubClient
        """
        try:
            return boto3.client(
                "s3",
                endpoint_url=self._config.s3.endpoint_url,
                aws_access_key_id=self._config.s3.aws_access_key_id,
                aws_secret_access_key=self._config.s3.aws_secret_access_key,
            )
        except Exception as e:
            _LOGGER.error(f"Error in creating boto3 client object: {e}")
            warnings.warn(f"Error in creating boto3 client object: {e}", UserWarning)
            return None

    def _init_r2v_object(self) -> Union[Region2VecExModel, None]:
        """
        Create Region2VecExModel object using credentials provided in config file
        """
        try:
            return Region2VecExModel(self.config.path.region2vec)
        except Exception as e:
            _LOGGER.error(f"Error in creating Region2VecExModel object: {e}")
            warnings.warn(
                f"Error in creating Region2VecExModel object: {e}", UserWarning
            )
            return None

    def upload_s3(self, file_path: str, s3_path: Union[Path, str]) -> None:
        """
        Upload file to s3.

        :param file_path: local path to the file
        :param s3_path: path to the file in s3 with file name
        :return: None
        """
        if not self._boto3_client:
            _LOGGER.warning(
                "Could not upload file to s3. Connection to s3 not established. Skipping.."
            )
            raise BedbaseS3ConnectionError(
                "Could not upload file to s3. Connection error."
            )
        if not os.path.exists(file_path):
            raise BedBaseConfError(f"File {os.path.abspath(file_path)} does not exist.")
        _LOGGER.info(f"Uploading file to s3: {s3_path}")
        return self._boto3_client.upload_file(file_path, self.config.s3.bucket, s3_path)

    def upload_files_s3(
        self,
        identifier: str,
        files: Union[BedFiles, BedPlots, BedSetPlots],
        base_path: str,
        type: Literal["files", "plots", "bedsets"] = "files",
    ) -> Union[BedFiles, BedPlots, BedSetPlots]:
        """
        Upload files to s3.

        :param identifier: bed file identifier
        :param files: dictionary with files to upload
        :param base_path: local path to the output files
        :param type: type of files to upload [files, plots, bedsets]
        :return: None
        """

        if type == "files":
            s3_output_base_folder = S3_FILE_PATH_FOLDER
        elif type == "plots":
            s3_output_base_folder = S3_PLOTS_PATH_FOLDER
        elif type == "bedsets":
            s3_output_base_folder = S3_BEDSET_PATH_FOLDER
        else:
            raise BedBaseConfError(
                f"Invalid type: {type}. Should be 'files', 'plots', or 'bedsets'"
            )

        for key, value in files:
            if not value:
                continue
            file_base_name = os.path.basename(value.path)
            file_path = get_absolute_path(value.path, base_path)
            s3_path = os.path.join(
                s3_output_base_folder,
                identifier[0],
                identifier[1],
                file_base_name,
            )
            self.upload_s3(file_path, s3_path=s3_path)

            setattr(value, "name", key)
            setattr(value, "size", os.path.getsize(file_path))
            setattr(value, "path", s3_path)

            if value.path_thumbnail:
                file_base_name_thumbnail = os.path.basename(value.path_thumbnail)
                file_path_thumbnail = get_absolute_path(value.path_thumbnail, base_path)
                s3_path_thumbnail = os.path.join(
                    s3_output_base_folder,
                    identifier[0],
                    identifier[1],
                    file_base_name_thumbnail,
                )
                self.upload_s3(file_path_thumbnail, s3_path=s3_path_thumbnail)
                setattr(value, "path_thumbnail", s3_path_thumbnail)

        return files

    def delete_s3(self, s3_path: str) -> None:
        """
        Delete file from s3.

        :param s3_path: path to the file in s3
        :return: None
        """
        if not self._boto3_client:
            _LOGGER.warning(
                "Could not delete file from s3. Connection to s3 not established. Skipping.."
            )
            raise BedbaseS3ConnectionError(
                "Could not delete file from s3. Connection error."
            )
        try:
            _LOGGER.info(f"Deleting file from s3: {s3_path}")
            return self._boto3_client.delete_object(
                Bucket=self.config.s3.bucket, Key=s3_path
            )
        except EndpointConnectionError:
            raise BedbaseS3ConnectionError(
                "Could not delete file from s3. Connection error."
            )

    def delete_files_s3(self, files: List[FileModel]) -> None:
        """
        Delete files from s3.

        :param files: list of file objects
        :return: None
        """
        for file in files:
            self.delete_s3(file.path)
            if file.path_thumbnail:
                self.delete_s3(file.path_thumbnail)
        return None

    def get_prefixed_uri(self, postfix: str, access_id: str) -> str:
        """
        Return uri with correct prefix (schema)

        :param postfix: postfix of the uri (or everything after uri schema)
        :param access_id: access method name, e.g. http, s3, etc.
        :return: full uri path
        """

        try:
            prefix = getattr(self.config.access_methods, access_id).prefix
            return os.path.join(prefix, postfix)
        except KeyError:
            _LOGGER.error(f"Access method {access_id} is not defined.")
            raise BadAccessMethodError(f"Access method {access_id} is not defined.")

    def construct_access_method_list(self, rel_path: str) -> List[AccessMethod]:
        """
        Construct access method list for a given record

        :param rel_path: relative path to the record
        :return: list of access methods
        """
        access_methods = []
        for access_id in self.config.access_methods.model_dump().keys():
            access_dict = AccessMethod(
                type=access_id,
                access_id=access_id,
                access_url=AccessURL(url=self.get_prefixed_uri(rel_path, access_id)),
                region=self.config.access_methods.model_dump()[access_id].get(
                    "region", None
                ),
            )
            access_methods.append(access_dict)
        return access_methods
