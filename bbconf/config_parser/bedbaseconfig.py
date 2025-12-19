import io
import logging
import os
import warnings
from pathlib import Path
from typing import List, Literal, Union

import boto3
import joblib
import qdrant_client
import requests
import s3fs
import yacman
import zarr
from botocore.exceptions import BotoCoreError, EndpointConnectionError
from fastembed import TextEmbedding
from geniml.region2vec.main import Region2VecExModel
from geniml.search import BED2BEDSearchInterface
from geniml.search.backends import BiVectorBackend, QdrantBackend
from geniml.search.interfaces import BiVectorSearchInterface
from geniml.search.query2vec import BED2Vec
from pephubclient import PEPHubClient
from qdrant_client import QdrantClient, models
from sentence_transformers import SparseEncoder
from umap import UMAP
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


class BedBaseConfig(object):
    """
    Class to handle BEDbase configuration file and create objects for different modules.
    """

    def __init__(self, config: Union[Path, str], init_ml: bool = True):
        """
        Initialize BedBaseConfig object

        :param config: path to the configuration file
        :param init_ml: initialize machine learning models used for search
        """

        self.cfg_path = get_bedbase_cfg(config)
        self._config = self._read_config_file(self.cfg_path)
        self._db_engine = self._init_db_engine()

        try:
            self.qdrant_client: QdrantClient = self._init_qdrant_client()
        except Exception as err:
            _LOGGER.error(
                f"Unable to create Qdrant client. Skipping ML model initialization. Error: {err}"
            )
            init_ml = False

        if init_ml:

            self.dense_encoder: TextEmbedding = self._init_dense_encoder()
            self.sparse_encoder: Union[SparseEncoder, None] = self._init_sparce_model()
            self.umap_encoder: Union[UMAP, None] = self._init_umap_model()
            self.r2v_encoder: Union[Region2VecExModel, None] = self._init_r2v_encoder()

            self._init_qdrant_hybrid(
                qdrant_cl=self.qdrant_client,
                dense_encoder=self.dense_encoder,
            )

            self.qdrant_file_backend: Union[QdrantBackend, None] = (
                self._init_qdrant_file_backend(qdrant_cl=self.qdrant_client)
            )  # used for bivec search
            self._qdrant_text_backend: Union[QdrantBackend, None] = (
                self._init_qdrant_text_backend(
                    qdrant_cl=self.qdrant_client,
                    dense_encoder=self.dense_encoder,
                )
            )  # used for bivec search

            self.b2b_search_interface = self._init_b2b_search_interface(
                qdrant_file_backend=self.qdrant_file_backend,
                region_encoder=self.r2v_encoder,
            )

            self.bivec_search_interface = self._init_bivec_interface(
                qdrant_file_backend=self.qdrant_file_backend,
                qdrant_text_backend=self._qdrant_text_backend,
                text_encoder=self.dense_encoder,
            )
        else:
            _LOGGER.info(
                "Skipping initialization of ML models, init_ml parameter set to False."
            )
            self.r2v_encoder = None
            self.b2b_search_interface = None
            self.bivec_search_interface = None
            self.umap_encoder: Union[UMAP, None] = None
            self.sparse_encoder = None

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
        """
        Create database engine object using credentials provided in config file
        """

        _LOGGER.info("Initializing database engine...")
        return BaseEngine(
            host=self._config.database.host,
            port=self._config.database.port,
            database=self._config.database.database,
            user=self._config.database.user,
            password=self._config.database.password,
            drivername=f"{self._config.database.dialect}+{self._config.database.driver}",
        )

    def _init_qdrant_client(self) -> QdrantClient:
        """
        Create qdrant client object using credentials provided in config file
        """

        _LOGGER.info("Initializing qdrant client...")

        try:
            qdrant_cl = QdrantClient(
                url=self.config.qdrant.host,
                port=self.config.qdrant.port,
                api_key=self.config.qdrant.api_key,
            )

        except qdrant_client.http.exceptions.ResponseHandlingException as err:
            raise BedBaseConfError(
                f"Error in Connection to qdrant! skipping... Error: {err}"
            )

        return qdrant_cl

    def _init_qdrant_file_backend(
        self, qdrant_cl: QdrantClient
    ) -> Union[QdrantBackend, None]:
        """
        Create qdrant client object using credentials provided in config file

        :param: qdrant_cl: QdrantClient object
        :return: QdrantClient
        """

        _LOGGER.info("Initializing qdrant bivec file backend...")

        if not isinstance(qdrant_cl, QdrantClient):
            _LOGGER.error(
                f"Unable to create Qdrant bivec file collection, qdrant client is None."
            )
            return None

        try:
            return QdrantBackend(
                qdrant_client=qdrant_cl,
                collection=self.config.qdrant.file_collection,
            )
        except Exception as e:
            _LOGGER.error(f"Unable to create Qdrant collection: {e}")
            return None

    def _init_qdrant_text_backend(
        self, qdrant_cl: QdrantClient, dense_encoder: TextEmbedding
    ) -> Union[QdrantBackend, None]:
        """
        Create qdrant client text embedding object using credentials provided in config file

        :param: qdrant_cl: QdrantClient object
        :param: dense_encoder: TextEmbedding model for encoding text queries
        :return: QdrantClient
        """

        _LOGGER.info("Initializing qdrant bivec text backend...")

        if not isinstance(qdrant_cl, QdrantClient):
            _LOGGER.error(
                f"Unable to create Qdrant bivec text collection, qdrant client is None."
            )
            return None
        if not isinstance(dense_encoder, TextEmbedding):
            _LOGGER.error(
                f"Unable to create Qdrant bivec text collection, dense encoder is None."
            )
            return None

        dimensions = int(dense_encoder.get_embedding_size(self._config.path.text2vec))
        try:
            return QdrantBackend(
                qdrant_client=qdrant_cl,
                dim=dimensions,
                collection=self.config.qdrant.text_collection,
            )
        except Exception as e:
            _LOGGER.error(f"Unable to create Qdrant collection: {e}")
            return None

    def _init_qdrant_hybrid(
        self, qdrant_cl: QdrantClient, dense_encoder: TextEmbedding
    ) -> None:
        """
        Create qdrant client with sparse and text embedding object using credentials provided in config file

        :param: qdrant_cl: QdrantClient object
        :param: dense_encoder: TextEmbedding model for encoding text queries
        :return: QdrantClient
        """

        _LOGGER.info("Initializing qdrant sparse collection...")

        if not isinstance(qdrant_cl, QdrantClient):
            _LOGGER.error(
                f"Unable to create Qdrant hybrid collection, qdrant client is None."
            )
            return None
        if not isinstance(dense_encoder, TextEmbedding):
            _LOGGER.error(
                f"Unable to create Qdrant hybrid collection, dense encoder is None."
            )
            return None

        dimensions = int(dense_encoder.get_embedding_size(self._config.path.text2vec))
        collection_name = self.config.qdrant.hybrid_collection

        try:
            if not qdrant_cl.collection_exists(collection_name):
                _LOGGER.info(
                    "Collection 'bedbase_query_search' does not exist, creating it."
                )

                qdrant_cl.create_collection(
                    collection_name=collection_name,
                    vectors_config={
                        "dense": models.VectorParams(
                            size=dimensions, distance=models.Distance.COSINE
                        ),
                    },
                    sparse_vectors_config={
                        "sparse": models.SparseVectorParams(
                            index=models.SparseIndexParams(
                                on_disk=False,
                            )
                        )
                    },
                    quantization_config=models.ScalarQuantization(
                        scalar=models.ScalarQuantizationConfig(
                            type=models.ScalarType.INT8,
                            quantile=0.99,
                            always_ram=True,
                        ),
                    ),
                )

                qdrant_cl.create_payload_index(
                    collection_name=collection_name,
                    field_name="assay",
                    field_type=models.PayloadSchemaType.KEYWORD,
                )
                qdrant_cl.create_payload_index(
                    collection_name=collection_name,
                    field_name="genome_alias",
                    field_type=models.PayloadSchemaType.KEYWORD,
                )

        except Exception as err:
            _LOGGER.error(
                f"Error in creating Qdrant hybrid collection! skipping... Error: {err}. Qdrant host: {self._config.qdrant.host}"
            )
            warnings.warn(
                f"error in creating Qdrant hybrid collection! skipping... Error: {err}",
                UserWarning,
            )
            return None

    def _init_bivec_interface(
        self,
        qdrant_file_backend: QdrantBackend,
        qdrant_text_backend: QdrantBackend,
        text_encoder: TextEmbedding,
    ) -> Union[BiVectorSearchInterface, None]:
        """
        Create BiVectorSearchInterface object using credentials provided in config file

        :param: qdrant_file_backend: QdrantBackend for file vectors
        :param: qdrant_text_backend: QdrantBackend for text vectors
        :param: text_encoder: TextEmbedding model for encoding text queries
        :return: BiVectorSearchInterface
        """

        _LOGGER.info("Initializing BiVectorBackend...")
        search_backend = BiVectorBackend(
            metadata_backend=qdrant_text_backend, bed_backend=qdrant_file_backend
        )
        _LOGGER.info("Initializing BiVectorSearchInterface...")
        search_interface = BiVectorSearchInterface(
            backend=search_backend,
            query2vec=text_encoder,
        )
        return search_interface

    def _init_b2b_search_interface(
        self,
        qdrant_file_backend: QdrantBackend,
        region_encoder: Union[Region2VecExModel, str],
    ) -> Union[BED2BEDSearchInterface, None]:
        """
        Create Bed 2 BED search interface and return this object

        :return: Bed2BEDSearchInterface object
        """
        try:
            _LOGGER.info("Initializing search bed 2 bed search interfaces...")
            return BED2BEDSearchInterface(
                backend=qdrant_file_backend,
                query2vec=BED2Vec(model=region_encoder),
            )
        except Exception as e:
            _LOGGER.error("Error in creating BED2BEDSearchInterface object: " + str(e))
            warnings.warn(
                "Error in creating BED2BEDSearchInterface object: " + str(e),
                UserWarning,
            )
            return None

    def _init_r2v_encoder(self) -> Union[Region2VecExModel, None]:
        """
        Create Region2VecExModel object using credentials provided in config file
        """
        try:
            _LOGGER.info(
                f"Initializing region2vec encoder... Model used: {self.config.path.region2vec}"
            )
            return Region2VecExModel(self.config.path.region2vec)
        except Exception as e:
            _LOGGER.error(f"Error in creating Region2VecExModel object: {e}")
            warnings.warn(
                f"Error in creating Region2VecExModel object: {e}", UserWarning
            )
            return None

    def _init_dense_encoder(self) -> Union[None, TextEmbedding]:
        """
        Initialize dense model from the specified path or huggingface model hub
        """

        _LOGGER.info(
            f"Initializing dense encoder... Model used: {self.config.path.text2vec}"
        )
        dense_encoder = TextEmbedding(self.config.path.text2vec)
        return dense_encoder

    def _init_sparce_model(self) -> Union[None, SparseEncoder]:
        """
        Initialize SparseEncoder model from the specified path or huggingface model hub
        """
        try:
            _LOGGER.info(
                f"Initializing sparse encoder... Model used: {self.config.path.sparse_model}"
            )
            sparse_encoder = SparseEncoder(self.config.path.sparse_model)
        except Exception as e:
            _LOGGER.error(f"Error in creating SparseEncoder object: {e}")
            warnings.warn(f"Error in creating SparseEncoder object: {e}", UserWarning)
            return None
        return sparse_encoder

    def _init_umap_model(self) -> Union[UMAP, None]:
        """
        Load UMAP model from the specified path, or url
        """

        if not self.config.path.umap_model:
            _LOGGER.warning(
                "UMAP model path is not specified in the configuration, and won't be used."
            )
            return None

        model_path = self.config.path.umap_model
        umap_model = None
        if model_path.startswith(("http://", "https://")):

            try:
                response = requests.get(model_path)
                response.raise_for_status()
                buffer = io.BytesIO(response.content)
                umap_model = joblib.load(buffer)
                print(f"UMAP model loaded from URL: {model_path}")
            except requests.RequestException as e:
                _LOGGER.error(f"Error downloading UMAP model from URL: {e}")
                return None
        else:
            try:
                with open(model_path, "rb") as file:
                    umap_model = joblib.load(file)
                print(f"UMAP model loaded from local path: {model_path}")
            except FileNotFoundError as e:
                _LOGGER.error(f"Error loading UMAP model from local path: {e}")
                return None

        if not isinstance(umap_model, UMAP):
            _LOGGER.error(f"Loaded object is not a UMAP instance: {type(umap_model)}")
            return None
        # np.random.seed(42)
        umap_model.random_state = 42
        return umap_model

    def _init_boto3_client(
        self,
    ) -> Union[boto3.client, None]:
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

    @staticmethod
    def _init_pephubclient() -> Union[PEPHubClient, None]:
        """
        Create Pephub client object using credentials provided in config file

        :return: PephubClient
        """

        # try:
        #     _LOGGER.info("Initializing PEPHub client...")
        #     return PEPHubClient()
        # except Exception as e:
        #     _LOGGER.error(f"Error in creating PephubClient object: {e}")
        #     warnings.warn(f"Error in creating PephubClient object: {e}", UserWarning)
        #     return None
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
