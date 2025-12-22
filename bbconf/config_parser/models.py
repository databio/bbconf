import logging
from pathlib import Path
from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, computed_field, field_validator
from yacman import load_yaml

from bbconf.config_parser.const import (  # DEFAULT_VEC2VEC_MODEL,
    DEFAULT_DB_DIALECT,
    DEFAULT_DB_DRIVER,
    DEFAULT_DB_NAME,
    DEFAULT_DB_PORT,
    DEFAULT_PEPHUB_NAME,
    DEFAULT_PEPHUB_NAMESPACE,
    DEFAULT_PEPHUB_TAG,
    DEFAULT_QDRANT_BIVEC_COLLECTION_NAME,
    DEFAULT_QDRANT_FILE_COLLECTION_NAME,
    DEFAULT_QDRANT_HYBRID_COLLECTION_NAME,
    DEFAULT_QDRANT_PORT,
    DEFAULT_REGION2_VEC_MODEL,
    DEFAULT_S3_BUCKET,
    DEFAULT_SERVER_HOST,
    DEFAULT_SERVER_PORT,
    DEFAULT_SPARSE_MODEL,
    DEFAULT_TEXT2VEC_MODEL,
)

_LOGGER = logging.getLogger(__name__)


class ConfigDB(BaseModel):
    host: str
    port: int = DEFAULT_DB_PORT
    user: str
    password: str
    database: str = DEFAULT_DB_NAME
    dialect: str = DEFAULT_DB_DIALECT
    driver: Optional[str] = DEFAULT_DB_DRIVER

    model_config = ConfigDict(extra="forbid")

    @computed_field
    @property
    def url(self) -> str:
        """
        The URL of the database.

        :return str: The URL of the database.
        """
        return f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class ConfigQdrant(BaseModel):
    host: str
    port: int = DEFAULT_QDRANT_PORT
    api_key: Optional[str] = None
    file_collection: str = DEFAULT_QDRANT_FILE_COLLECTION_NAME
    text_collection: Optional[str] = DEFAULT_QDRANT_BIVEC_COLLECTION_NAME
    hybrid_collection: Optional[str] = DEFAULT_QDRANT_HYBRID_COLLECTION_NAME


class ConfigServer(BaseModel):
    host: str = DEFAULT_SERVER_HOST
    port: int = DEFAULT_SERVER_PORT


class ConfigPath(BaseModel):
    region2vec: str = DEFAULT_REGION2_VEC_MODEL
    # vec2vec: str = DEFAULT_VEC2VEC_MODEL
    text2vec: str = DEFAULT_TEXT2VEC_MODEL
    sparse_model: str = DEFAULT_SPARSE_MODEL
    umap_model: Union[str, None] = None  # Path or link to pre-trained UMAP model


class AccessMethodsStruct(BaseModel):
    type: str
    description: str = None
    prefix: str


class AccessMethods(BaseModel):
    http: AccessMethodsStruct = None
    s3: AccessMethodsStruct = None
    local: AccessMethodsStruct = None


class ConfigS3(BaseModel):
    endpoint_url: Union[str, None] = None
    aws_access_key_id: Union[str, None] = None
    aws_secret_access_key: Union[str, None] = None
    bucket: Union[str, None] = DEFAULT_S3_BUCKET

    @field_validator("aws_access_key_id", "aws_secret_access_key")
    def validate_aws_credentials(cls, value):
        # Do this if AWS credentials are not provided
        if value in [
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
            "",
            "$AWS_ACCESS_KEY_ID",
            "$AWS_SECRET_ACCESS_KEY",
        ]:
            return None
        return value

    @computed_field
    @property
    def modify_access(self) -> bool:
        """
        If the AWS credentials are provided, set the modify access to True. (create = True)

        :return str: The URL of the database.
        """
        if self.aws_access_key_id and self.aws_secret_access_key:
            return True
        _LOGGER.warning(
            "AWS credentials are not provided. The S3 bucket will be read-only."
        )
        return False


class ConfigPepHubClient(BaseModel):
    namespace: Union[str, None] = DEFAULT_PEPHUB_NAMESPACE
    name: Union[str, None] = DEFAULT_PEPHUB_NAME
    tag: Union[str, None] = DEFAULT_PEPHUB_TAG


class ConfigFile(BaseModel):
    database: ConfigDB
    qdrant: ConfigQdrant = None
    server: ConfigServer
    path: ConfigPath
    access_methods: AccessMethods = None
    s3: ConfigS3 = None
    phc: ConfigPepHubClient = None

    model_config = ConfigDict(extra="allow")

    @classmethod
    def from_yaml(cls, path: Path):
        """
        Load the database configuration from a YAML file.

        :param path: The path to the YAML file.

        :returns: DatabaseConfig: The database configuration.
        """
        return cls.model_validate(load_yaml(path.as_posix()))
