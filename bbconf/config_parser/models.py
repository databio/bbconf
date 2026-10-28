import logging
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, computed_field, field_validator
from yacman import load_yaml

from bbconf.config_parser.const import (  # DEFAULT_VEC2VEC_MODEL,
    DEFAULT_DB_DIALECT,
    DEFAULT_DB_DRIVER,
    DEFAULT_DB_NAME,
    DEFAULT_DB_PORT,
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
    driver: str | None = DEFAULT_DB_DRIVER
    run_migrations: bool = False

    model_config = ConfigDict(extra="forbid")

    @computed_field
    @property
    def url(self) -> str:
        """
        The URL of the database.

        Returns:
            The URL of the database.
        """
        return f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class ConfigQdrant(BaseModel):
    host: str
    port: int = DEFAULT_QDRANT_PORT
    api_key: str | None = None
    file_collection: str = DEFAULT_QDRANT_FILE_COLLECTION_NAME
    text_collection: str | None = DEFAULT_QDRANT_BIVEC_COLLECTION_NAME
    hybrid_collection: str | None = DEFAULT_QDRANT_HYBRID_COLLECTION_NAME


class ConfigServer(BaseModel):
    host: str = DEFAULT_SERVER_HOST
    port: int = DEFAULT_SERVER_PORT


class ConfigPath(BaseModel):
    region2vec: str = DEFAULT_REGION2_VEC_MODEL
    # vec2vec: str = DEFAULT_VEC2VEC_MODEL
    text2vec: str = DEFAULT_TEXT2VEC_MODEL
    sparse_model: str = DEFAULT_SPARSE_MODEL
    umap_model: str | None = None  # Path or link to pre-trained UMAP model


class ConfigEmbeddingServer(BaseModel):
    """
    HTTP servers to get embeddings from, instead of loading the models in-process.
    Each URL that is set switches that one model to remote; unset means local.
    The models are still named in `path`, and are checked against the servers.
    """

    text_url: str | None = None  # TEI server for path.text2vec
    sparse_url: str | None = None  # TEI server for path.sparse_model
    region_url: str | None = None  # region embedding API server for path.region2vec
    api_key: str | None = None

    model_config = ConfigDict(extra="forbid")

    @field_validator("*")
    def unset_blank(cls, value):
        # yacman leaves unset env vars as literal "$VAR"
        if not value or value.startswith("$"):
            return None
        return value


class AccessMethodsStruct(BaseModel):
    type: str
    description: str | None = None
    prefix: str


class AccessMethods(BaseModel):
    http: AccessMethodsStruct | None = None
    s3: AccessMethodsStruct | None = None
    local: AccessMethodsStruct | None = None


class ConfigS3(BaseModel):
    endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    bucket: str | None = DEFAULT_S3_BUCKET

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

        Returns:
            True if AWS credentials are provided, False otherwise.
        """
        if self.aws_access_key_id and self.aws_secret_access_key:
            return True
        _LOGGER.warning(
            "AWS credentials are not provided. The S3 bucket will be read-only."
        )
        return False


class ConfigAnalysis(BaseModel):
    """Analysis backend configuration.

    Controls which statistics engine is used for BED file analysis.
    """

    backend: Literal["r", "gtars"] = "r"

    model_config = ConfigDict(extra="forbid")


class ConfigFile(BaseModel):
    database: ConfigDB
    qdrant: ConfigQdrant | None = None
    server: ConfigServer
    path: ConfigPath
    embedding_server: ConfigEmbeddingServer | None = None
    access_methods: AccessMethods | None = None
    s3: ConfigS3 | None = None
    analysis: ConfigAnalysis | None = ConfigAnalysis()

    model_config = ConfigDict(extra="allow")

    @classmethod
    def from_yaml(cls, path: Path):
        """
        Load the database configuration from a YAML file.

        Args:
            path: The path to the YAML file.

        Returns:
            The database configuration.
        """
        return cls.model_validate(load_yaml(path.as_posix()))
