from typing import Optional, Union

from pydantic import BaseModel, ConfigDict, computed_field
from yacman import load_yaml
from pathlib import Path


from bbconf.config_parser.const import (
    DEFAULT_DB_DIALECT,
    DEFAULT_DB_DRIVER,
    DEFAULT_DB_NAME,
    DEFAULT_DB_PORT,
    DEFAULT_PEPHUB_NAME,
    DEFAULT_PEPHUB_NAMESPACE,
    DEFAULT_PEPHUB_TAG,
    DEFAULT_QDRANT_COLLECTION_NAME,
    DEFAULT_QDRANT_PORT,
    DEFAULT_REGION2_VEC_MODEL,
    DEFAULT_S3_BUCKET,
    DEFAULT_SERVER_HOST,
    DEFAULT_SERVER_PORT,
    DEFAULT_TEXT2VEC_MODEL,
    DEFAULT_VEC2VEC_MODEL,
)


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
        return f"{self.dialect}+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


class ConfigQdrant(BaseModel):
    host: str
    port: int = DEFAULT_QDRANT_PORT
    api_key: Optional[str] = None
    collection: str = DEFAULT_QDRANT_COLLECTION_NAME


class ConfigServer(BaseModel):
    host: str = DEFAULT_SERVER_HOST
    port: int = DEFAULT_SERVER_PORT


class ConfigPath(BaseModel):
    region2vec: str = DEFAULT_REGION2_VEC_MODEL
    vec2vec: str = DEFAULT_VEC2VEC_MODEL
    text2vec: str = DEFAULT_TEXT2VEC_MODEL


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
