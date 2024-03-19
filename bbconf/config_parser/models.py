from typing import Optional, List, Union
from pydantic import BaseModel, ConfigDict

from bbconf.config_parser.const import (
    DEFAULT_VEC2VEC_MODEL,
    DEFAULT_TEXT2VEC_MODEL,
    DEFAULT_REGION2_VEC_MODEL,
    DEFAULT_DB_DIALECT,
    DEFAULT_DB_NAME,
    DEFAULT_DB_DRIVER,
    DEFAULT_QDRANT_COLLECTION_NAME,
    DEFAULT_DB_PORT,
    DEFAULT_QDRANT_PORT,
    DEFAULT_SERVER_HOST,
    DEFAULT_SERVER_PORT,
    DEFAULT_PEPHUB_NAMESPACE,
    DEFAULT_PEPHUB_NAME,
    DEFAULT_PEPHUB_TAG,
    DEFAULT_S3_BUCKET,
)


class ConfigDB(BaseModel):
    host: str
    port: int = DEFAULT_DB_PORT
    user: str
    password: str
    database: str = DEFAULT_DB_NAME
    dialect: str = DEFAULT_DB_DIALECT
    driver: Optional[str] = DEFAULT_DB_DRIVER


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
