from typing import Optional, List, Union
from pydantic import BaseModel, ConfigDict

from bbconf.const_new import DEFAULT_VEC2VEC_MODEL, DEFAULT_TEXT2VEC_MODEL, DEFAULT_REGION2_VEC_MODEL


class ConfigDB(BaseModel):
    host: str
    port: int = 5432
    user: str
    password: str
    database: str = "bedbase"
    dialect: str = "postgresql"
    driver: Optional[str] = "psycopg"


class ConfigQdrant(BaseModel):
    host: str
    port: int = 6333
    api_key: Optional[str] = None
    collection: str = "bedbase"


class ConfigServer(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000


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
    bucket: Union[str, None] = "bedbase"


class ConfigPepHubClient(BaseModel):
    namespace: Union[str, None] = "databio"
    name: Union[str, None] = "allbeds"
    tag: Union[str, None] = "bedbase"


class ConfigFile(BaseModel):
    database: ConfigDB
    qdrant: ConfigQdrant
    server: ConfigServer
    path: ConfigPath
    access_methods: AccessMethods = None
    s3: ConfigS3 = None
    phc: ConfigPepHubClient = None

    model_config = ConfigDict(extra="allow")
