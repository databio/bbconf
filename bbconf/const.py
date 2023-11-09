"""
Constant variables shared among packages that constitute bedbase project
"""

import os

SCHEMA_DIRNAME = "schemas"
SCHEMAS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), SCHEMA_DIRNAME)
BED_TABLE_SCHEMA = os.path.join(SCHEMAS_PATH, "bedfiles_schema.yaml")
BEDSET_TABLE_SCHEMA = os.path.join(SCHEMAS_PATH, "bedsets_schema.yaml")
DIST_TABLE_SCHEMA = os.path.join(SCHEMAS_PATH, "distance_schema.yaml")

PKG_NAME = "bbconf"
DOC_URL = "TBA"  # TODO: add documentation URL once it's established

BED_TABLE = "bedfile__sample"
BEDSET_TABLE = "bedsets__sample"

BEDFILES_REL_KEY = "bedfiles"
BEDSETS_REL_KEY = "bedsets"

BEDFILE_BEDSET_ASSOCIATION_TABLE_KEY = "bedset_bedfiles"

CFG_ENV_VARS = ["BEDBASE"]

PIPESTATS_KEY = "__pipestats"
COMMON_DECL_BASE_KEY = "__common_declarative_base"

HIDDEN_ATTR_KEYS = [PIPESTATS_KEY, COMMON_DECL_BASE_KEY]

# config file constants
CFG_PATH_KEY = "path"
CFG_PATH_BEDSTAT_DIR_KEY = "bedstat_dir"
CFG_PATH_BEDBUNCHER_DIR_KEY = "bedbuncher_dir"
CFG_PATH_PIPELINE_OUTPUT_KEY = "pipeline_output_path"
CFG_PATH_REGION2VEC_KEY = "region2vec"
CFG_PATH_VEC2VEC_KEY = "vec2vec"


CFG_DATABASE_KEY = "database"
CFG_DATABASE_NAME_KEY = "name"
CFG_DATABASE_HOST_KEY = "host"
CFG_DATABASE_PORT_KEY = "port"
CFG_DATABASE_PASSWORD_KEY = "password"
CFG_DATABASE_USER_KEY = "user"

CFG_QDRANT_KEY = "qdrant"

CFG_QDRANT_HOST_KEY = "host"
CFG_QDRANT_PORT_KEY = "port"
CFG_QDRANT_API_KEY = "api_key"
CFG_QDRANT_COLLECTION_NAME_KEY = "collection"

CFG_SERVER_KEY = "server"
CFG_SERVER_HOST_KEY = "host"
CFG_SERVER_PORT_KEY = "port"

CFG_REMOTE_KEY = "remotes"

DB_DEFAULT_HOST = "localhost"
DB_DEFAULT_USER = "postgres"
DB_DEFAULT_PASSWORD = "bedbasepassword"
DB_DEFAULT_NAME = "postgres"
DB_DEFAULT_PORT = 5432
DB_DEFAULT_DIALECT = "postgresql"

CFG_ACCESS_METHOD_KEY = "access_methods"

DEFAULT_QDRANT_HOST = "localhost"
DEFAULT_QDRANT_PORT = 6333
DEFAULT_QDRANT_COLLECTION_NAME = "bedbase"
DEFAULT_QDRANT_API_KEY = None

SERVER_DEFAULT_PORT = 80
SERVER_DEFAULT_HOST = "0.0.0.0"

DEFAULT_SECTION_VALUES = {
    CFG_DATABASE_KEY: {
        CFG_DATABASE_USER_KEY: DB_DEFAULT_USER,
        CFG_DATABASE_PASSWORD_KEY: DB_DEFAULT_PASSWORD,
        CFG_DATABASE_NAME_KEY: DB_DEFAULT_NAME,
        CFG_DATABASE_PORT_KEY: DB_DEFAULT_PORT,
        CFG_DATABASE_HOST_KEY: DB_DEFAULT_HOST,
    },
    CFG_SERVER_KEY: {
        CFG_SERVER_HOST_KEY: SERVER_DEFAULT_HOST,
        CFG_SERVER_PORT_KEY: SERVER_DEFAULT_PORT,
    },
    CFG_QDRANT_KEY: {
        CFG_QDRANT_HOST_KEY: DEFAULT_QDRANT_HOST,
        CFG_QDRANT_PORT_KEY: DEFAULT_QDRANT_PORT,
        CFG_QDRANT_COLLECTION_NAME_KEY: DEFAULT_QDRANT_COLLECTION_NAME,
        CFG_QDRANT_API_KEY: DEFAULT_QDRANT_API_KEY,
    },
}

DEFAULT_HF_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DEFAULT_VEC2VEC_MODEL = "databio/v2v-MiniLM-v2-ATAC-hg38"
DEFAULT_REGION2_VEC_MODEL = "databio/r2v-ChIP-atlas-hg38"

DRS_ACCESS_URL = "{server_url}/objects/{object_id}/access/{access_id}"
