"""
Constant variables shared among packages that constitute bedbase project
"""

import os

SCHEMA_DIRNAME = "schemas"
SCHEMAS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), SCHEMA_DIRNAME)
BED_TABLE_SCHEMA = os.path.join(SCHEMAS_PATH, "bedfiles_schema.yaml")
BEDSET_TABLE_SCHEMA = os.path.join(SCHEMAS_PATH, "bedsets_schema.yaml")

PKG_NAME = "bbconf"
DOC_URL = "TBA"  # TODO: add documentation URL once it's established

BED_TABLE = "bedfiles"
BEDSET_TABLE = "bedsets"
REL_TABLE = "bedset_bedfiles"

CFG_ENV_VARS = ["BEDBASE"]

DB_DEFAULT_HOST = "localhost"
DB_DEFAULT_USER = "postgres"
DB_DEFAULT_PASSWORD = "bedbasepassword"
DB_DEFAULT_NAME = "postgres"
DB_DEFAULT_PORT = 5432

SERVER_DEFAULT_PORT = 80
SERVER_DEFAULT_HOST = "0.0.0.0"

PATH_DEFAULT_REMOTE_URL_BASE = None

PIPESTATS_KEY = "__pipestats"

HIDDEN_ATTR_KEYS = PIPESTATS_KEY

# bedset_bedfiles table definition

REL_BED_ID_KEY = "bedfile_id"
REL_BEDSET_ID_KEY = "bedset_id"

# config file constants
CFG_PATH_KEY = "path"
CFG_SERVER_KEY = "server"
CFG_DATABASE_KEY = "database"
CFG_NAME_KEY = "name"
CFG_HOST_KEY = "host"
CFG_PORT_KEY = "port"
CFG_PASSWORD_KEY = "password"
CFG_USER_KEY = "user"
CFG_BEDSTAT_DIR_KEY = "bedstat_dir"
CFG_BEDBUNCHER_DIR_KEY = "bedbuncher_dir"
CFG_PIPELINE_OUT_PTH_KEY = "pipeline_output_path"
CFG_REMOTE_URL_BASE_KEY = "remote_url_base"

DEFAULT_SECTION_VALUES = {
    CFG_PATH_KEY: {CFG_REMOTE_URL_BASE_KEY: PATH_DEFAULT_REMOTE_URL_BASE},
    CFG_DATABASE_KEY: {
        CFG_USER_KEY: DB_DEFAULT_USER,
        CFG_PASSWORD_KEY: DB_DEFAULT_PASSWORD,
        CFG_NAME_KEY: DB_DEFAULT_NAME,
        CFG_PORT_KEY: DB_DEFAULT_PORT,
        CFG_HOST_KEY: DB_DEFAULT_HOST,
    },
    CFG_SERVER_KEY: {
        CFG_HOST_KEY: SERVER_DEFAULT_HOST,
        CFG_PORT_KEY: SERVER_DEFAULT_PORT,
    },
}

CFG_KEYS = [
    "CFG_PATH_KEY",
    "CFG_SERVER_KEY",
    "CFG_DATABASE_KEY",
    "CFG_HOST_KEY",
    "CFG_PORT_KEY",
    "CFG_NAME_KEY",
    "CFG_PASSWORD_KEY",
    "CFG_USER_KEY",
    "CFG_REMOTE_URL_BASE_KEY",
    "CFG_PIPELINE_OUT_PTH_KEY",
    "CFG_BEDSTAT_DIR_KEY",
    "CFG_BEDBUNCHER_DIR_KEY",
    "PIPESTATS_KEY",
]


__all__ = [
    "BED_TABLE",
    "BEDSET_TABLE",
    "REL_TABLE",
    "CFG_ENV_VARS",
    "DB_DEFAULT_HOST",
    "SERVER_DEFAULT_PORT",
    "SERVER_DEFAULT_HOST",
    "PKG_NAME",
    "DEFAULT_SECTION_VALUES",
    "HIDDEN_ATTR_KEYS",
    "REL_BED_ID_KEY",
    "REL_BEDSET_ID_KEY",
    "BED_TABLE_SCHEMA",
    "BEDSET_TABLE_SCHEMA",
] + CFG_KEYS
