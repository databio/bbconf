"""
Constant variables shared among packages that constitute bedbase project
"""
PKG_NAME = "bbconf"
DOC_URL = "TBA"  # add documentation URL once it's established

BED_INDEX = "bedfiles"
BEDSET_INDEX = "bedsets"

CFG_ENV_VARS = ["BEDBASE"]

SEARCH_TERMS = ['cellType', 'cellTypeSubtype', 'antibody', 'mappingGenome',
                'description', 'tissue', 'species', 'protocol', 'genome']
RAW_BEDFILE_KEY = "raw_bedfile"

DB_DEFAULT_HOST = "localhost"

SERVER_DEFAULT_PORT = 80
SERVER_DEFAULT_HOST = '0.0.0.0'

# config file constants
CFG_PATH_KEY = "path"
CFG_SERVER_KEY = "server"
CFG_DATABASE_KEY = "database"
CFG_HOST_KEY = "host"
CFG_PORT_KEY = "port"
CFG_BEDSTAT_OUTPUT_KEY = "bedstat_output"
CFG_BED_INDEX_KEY = "bed_index"
CFG_BEDSET_INDEX_KEY = "bedset_index"

IDX_MAP = {CFG_BED_INDEX_KEY: BED_INDEX, CFG_BEDSET_INDEX_KEY: BEDSET_INDEX}

CFG_KEYS = ["CFG_PATH_KEY", "CFG_SERVER_KEY", "CFG_DATABASE_KEY", "CFG_HOST_KEY",
            "CFG_PORT_KEY", "CFG_BEDSTAT_OUTPUT_KEY", "CFG_BED_INDEX_KEY", "CFG_BEDSET_INDEX_KEY"]

__all__ = ["BED_INDEX", "BEDSET_INDEX", "SEARCH_TERMS", "RAW_BEDFILE_KEY", "CFG_ENV_VARS",
           "DB_DEFAULT_HOST", "SERVER_DEFAULT_PORT", "SERVER_DEFAULT_HOST", "PKG_NAME", "IDX_MAP"] + CFG_KEYS