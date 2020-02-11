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
BEDFILE_PATH_KEY = "bedfile_path"

DB_DEFAULT_HOST = "localhost"

SERVER_DEFAULT_PORT = 80
SERVER_DEFAULT_HOST = '0.0.0.0'

ES_CLIENT_KEY = "elasticsearch_client"

# config file constants
CFG_PATH_KEY = "path"
CFG_SERVER_KEY = "server"
CFG_DATABASE_KEY = "database"
CFG_HOST_KEY = "host"
CFG_PORT_KEY = "port"
CFG_BEDSTAT_OUTPUT_KEY = "bedstat_output"
CFG_BED_INDEX_KEY = "bed_index"
CFG_BEDSET_INDEX_KEY = "bedset_index"

CFG_KEYS = ["CFG_PATH_KEY", "CFG_SERVER_KEY", "CFG_DATABASE_KEY", "CFG_HOST_KEY",
            "CFG_PORT_KEY", "CFG_BEDSTAT_OUTPUT_KEY", "CFG_BED_INDEX_KEY", "CFG_BEDSET_INDEX_KEY"]

DEFAULT_SECTION_VALUES = {
    CFG_DATABASE_KEY: {
        CFG_HOST_KEY: DB_DEFAULT_HOST,
        CFG_BED_INDEX_KEY: BED_INDEX,
        CFG_BEDSET_INDEX_KEY: BEDSET_INDEX
    },
    CFG_SERVER_KEY: {
        CFG_HOST_KEY: SERVER_DEFAULT_HOST,
        CFG_PORT_KEY: SERVER_DEFAULT_PORT
    }
}

IDX_MAP = {CFG_BED_INDEX_KEY: BED_INDEX, CFG_BEDSET_INDEX_KEY: BEDSET_INDEX}

# JSON bed metadata constants and descriptions
# (the keys are actually established in bedstat/tools/regionstat.R)
JSON_GC_CONTENT_KEY = "gc_content"
JSON_ID_KEY = "id"
JSON_REGIONS_NO_KEY = "regions_no"
JSON_MEAN_ABS_TSS_DIST_KEY = "mean_abs_TSS_dist"
JSON_GEN_PART_KEY = "genomic_partitions"
JSON_MD5SUM_KEY = "md5sum"
JSON_PLOTS_KEY = "plots"
JSON_EXON_FREQUENCY = "exon_frequency"
JSON_INTRON_FREQUENCY = "intron_frequency"
JSON_INTERGENIC_FREQUENCY = "intergenic_frequency"
JSON_PROMOTERCORE_FREQUENCY = "promoterCore_frequency"
JSON_PROMOTERPROX_FREQUENCY = "promoterProx_frequency"
JSON_EXON_PERCENTAGE = "exon_percentage"
JSON_INTRON_PERCENTAGE = "intron_percentage"
JSON_INTERGENIC_PERCENTAGE = "intergenic_percentage"
JSON_PROMOTERCORE_PERCENTAGE = "promoterCore_percentage"
JSON_PROMOTERPROX_PERCENTAGE = "promoterProx_percentage"

JSON_KEYS = ["JSON_GC_CONTENT_KEY", "JSON_ID_KEY", "JSON_REGIONS_NO_KEY",
             "JSON_MEAN_ABS_TSS_DIST_KEY", "JSON_GEN_PART_KEY", "JSON_MD5SUM_KEY",
             "JSON_PLOTS_KEY", "JSON_EXON_FREQUENCY", "JSON_INTRON_FREQUENCY",
             "JSON_INTERGENIC_FREQUENCY", "JSON_PROMOTERCORE_FREQUENCY", "JSON_PROMOTERPROX",
             "JSON_EXON_PERCENTAGE", "JSON_INTRON_PERCENTAGE", "JSON_INTERGENIC_PERCENTAGE",
             "JSON_PROMOTERCORE_PERCENTAGE", "JSON_PROMOTERPROX"]

JSON_NUMERIC_KEYS = ["JSON_GC_CONTENT_KEY", "JSON_REGIONS_NO_KEY", "JSON_GEN_PART_KEY",
                     "JSON_MEAN_ABS_TSS_DIST_KEY"]

JSON_GC_CONTENT = {JSON_GC_CONTENT_KEY: "GC content"}
JSON_ID = {JSON_ID_KEY: "BED file ID"}
JSON_REGIONS_NO = {JSON_REGIONS_NO_KEY: "Number of regions"}
JSON_MEAN_ABS_TSS_DIST = {JSON_MEAN_ABS_TSS_DIST_KEY: "Mean absolute distance from transcription start sites"}
JSON_GEN_PART = {JSON_GEN_PART_KEY: "Genomic partitions"}
JSON_MD5SUM = {JSON_MD5SUM_KEY: "BED file md5 checksum"}

JSON_DICTS_KEY_DESCS = ["JSON_GC_CONTENT", "JSON_ID", "JSON_REGIONS_NO",
                        "JSON_MEAN_ABS_TSS_DIST", "JSON_GEN_PART", "JSON_MD5SUM"]

JSON_DICTS_KEY_DESCS = {JSON_GC_CONTENT_KEY: "GC content", JSON_ID_KEY: "BED file ID",
                        JSON_REGIONS_NO_KEY: "Number of regions", JSON_MD5SUM_KEY: "BED file md5 checksum",
                        JSON_MEAN_ABS_TSS_DIST_KEY: "Mean absolute distance from transcription start sites",
                        JSON_GEN_PART_KEY: "Genomic partitions"}

__all__ = ["BED_INDEX", "BEDSET_INDEX", "SEARCH_TERMS", "RAW_BEDFILE_KEY", "CFG_ENV_VARS",
           "ES_CLIENT_KEY", "DB_DEFAULT_HOST", "SERVER_DEFAULT_PORT", "SERVER_DEFAULT_HOST",
           "PKG_NAME", "IDX_MAP", "BEDFILE_PATH_KEY", "DEFAULT_SECTION_VALUES", "JSON_DICTS_KEY_DESCS",
           "JSON_KEYS", "JSON_NUMERIC_KEYS"] + CFG_KEYS + JSON_KEYS
