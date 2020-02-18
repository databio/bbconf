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
CFG_PIP_OUTPUT_KEY = "pipelines_output"
CFG_BED_INDEX_KEY = "bed_index"
CFG_BEDSET_INDEX_KEY = "bedset_index"

CFG_KEYS = ["CFG_PATH_KEY", "CFG_SERVER_KEY", "CFG_DATABASE_KEY", "CFG_HOST_KEY",
            "CFG_PORT_KEY", "CFG_PIP_OUTPUT_KEY", "CFG_BED_INDEX_KEY", "CFG_BEDSET_INDEX_KEY"]

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
JSON_MEAN_ABS_TSS_DIST_KEY = "mean_absolute_TSS_dist"
JSON_MD5SUM_KEY = "md5sum"
JSON_PLOTS_KEY = "plots"
JSON_EXON_FREQUENCY_KEY = "exon_frequency"
JSON_INTRON_FREQUENCY_KEY = "intron_frequency"
JSON_INTERGENIC_FREQUENCY_KEY = "intergenic_frequency"
JSON_PROMOTERCORE_FREQUENCY_KEY = "promoterCore_frequency"
JSON_PROMOTERPROX_FREQUENCY_KEY = "promoterProx_frequency"
JSON_EXON_PERCENTAGE_KEY = "exon_percentage"
JSON_INTRON_PERCENTAGE_KEY = "intron_percentage"
JSON_INTERGENIC_PERCENTAGE_KEY = "intergenic_percentage"
JSON_PROMOTERCORE_PERCENTAGE_KEY = "promoterCore_percentage"
JSON_PROMOTERPROX_PERCENTAGE_KEY = "promoterProx_percentage"
JSON_BEDSETS_AFFILIATION_KEY = "bedsets_affiliation"

JSON_NUMERIC_KEY_NAMES = ["JSON_GC_CONTENT_KEY", "JSON_REGIONS_NO_KEY", "JSON_MEAN_ABS_TSS_DIST_KEY",
                     "JSON_EXON_FREQUENCY_KEY", "JSON_INTRON_FREQUENCY_KEY", "JSON_PROMOTERPROX_FREQUENCY_KEY",
                     "JSON_INTERGENIC_FREQUENCY_KEY", "JSON_PROMOTERCORE_FREQUENCY_KEY",
                     "JSON_PROMOTERPROX_PERCENTAGE_KEY", "JSON_EXON_PERCENTAGE_KEY", "JSON_INTRON_PERCENTAGE_KEY",
                     "JSON_INTERGENIC_PERCENTAGE_KEY", "JSON_PROMOTERCORE_PERCENTAGE_KEY"]

JSON_NUMERIC_KEY_VALUES = [JSON_GC_CONTENT_KEY, JSON_REGIONS_NO_KEY, JSON_MEAN_ABS_TSS_DIST_KEY,
                     JSON_EXON_FREQUENCY_KEY, JSON_INTRON_FREQUENCY_KEY, JSON_PROMOTERPROX_FREQUENCY_KEY,
                     JSON_INTERGENIC_FREQUENCY_KEY, JSON_PROMOTERCORE_FREQUENCY_KEY,
                     JSON_PROMOTERPROX_PERCENTAGE_KEY, JSON_EXON_PERCENTAGE_KEY, JSON_INTRON_PERCENTAGE_KEY,
                     JSON_INTERGENIC_PERCENTAGE_KEY, JSON_PROMOTERCORE_PERCENTAGE_KEY]

JSON_BEDSET_MEANS_KEY = "bedset_means"
JSON_BEDSET_SD_KEY = "bedset_standard_deviation"
JSON_BEDSET_TAR_PATH_KEY = "bedset_tar_archive_path"
JSON_BEDSET_BEDFILES_GD_STATS_KEY = "bedset_bedfiles_gd_stats"
JSON_BEDSET_IGD_DB_KEY = "bedset_igd_database_path"
JSON_BEDSET_GD_STATS = "bedset_gd_stats"
JSON_BEDSET_KEY_VALUES = [JSON_BEDSET_MEANS_KEY, JSON_BEDSET_SD_KEY, JSON_BEDSET_TAR_PATH_KEY,
                    JSON_BEDSET_BEDFILES_GD_STATS_KEY, JSON_BEDSET_IGD_DB_KEY, JSON_BEDSET_GD_STATS]
JSON_BEDSET_KEY_NAMES = ["JSON_BEDSET_MEANS_KEY", "JSON_BEDSET_SD_KEY", "JSON_BEDSET_TAR_PATH_KEY",
                    "JSON_BEDSET_BEDFILES_GD_STATS_KEY", "JSON_BEDSET_IGD_DB_KEY", "JSON_BEDSET_GD_STATS"]

JSON_KEYS = ["JSON_GC_CONTENT_KEY", "JSON_ID_KEY", "JSON_PLOTS_KEY", "JSON_BEDSETS_AFFILIATION_KEY"] + \
            JSON_NUMERIC_KEY_NAMES + JSON_BEDSET_KEY_NAMES

_PERC_TXT = "Percentage of regions in "
_FREQ_TXT = "Frequency of regions in "
JSON_DICTS_KEY_DESCS = {JSON_GC_CONTENT_KEY: "GC content", JSON_ID_KEY: "BED file ID",
                        JSON_REGIONS_NO_KEY: "Number of regions", JSON_MD5SUM_KEY: "BED file md5 checksum",
                        JSON_MEAN_ABS_TSS_DIST_KEY: "Mean absolute distance from transcription start sites",
                        JSON_PROMOTERPROX_PERCENTAGE_KEY: _PERC_TXT + "promoter proximity",
                        JSON_PROMOTERCORE_PERCENTAGE_KEY: _PERC_TXT + "promoter core",
                        JSON_EXON_PERCENTAGE_KEY: _PERC_TXT + "exons",
                        JSON_INTRON_PERCENTAGE_KEY: _PERC_TXT + "introns",
                        JSON_INTERGENIC_PERCENTAGE_KEY: _PERC_TXT + "intergenic",
                        JSON_PROMOTERPROX_FREQUENCY_KEY: _FREQ_TXT + "promoter proximity",
                        JSON_PROMOTERCORE_FREQUENCY_KEY: _FREQ_TXT + "promoter core",
                        JSON_EXON_FREQUENCY_KEY: _FREQ_TXT + "exons",
                        JSON_INTRON_FREQUENCY_KEY: _FREQ_TXT + "introns",
                        JSON_INTERGENIC_FREQUENCY_KEY: _FREQ_TXT + "intergenic",
                        JSON_BEDSET_MEANS_KEY: "Average bedset statistics",
                        JSON_BEDSET_SD_KEY: "Standard deviation of bedset statistics",
                        JSON_BEDSET_TAR_PATH_KEY: "TAR archive",
                        JSON_BEDSET_BEDFILES_GD_STATS_KEY: "Individual bedfiles statistics CSV",
                        JSON_BEDSET_IGD_DB_KEY: "Bedset iGD database",
                        JSON_BEDSET_GD_STATS: "Bedset statistics CSV"}

__all__ = ["BED_INDEX", "BEDSET_INDEX", "SEARCH_TERMS", "RAW_BEDFILE_KEY", "CFG_ENV_VARS",
           "ES_CLIENT_KEY", "DB_DEFAULT_HOST", "SERVER_DEFAULT_PORT", "SERVER_DEFAULT_HOST",
           "PKG_NAME", "IDX_MAP", "BEDFILE_PATH_KEY", "DEFAULT_SECTION_VALUES", "JSON_DICTS_KEY_DESCS",
           "JSON_KEYS", "JSON_NUMERIC_KEY_VALUES", "JSON_NUMERIC_KEY_NAMES", "JSON_BEDSET_KEY_VALUES",
           "JSON_BEDSET_KEY_NAMES"] + CFG_KEYS + JSON_KEYS
