"""
Constant variables shared among packages that constitute bedbase project
"""


def _make_columns_list(template, nonnull_list, unique_list, col_list):
    """
    Create a list of column initialization strings with constraints

    :param str template: column template, containing type
    :param list[str] nonnull_list: list of column names that are not nullable
    :param list[str] unique_list: list of column names that must be unique
    :param list[str] col_list: list of columns names to process
    :return list[str]: processed list of column names, enrichecd with
        types and constraints
    """
    result = []
    for c in col_list:
        res = template.format(c)
        if c in nonnull_list:
            res += " NOT NULL"
        if c in unique_list:
            res += " UNIQUE"
        result.append(res)
    return result


PKG_NAME = "bbconf"
DOC_URL = "TBA"  # add documentation URL once it's established

BED_TABLE = "bedfiles"
BEDSET_TABLE = "bedsets"
REL_TABLE = "bedset_bedfiles"

CFG_ENV_VARS = ["BEDBASE"]

RAW_BEDFILE_KEY = "raw_bedfile"
BEDFILE_PATH_KEY = "bedfile_path"

DB_DEFAULT_HOST = "localhost"
DB_DEFAULT_USER = "postgres"
DB_DEFAULT_PASSWORD = "bedbasepassword"
DB_DEFAULT_NAME = "postgres"
DB_DEFAULT_PORT = 5432


SERVER_DEFAULT_PORT = 80
SERVER_DEFAULT_HOST = '0.0.0.0'

PATH_DEFAULT_REMOTE_URL_BASE = None

PG_CLIENT_KEY = "__postgres_client"

HIDDEN_ATTR_KEYS = (PG_CLIENT_KEY)

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
CFG_BED_TABLE_KEY = "bed_table"
CFG_REL_TABLE_KEY = "relationship_table"
CFG_BEDSET_TABLE_KEY = "bedset_table"

CFG_KEYS = [
    "CFG_PATH_KEY", "CFG_SERVER_KEY", "CFG_DATABASE_KEY", "CFG_HOST_KEY",
    "CFG_PORT_KEY", "CFG_BED_TABLE_KEY", "CFG_BEDSET_TABLE_KEY", "CFG_NAME_KEY",
    "CFG_PASSWORD_KEY", "CFG_USER_KEY", "CFG_REL_TABLE_KEY",
    "CFG_REMOTE_URL_BASE_KEY", "CFG_PIPELINE_OUT_PTH_KEY", "CFG_BEDSTAT_DIR_KEY",
    "CFG_BEDBUNCHER_DIR_KEY"]

DEFAULT_SECTION_VALUES = {
    CFG_PATH_KEY: {
        CFG_REMOTE_URL_BASE_KEY: PATH_DEFAULT_REMOTE_URL_BASE
    },
    CFG_DATABASE_KEY: {
        CFG_USER_KEY: DB_DEFAULT_USER,
        CFG_PASSWORD_KEY: DB_DEFAULT_PASSWORD,
        CFG_NAME_KEY: DB_DEFAULT_NAME,
        CFG_PORT_KEY: DB_DEFAULT_PORT,
        CFG_HOST_KEY: DB_DEFAULT_HOST,
        CFG_BED_TABLE_KEY: BED_TABLE,
        CFG_REL_TABLE_KEY: REL_TABLE,
        CFG_BEDSET_TABLE_KEY: BEDSET_TABLE
    },
    CFG_SERVER_KEY: {
        CFG_HOST_KEY: SERVER_DEFAULT_HOST,
        CFG_PORT_KEY: SERVER_DEFAULT_PORT
    }
}

IDX_MAP = {CFG_BED_TABLE_KEY: BED_TABLE,
           CFG_BEDSET_TABLE_KEY: BEDSET_TABLE,
           CFG_REL_TABLE_KEY: REL_TABLE}

# TODO: streamline DB column names naming
# JSON bed metadata constants and descriptions (the keys are actually
# established in bedstat/tools/regionstat.R and some of them
# in GenomicDistributions)
JSON_GC_CONTENT_KEY = "gc_content"
JSON_NAME_KEY = "name"
JSON_GENOME_KEY = "genome"
JSON_PROTOCOL_KEY = "exp_protocol"
JSON_CELL_TYPE_KEY = "cell_type"
JSON_TISSUE_KEY = "tissue"
JSON_ANTIBODY_KEY = "antibody"
JSON_TREATMENT_KEY = "treatment"
JSON_DATA_SOURCE_KEY = "data_source"
JSON_DESCRIPTION_KEY = "description"
JSON_REGIONS_NO_KEY = "regions_no"
JSON_MEAN_ABS_TSS_DIST_KEY = "mean_absolute_TSS_dist"
JSON_MEAN_REGION_WIDTH = "mean_region_width"
JSON_MD5SUM_KEY = "md5sum"
JSON_PLOTS_KEY = "plots"
JSON_OTHER_KEY = "other"
JSON_5UTR_FREQUENCY_KEY = "fiveutr_frequency"
JSON_5UTR_PERCENTAGE_KEY = "fiveutr_percentage"
JSON_3UTR_PERCENTAGE_KEY = "threeutr_percentage"
JSON_3UTR_FREQUENCY_KEY = "threeutr_frequency"
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
JSON_BEDSET_PEP_KEY = "bedset_pep"
JSON_BEDSET_BED_IDS_KEY = "bedset_bed_ids"

JSON_METADATA_NAMES = [
    "JSON_GENOME_KEY", "JSON_PROTOCOL_KEY", "JSON_CELL_TYPE_KEY",
    "JSON_TISSUE_KEY", "JSON_ANTIBODY_KEY",
    "JSON_TREATMENT_KEY", "JSON_DATA_SOURCE_KEY", "JSON_DESCRIPTION_KEY",
    "JSON_NAME_KEY", "JSON_MD5SUM_KEY", "JSON_PLOTS_KEY", "JSON_OTHER_KEY",
    "BEDFILE_PATH_KEY"]

JSON_METADATA_VALUES = [eval(x) for x in JSON_METADATA_NAMES]

JSON_STATS_SECTION_KEY = "statistics"
JSON_METADATA_SECTION_KEY = "metadata"

JSON_FLOAT_KEY_NAMES = [
    "JSON_GC_CONTENT_KEY", "JSON_MEAN_ABS_TSS_DIST_KEY",
    "JSON_MEAN_REGION_WIDTH", "JSON_EXON_FREQUENCY_KEY",
    "JSON_INTRON_FREQUENCY_KEY", "JSON_PROMOTERPROX_FREQUENCY_KEY",
    "JSON_INTERGENIC_FREQUENCY_KEY", "JSON_PROMOTERCORE_FREQUENCY_KEY",
    "JSON_5UTR_FREQUENCY_KEY", "JSON_5UTR_PERCENTAGE_KEY",
    "JSON_3UTR_FREQUENCY_KEY", "JSON_3UTR_PERCENTAGE_KEY",
    "JSON_PROMOTERPROX_PERCENTAGE_KEY", "JSON_EXON_PERCENTAGE_KEY",
    "JSON_INTRON_PERCENTAGE_KEY", "JSON_INTERGENIC_PERCENTAGE_KEY",
    "JSON_PROMOTERCORE_PERCENTAGE_KEY"]

JSON_FLOAT_KEY_VALUES = [eval(x) for x in JSON_FLOAT_KEY_NAMES]


JSON_INT_KEY_NAMES = ["JSON_REGIONS_NO_KEY"]

JSON_INT_KEY_VALUES = [eval(x) for x in JSON_INT_KEY_NAMES]

JSON_BEDSET_MEANS_KEY = "bedset_means"
JSON_BEDSET_SD_KEY = "bedset_standard_deviation"
JSON_BEDSET_TAR_PATH_KEY = "bedset_tar_archive_path"
JSON_BEDSET_BEDFILES_GD_STATS_KEY = "bedset_bedfiles_gd_stats"
JSON_BEDSET_IGD_DB_KEY = "bedset_igd_database_path"
JSON_BEDSET_GD_STATS_KEY = "bedset_gd_stats"
JSON_BEDSET_KEY_VALUES = [
    JSON_BEDSET_MEANS_KEY, JSON_BEDSET_SD_KEY, JSON_BEDSET_TAR_PATH_KEY,
    JSON_BEDSET_BEDFILES_GD_STATS_KEY, JSON_BEDSET_IGD_DB_KEY,
    JSON_BEDSET_GD_STATS_KEY]
JSON_BEDSET_KEY_NAMES = [
    "JSON_BEDSET_MEANS_KEY", "JSON_BEDSET_SD_KEY", "JSON_BEDSET_TAR_PATH_KEY",
    "JSON_BEDSET_BEDFILES_GD_STATS_KEY", "JSON_BEDSET_IGD_DB_KEY",
    "JSON_BEDSET_GD_STATS_KEY", "JSON_BEDSET_PEP_KEY", "JSON_BEDSET_BED_IDS_KEY"]

JSON_KEYS = ["JSON_GC_CONTENT_KEY", "JSON_NAME_KEY", "JSON_PLOTS_KEY",
             "JSON_MD5SUM_KEY", "JSON_STATS_SECTION_KEY", "JSON_METADATA_VALUES",
             "JSON_METADATA_SECTION_KEY", "JSON_OTHER_KEY"] + \
            JSON_FLOAT_KEY_NAMES +  JSON_INT_KEY_NAMES + JSON_BEDSET_KEY_NAMES \
            + JSON_METADATA_NAMES
          

_PERC_TXT = "Percentage of regions in "
_FREQ_TXT = "Frequency of regions in "
JSON_DICTS_KEY_DESCS = {
    JSON_GC_CONTENT_KEY: "GC content",
    JSON_NAME_KEY: "BED file name",
    JSON_5UTR_FREQUENCY_KEY: _FREQ_TXT + "5' UTR",
    JSON_5UTR_PERCENTAGE_KEY: _PERC_TXT + "5' UTR",
    JSON_3UTR_FREQUENCY_KEY: _FREQ_TXT + "3' UTR",
    JSON_3UTR_PERCENTAGE_KEY: _PERC_TXT + "3' UTR",
    JSON_REGIONS_NO_KEY: "Number of regions",
    JSON_MD5SUM_KEY: "BED file md5 checksum",
    JSON_MEAN_ABS_TSS_DIST_KEY: "Mean absolute distance from transcription start sites",
    JSON_MEAN_REGION_WIDTH: "Mean width of the regions in the BED file",
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
    JSON_BEDSET_GD_STATS_KEY: "Bedset statistics CSV",
    JSON_BEDSET_PEP_KEY: "Beset PEP",
    JSON_BEDSET_BED_IDS_KEY: "BED files in this set"
}

# bedfiles table columns definition

COL_FLOAT = '{} FLOAT'
COL_INT = '{} INT'
COL_JSONB = '{} JSONB'
COL_CHAR = "{} VARCHAR(300)"
ID_COL = "id BIGSERIAL PRIMARY KEY"

BED_FLOAT_COLS = JSON_FLOAT_KEY_VALUES
BED_INT_COLS = JSON_INT_KEY_VALUES
BED_CHAR_COLS = [JSON_MD5SUM_KEY, BEDFILE_PATH_KEY, JSON_NAME_KEY]
BED_JSONB_COLS = [JSON_PLOTS_KEY, JSON_OTHER_KEY]

BED_NONNULL_COLS = [JSON_NAME_KEY, JSON_MD5SUM_KEY, BEDFILE_PATH_KEY]
BED_UNIQUE_COLS = [JSON_MD5SUM_KEY]

chars = _make_columns_list(COL_CHAR, BED_NONNULL_COLS, BED_UNIQUE_COLS, BED_CHAR_COLS)
floats = _make_columns_list(COL_FLOAT, BED_NONNULL_COLS, BED_UNIQUE_COLS, BED_FLOAT_COLS)
ints = _make_columns_list(COL_INT, BED_NONNULL_COLS, BED_UNIQUE_COLS, BED_INT_COLS)
jsonbs = _make_columns_list(COL_JSONB, BED_NONNULL_COLS, BED_UNIQUE_COLS, BED_JSONB_COLS)

BED_COLUMNS = [ID_COL] + chars + floats + ints + jsonbs

# bedsets table columns definition

BEDSET_CHAR_COLS = [JSON_MD5SUM_KEY, JSON_NAME_KEY, JSON_BEDSET_TAR_PATH_KEY, JSON_BEDSET_BEDFILES_GD_STATS_KEY, JSON_BEDSET_GD_STATS_KEY, JSON_BEDSET_IGD_DB_KEY, JSON_BEDSET_PEP_KEY]
BEDSET_JSONB_COLS = [JSON_PLOTS_KEY, JSON_BEDSET_MEANS_KEY, JSON_BEDSET_SD_KEY, JSON_BEDSET_BED_IDS_KEY]

BEDSET_NONNULL_COLS = [JSON_NAME_KEY, JSON_MD5SUM_KEY]
BEDSET_UNIQUE_COLS = [JSON_MD5SUM_KEY]

chars = _make_columns_list(COL_CHAR, BEDSET_NONNULL_COLS, BEDSET_UNIQUE_COLS, BEDSET_CHAR_COLS)
jsonbs = _make_columns_list(COL_JSONB, BEDSET_NONNULL_COLS, BEDSET_UNIQUE_COLS, BEDSET_JSONB_COLS)

BEDSET_COLUMNS = [ID_COL] + chars + jsonbs

# bedset_bedfiles table definition

REL_BED_ID_KEY = "bedfile_id"
REL_BEDSET_ID_KEY = "bedset_id"

__all__ = ["BED_TABLE", "BEDSET_TABLE", "REL_TABLE", "RAW_BEDFILE_KEY",
           "CFG_ENV_VARS", "PG_CLIENT_KEY", "DB_DEFAULT_HOST",
           "SERVER_DEFAULT_PORT", "SERVER_DEFAULT_HOST", "PKG_NAME", "IDX_MAP",
           "BEDFILE_PATH_KEY", "DEFAULT_SECTION_VALUES", "JSON_DICTS_KEY_DESCS",
           "JSON_KEYS", "JSON_FLOAT_KEY_VALUES", "JSON_FLOAT_KEY_NAMES",
           "JSON_INT_KEY_VALUES", "JSON_INT_KEY_NAMES", "JSON_BEDSET_KEY_VALUES",
           "JSON_BEDSET_KEY_NAMES", "JSON_METADATA_NAMES", "BEDSET_COLUMNS",
           "JSON_METADATA_VALUES", "HIDDEN_ATTR_KEYS", "BED_COLUMNS",
           "REL_BED_ID_KEY", "REL_BEDSET_ID_KEY"]\
          + CFG_KEYS + JSON_KEYS
