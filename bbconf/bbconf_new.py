import pipestat
import yacman
from pipestat.const import *
from .exceptions import *
from .const import *

from logging import getLogger

_LOGGER = getLogger(PKG_NAME)


class BedBaseConf(dict):
    """
    This class provides is an in-memory representation of the configuration
    file for the *BEDBASE* project. Additionally it implements multiple
    convenience methods for interacting with the database backend,
    i.e. [PostgreSQL](https://www.postgresql.org/)
    """
    def __init__(self, config_path):
        """
        Create the config instance with a filepath
        """

        def _raise_missing_key(key):
            raise MissingConfigDataError("Config lacks '{}' key".format(key))

        super(BedBaseConf, self).__init__()

        self[CONFIG_KEY] = yacman.YacAttMap(filepath=config_path)
        if CFG_PATH_KEY not in self[CONFIG_KEY]:
            _raise_missing_key(CFG_PATH_KEY)
        if not self[CONFIG_KEY][CFG_PATH_KEY]:
            # if there's nothing under path key (None)
            self[CONFIG_KEY][CFG_PATH_KEY] = {}
        if CFG_PIPELINE_OUT_PTH_KEY not in self[CONFIG_KEY][CFG_PATH_KEY]:
            _raise_missing_key(CFG_PIPELINE_OUT_PTH_KEY)

        if CFG_BEDSTAT_DIR_KEY not in self[CONFIG_KEY][CFG_PATH_KEY]:
            _raise_missing_key(CFG_BEDSTAT_DIR_KEY)

        if CFG_BEDBUNCHER_DIR_KEY not in self[CONFIG_KEY][CFG_PATH_KEY]:
            _raise_missing_key(CFG_BEDBUNCHER_DIR_KEY)

        for section, mapping in DEFAULT_SECTION_VALUES.items():
            if section not in self[CONFIG_KEY]:
                self[CONFIG_KEY][section] = {}
            for key, default in mapping.items():
                if key not in self[CONFIG_KEY][section]:
                    _LOGGER.debug(f"Config lacks '{section}.{key}' key. "
                                  f"Setting to: {default}")
                    self[CONFIG_KEY][section][key] = default

        self[PIPESTATS_KEY] = {}
        self[PIPESTATS_KEY][BED_TABLE] = pipestat.PipestatManager(
            name=BED_TABLE,
            database_config=config_path,
            schema_path=BED_TABLE_SCHEMA
        )
        self[PIPESTATS_KEY][BEDSET_TABLE] = pipestat.PipestatManager(
            name=BEDSET_TABLE,
            database_config=config_path,
            schema_path=BEDSET_TABLE_SCHEMA
        )

    @property
    def bed(self):
        return self[PIPESTATS_KEY][BED_TABLE]

    @property
    def bedset(self):
        return self[PIPESTATS_KEY][BEDSET_TABLE]

    def create_bedset_bedfiles_table(self):
        """
        Create a bedsets table, id column is defined by default
        """
        columns = [f"PRIMARY KEY ({REL_BEDSET_ID_KEY}, {REL_BED_ID_KEY})",
                   f"{REL_BEDSET_ID_KEY} INT NOT NULL",
                   f"{REL_BED_ID_KEY} INT NOT NULL",
                   f"FOREIGN KEY ({REL_BEDSET_ID_KEY}) REFERENCES {BEDSET_TABLE} (id)",
                   f"FOREIGN KEY ({REL_BED_ID_KEY}) REFERENCES {BED_TABLE} (id)"]
        self[PIPESTATS_KEY][BED_TABLE]._create_table(
            table_name=REL_TABLE, columns=columns)

    def report_bedfile_for_bedset(self, bedset_id, bedfile_id,
                                  skip_id_return=False):
        """

        :param bedset_id:
        :param bedfile_id:
        :param skip_id_return:
        :return int:
        """
        with self.bed.db_cursor as cur:
            statement = f"INSERT INTO {REL_TABLE} " \
                        f"({REL_BEDSET_ID_KEY},{REL_BED_ID_KEY}) VALUES (%s,%s)"
            if not skip_id_return:
                statement += " RETURNING id"
            cur.execute(statement, (bedset_id, bedfile_id))
            if not skip_id_return:
                return cur.fetchone()[0]

