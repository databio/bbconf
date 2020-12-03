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
    def __init__(self, config_path, database_only=False):
        """
        Initialize the object

        :param str config_path: path to the bedbase configuration file
        :param bool database_only: whether the database managers should not
            keep an in-memory copy of the data in the database
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
            schema_path=BED_TABLE_SCHEMA,
            database_only=database_only
        )
        self[PIPESTATS_KEY][BEDSET_TABLE] = pipestat.PipestatManager(
            name=BEDSET_TABLE,
            database_config=config_path,
            schema_path=BEDSET_TABLE_SCHEMA,
            database_only=database_only
        )

    def __str__(self):
        """
        Generate string representation of the object

        :return str: string representation of the object
        """
        from textwrap import indent
        res = f"{self.__class__.__name__}\n"
        res += f"{BED_TABLE}:\n"
        res += f"{indent(str(self.bed), '  ')}"
        res += f"\n{BEDSET_TABLE}:\n"
        res += f"{indent(str(self.bedset), '  ')}"
        res += f"\nconfig:\n"
        res += f"{indent(str(self.config), '  ')}"
        return res

    @property
    def config(self):
        """
        Config used to initialize the object

        :return:
        """
        return self[CONFIG_KEY]

    @property
    def bed(self):
        """
        PipestatManager of the bedfiles table

        :return pipestat.PipestatManager: manager of the bedfiles table
        """
        return self[PIPESTATS_KEY][BED_TABLE]

    @property
    def bedset(self):
        """
        PipestatManager of the bedsets table

        :return pipestat.PipestatManager: manager of the bedsets table
        """
        return self[PIPESTATS_KEY][BEDSET_TABLE]

    def _create_bedset_bedfiles_table(self):
        """
        Create a bedsets table, id column is defined by default
        """
        columns = [f"PRIMARY KEY ({REL_BEDSET_ID_KEY}, {REL_BED_ID_KEY})",
                   f"{REL_BEDSET_ID_KEY} INT NOT NULL",
                   f"{REL_BED_ID_KEY} INT NOT NULL",
                   f"FOREIGN KEY ({REL_BEDSET_ID_KEY}) REFERENCES {BEDSET_TABLE} (id)",
                   f"FOREIGN KEY ({REL_BED_ID_KEY}) REFERENCES {BED_TABLE} (id)"]
        self.bed._create_table(table_name=REL_TABLE, columns=columns)

    def report_bedfile_for_bedset(self, bedset_id, bedfile_id):
        """
        Report a bedfile for bedset.

        Inserts the ID pair into the relationship table, which allows to
        manage many to many bedfile bedset relationships

        :param int bedset_id: id of the bedset to report bedfile for
        :param int bedfile_id: id of the bedfile to report
        """
        if not self.bed._check_table_exists(table_name=REL_TABLE):
            self._create_bedset_bedfiles_table()
        with self.bed.db_cursor as cur:
            statement = f"INSERT INTO {REL_TABLE} " \
                        f"({REL_BEDSET_ID_KEY},{REL_BED_ID_KEY}) VALUES (%s,%s)"
            cur.execute(statement, (bedset_id, bedfile_id))
