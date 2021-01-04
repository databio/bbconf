import os
from logging import getLogger
from psycopg2 import sql

import yacman
import pipestat
from pipestat.const import *
from pipestat.helpers import mk_list_of_str

from .exceptions import *
from .const import *
from .helpers import *

_LOGGER = getLogger(PKG_NAME)


class BedBaseConf(dict):
    """
    This class provides is an in-memory representation of the configuration
    file for the *BEDBASE* project. Additionally it implements multiple
    convenience methods for interacting with the database backend,
    i.e. [PostgreSQL](https://www.postgresql.org/)
    """
    def __init__(self, config_path=None, database_only=False):
        """
        Initialize the object

        :param str config_path: path to the bedbase configuration file
        :param bool database_only: whether the database managers should not
            keep an in-memory copy of the data in the database
        """
        def _raise_missing_key(key):
            raise MissingConfigDataError("Config lacks '{}' key".format(key))

        super(BedBaseConf, self).__init__()

        cfg_path = get_bedbase_cfg(config_path)
        self[CONFIG_KEY] = yacman.YacAttMap(filepath=cfg_path)
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
            config=self.config,
            schema_path=BED_TABLE_SCHEMA,
            database_only=database_only
        )
        self[PIPESTATS_KEY][BEDSET_TABLE] = pipestat.PipestatManager(
            name=BEDSET_TABLE,
            config=self.config,
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

        :return yacman.YacAttMap: bedbase configuration file contents
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

    def _get_output_path(self, table_name, remote=False):
        """
        Get path to the output of the selected pipeline

        :param bool remote: whether to use remote url base
        :param str table_name: name of the table that is populated by the
            pipeline to return the output path for
        :return str: path to the selected pipeline output
        """
        dir_key = CFG_BEDBUNCHER_DIR_KEY if table_name == BEDSET_TABLE \
            else CFG_BEDSTAT_DIR_KEY
        base = self.config[CFG_PATH_KEY][CFG_REMOTE_URL_BASE_KEY] if remote \
            else self.config[CFG_PATH_KEY][CFG_PIPELINE_OUT_PTH_KEY]
        if remote and not base:
            raise MissingConfigDataError(
                f"{CFG_REMOTE_URL_BASE_KEY} key value is invalid: {base}")
        return os.path.join(base, self.config[CFG_PATH_KEY][dir_key])

    def get_bedbuncher_output_path(self, remote=False):
        """
        Get path to the output of the bedbuncher pipeline

        :param bool remote: whether to use remote url base
        :return str: path to the bedbuncher pipeline output
        """
        return self._get_output_path(table_name=BEDSET_TABLE, remote=remote)

    def get_bedstat_output_path(self, remote=False):
        """
        Get path to the output of the bedstat pipeline

        :param bool remote: whether to use remote url base
        :return str: path to the bedstat pipeline output
        """
        return self._get_output_path(table_name=BED_TABLE, remote=remote)

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

    def report_relationship(self, bedset_id, bedfile_id):
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

    def remove_relationship(self, bedset_id, bedfile_ids=None):
        """
        Remove entries from the relationships table

        :param str bedset_id: id of the bedset to remove
        :param list[str] bedfile_ids: ids of the bedfiles to remove for the
            selected bedset. If none provided, all the relationsips for the
            selected bedset will be removed.
        """
        if not self.bed._check_table_exists(table_name=REL_TABLE):
            raise BedBaseConfError(f"'{REL_TABLE}' not found")
        bedfile_ids = mk_list_of_str(bedfile_ids)
        if bedfile_ids is None:
            res = self.select_bedfiles_for_bedset(
                bedfile_col="id", condition="id=%s", condition_val=[bedset_id])
            bedfile_ids = [i[0] for i in res]
        with self.bed.db_cursor as cur:
            for bedfile_id in bedfile_ids:
                statment = f"DELETE FROM {REL_TABLE} " \
                           f"WHERE {REL_BEDSET_ID_KEY} = %s and " \
                           f"{REL_BED_ID_KEY} = %s"
                cur.execute(statment, (bedset_id, bedfile_id))

    def select_bedfiles_for_bedset(self, condition=None, condition_val=None,
                                   bedfile_col=None):
        """
        Select bedfiles that are part of a bedset that matches the query

        :param str condition: bedsets table query to restrict the results with,
            for instance `"id=%s"`
        :param list condition_val:
        :param list[str] | str bedfile_col: bedfile columns to include in the
            result, if none specified all columns will be included
        :return list[psycopg2.extras.DictRow]: matched bedfiles table contents
        """
        condition, condition_val = \
            pipestat.helpers.preprocess_condition_pair(condition, condition_val)
        columns = ["f." + c for c in pipestat.helpers.mk_list_of_str(
            bedfile_col or list(self.bed.schema.keys()))]
        columns = sql.SQL(',').join([sql.SQL(v) for v in columns])
        statement_str = \
            "SELECT {} FROM {} f INNER JOIN {} r ON r.bedfile_id = f.id INNER" \
            " JOIN {} s ON r.bedset_id = s.id WHERE s."
        with self.bed.db_cursor as cur:
            statement = sql.SQL(statement_str).format(
                columns, sql.Identifier(BED_TABLE),
                sql.Identifier(REL_TABLE), sql.Identifier(BEDSET_TABLE))
            statement += condition
            cur.execute(statement, condition_val)
            return cur.fetchall()
