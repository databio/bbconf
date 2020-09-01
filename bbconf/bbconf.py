import psycopg2
from psycopg2.extras import DictCursor, Json

from logging import getLogger
from contextlib import contextmanager
from collections import Mapping

import yacman

from attmap import PathExAttMap as PXAM

from .const import *
from .exceptions import *

_LOGGER = getLogger(PKG_NAME)

__all__ = ["BedBaseConf", "get_bedbase_cfg"]


class BedBaseConf(yacman.YacAttMap):
    def __init__(self, filepath):
        """
        Create the config instance by with a filepath

        :param str filepath: a path to the YAML file to read
        """

        def _raise_missing_key(key):
            raise MissingConfigDataError("Config lacks '{}' key".format(key))

        super(BedBaseConf, self).__init__(filepath=filepath)

        if CFG_PATH_KEY not in self:
            _raise_missing_key(CFG_PATH_KEY)
        if not self[CFG_PATH_KEY]:
            # if there's nothing under path key (None)
            self[CFG_PATH_KEY] = PXAM()

        if CFG_BEDSTAT_OUTPUT_KEY not in self[CFG_PATH_KEY]:
            _raise_missing_key(CFG_BEDSTAT_OUTPUT_KEY)
            
        if CFG_BEDBUNCHER_OUTPUT_KEY not in self[CFG_PATH_KEY]:
            _raise_missing_key(CFG_BEDBUNCHER_OUTPUT_KEY)          
        
        for section, mapping in DEFAULT_SECTION_VALUES.items():
            if section not in self:
                self[section] = PXAM()
            for key, default in mapping.items():
                if key not in self[section]:
                    _LOGGER.debug("Config lacks '{}.{}' key. Setting to: {}".
                                  format(section, key, default))
                    self[section][key] = default

    def _excl_from_repr(self, k, cls):
        return k in \
               [attr for attr in self.to_dict().keys() if attr.startswith("_")]

    def establish_postgres_connection(self, suppress=False):
        """
        Establish PostgreSQL connection using the config data

        :param str host: database host
        :param bool suppress: whether to suppress any connection errors
        :return bool: whether the connection has been established succesfully
        """
        if hasattr(self, PG_CLIENT_KEY):
            raise BedBaseConnectionError(
                "The connection is already established: {}".
                    format(str(self[PG_CLIENT_KEY].info.host))
            )
        db = self[CFG_DATABASE_KEY]
        try:
            self[PG_CLIENT_KEY] = psycopg2.connect(
                dbname=db[CFG_NAME_KEY],
                user=db[CFG_USER_KEY],
                password=db[CFG_PASSWORD_KEY],
                host=self[CFG_DATABASE_KEY][CFG_HOST_KEY],
                port=db[CFG_PORT_KEY]
            )
        except psycopg2.Error as e:
            _LOGGER.error("Could not connect to: {}".
                          format(self[CFG_DATABASE_KEY][CFG_HOST_KEY]))
            _LOGGER.info("Caught error: {}".format(e))
            if suppress:
                return False
            raise
        else:
            _LOGGER.info("Established connection with PostgreSQL: {}".
                         format(self[CFG_DATABASE_KEY][CFG_HOST_KEY]))
            return True

    def close_postgres_connection(self):
        """
        Close connection and remove client bound
        """
        self.assert_connection()
        self[PG_CLIENT_KEY].close()
        del self[PG_CLIENT_KEY]
        _LOGGER.info("Closed connection with PostgreSQL: {}".
                     format(self[CFG_DATABASE_KEY][CFG_HOST_KEY]))

    def assert_connection(self):
        """
        Check whether an Elasticsearch connection has been established

        :raise BedBaseConnectionError: if there is no active connection
        """
        if not hasattr(self, PG_CLIENT_KEY):
            raise BedBaseConnectionError(
                "No active connection with PostgreSQL"
            )

    def _select_all(self, table_name):
        """
        Get all the contents from the selected table.
        Convenience method for table exploration

        :param str table_name: name of the table to list contents for
        :return list[psycopg2.extras.DictRow]: all table contents
        """
        with self.db_cursor as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            result = cur.fetchall()
        return result

    def select(self, table_name, columns=None, condition=None):
        """
        Get all the contents from the selected table

        :param str table_name: name of the table to list contents for
        :param str | list[str] columns: columns to select
        :param str condition: to restrict the results with
        :return list[psycopg2.extras.DictRow]: all table contents
        """
        if condition and not isinstance(condition, str):
            raise TypeError("Condition has to be a string, e.g. 'id=1'")
        columns = _mk_list_of_str(columns)
        columns = ",".join(columns) if columns else "*"
        statement = f"SELECT {columns} FROM {table_name}"
        if condition:
            statement += f" WHERE {condition}"
        statement += ";"
        with self.db_cursor as cur:
            _LOGGER.info(f"Selecting from DB:\n - statement: {statement}")
            cur.execute(statement)
            result = cur.fetchall()
        return result

    def _insert(self, table_name, values):
        """

        :param str table_name: name of the table to insert the values to
        :param dict values: a mapping of pairs of table column names and
            respective values to bne inserted to the database
        """
        statement = f"INSERT INTO {table_name} ({','.join(values.keys())})" \
                    f" VALUES ({','.join(['%s'] * len(values))});"
        # convert mappings to JSON for postgres
        values = tuple([Json(v) if isinstance(v, Mapping) else v
                        for v in list(values.values())])
        with self.db_cursor as cur:
            _LOGGER.info(f"Inserting into DB:\n - statement: {statement}"
                         f"\n - values: {values}")
            cur.execute(statement, values)

    def insert_bedfile_data(self, values):
        """

        :param dict values: a mapping of pairs of table column names and
            respective values to bne inserted to the database
        """
        self._insert(table_name=BED_TABLE, values=values)

    def insert_bedset_data(self, values):
        """

        :param dict values: a mapping of pairs of table column names and
            respective values to bne inserted to the database
        """
        self._insert(table_name=BEDSET_TABLE, values=values)

    @property
    @contextmanager
    def db_cursor(self):
        """
        Establish connection and Get a PostgreSQL database cursor,
        commit and close the connection afterwards

        :return DictCursor: Database cursor object
        """
        try:
            self.establish_postgres_connection()
            connection = self[PG_CLIENT_KEY]
            with connection as conn, conn.cursor(cursor_factory=DictCursor) as cur:
                yield cur
        except:
            raise
        finally:
            self.close_postgres_connection()

    def _count_rows(self, table_name):
        """
        Count rows in a selected table

        :param str table_name: table to count rows for
        :return int: numner of rows in the selected table
        """
        with self.db_cursor as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchall()[0][0]

    def count_bedfiles(self):
        """
        Count rows in the bedfiles table

        :return int: numner of rows in the bedfiles table
        """
        return self._count_rows(table_name=BED_TABLE)

    def count_bedsets(self):
        """
        Count rows in the bedsets table

        :return int: numner of rows in the bedsets table
        """
        return self._count_rows(table_name=BEDSET_TABLE)


    # def _delete_index(self, index):
    #     """
    #     Delete selected index from Elasticsearch
    #
    #     :param str index: name of the index to delete
    #     """
    #     self.assert_connection()
    #     self[ES_CLIENT_KEY].indices.delete(index=index)
    #
    # def delete_bedfiles_index(self):
    #     """
    #     Delete bedfiles index from Elasticsearch
    #     """
    #     self._delete_index(index=BED_INDEX)
    #
    # def delete_bedsets_index(self):
    #     """
    #     Delete bedsets index from Elasticsearch
    #     """
    #     self._delete_index(index=BEDSET_INDEX)


def _mk_list_of_str(x):
    """
    Make sure the input is a list of strings

    :param str | list[str] | falsy x: input to covert
    :return list[str]: converted input
    :raise TypeError: if the argument cannot be converted
    """
    if not x or isinstance(x, list):
        return x
    if isinstance(x, str):
        return [x]
    raise TypeError(f"String or list of strings required as input. Got: "
                    f"{x.__class__.__name__}")


def get_bedbase_cfg(cfg=None):
    """
    Determine path to the bedbase configuration file

    The path can be either explicitly provided
    or read from a $BEDBASE environment variable

    :param str cfg: path to the config file.
        Optional, the $BEDBASE config env var will be used if not provided
    :return str: configuration file path
    """
    selected_cfg = yacman.select_config(
        config_filepath=cfg,
        config_env_vars=CFG_ENV_VARS
    )
    if not selected_cfg:
        raise BedBaseConnectionError(
            "You must provide a config file or set the {} environment variable"
                .format("or ".join(CFG_ENV_VARS))
        )
    return selected_cfg



