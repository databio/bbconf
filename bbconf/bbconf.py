import psycopg2
from psycopg2.extras import DictCursor, Json
from psycopg2.extensions import connection

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
    """
    This class provides is an in-memory representation of the configuration
    file for the *BEDBASE* project. Additionally it implements multiple
    convenience methods for interacting with the database backend,
    i.e. [PostgreSQL](https://www.postgresql.org/)
    """
    def __init__(self, filepath):
        """
        Create the config instance with a filepath

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

    def check_connection(self):
        """
        Check whether a PostgreSQL connection has been established

        :return bool: whether the connection has been established
        """
        if hasattr(self, PG_CLIENT_KEY) and \
                isinstance(getattr(self, PG_CLIENT_KEY), connection):
            return True
        return False

    def establish_postgres_connection(self, suppress=False):
        """
        Establish PostgreSQL connection using the config data

        :param bool suppress: whether to suppress any connection errors
        :return bool: whether the connection has been established successfully
        """
        if self.check_connection():
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
            _LOGGER.debug("Established connection with PostgreSQL: {}".
                         format(self[CFG_DATABASE_KEY][CFG_HOST_KEY]))
            return True

    def close_postgres_connection(self):
        """
        Close connection and remove client bound
        """
        if not self.check_connection():
            raise BedBaseConnectionError(
                "The has not been established: {}".
                    format(str(self[CFG_DATABASE_KEY][CFG_HOST_KEY]))
            )
        self[PG_CLIENT_KEY].close()
        del self[PG_CLIENT_KEY]
        _LOGGER.debug("Closed connection with PostgreSQL: {}".
                     format(self[CFG_DATABASE_KEY][CFG_HOST_KEY]))

    @property
    @contextmanager
    def db_cursor(self):
        """
        Establish connection and get a PostgreSQL database cursor,
        commit and close the connection afterwards

        :return DictCursor: Database cursor object
        """
        try:
            if not self.check_connection():
                self.establish_postgres_connection()
            conn = self[PG_CLIENT_KEY]
            with conn as c, c.cursor(cursor_factory=DictCursor) as cur:
                yield cur
        except:
            raise
        finally:
            self.close_postgres_connection()

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
        :param str condition: condition to restrict the results with
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

    def _insert(self, table_name, values, skip_id_return=False):
        """

        :param str table_name: name of the table to insert the values to
        :param dict values: a mapping of pairs of table column names and
            respective values to bne inserted to the database
        :return int: id of the row just inserted
        """
        statement = f"INSERT INTO {table_name} ({','.join(values.keys())})" \
                    f" VALUES ({','.join(['%s'] * len(values))})"
        statement += "RETURNING id;" if not skip_id_return else ";"
        # convert mappings to JSON for postgres
        values = tuple([Json(v) if isinstance(v, Mapping) else v
                        for v in list(values.values())])
        with self.db_cursor as cur:
            _LOGGER.info(f"Inserting into DB:\n - statement: {statement}"
                         f"\n - values: {values}")
            cur.execute(statement, values)
            if not skip_id_return:
                return cur.fetchone()[0]

    def insert_bedfile_data(self, values):
        """

        :param dict values: a mapping of pairs of table column names and
            respective values to bne inserted to the database
        :return int: id of the row just inserted
        """
        return self._insert(table_name=BED_TABLE, values=values)

    def insert_bedset_data(self, values):
        """

        :param dict values: a mapping of pairs of table column names and
            respective values to bne inserted to the database
        :return int: id of the row just inserted
        """
        return self._insert(table_name=BEDSET_TABLE, values=values)

    def insert_bedset_bedfiles_data(self, values):
        """

        :param dict values: a mapping of pairs of table column names and
            respective values to bne inserted to the database
        """
        return self._insert(table_name=REL_TABLE, values=values, skip_id_return=True)

    def _count_rows(self, table_name):
        """
        Count rows in a selected table

        :param str table_name: table to count rows for
        :return int: number of rows in the selected table
        """
        with self.db_cursor as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchall()[0][0]

    def count_bedfiles(self):
        """
        Count rows in the bedfiles table

        :return int: number of rows in the bedfiles table
        """
        return self._count_rows(table_name=BED_TABLE)

    def count_bedsets(self):
        """
        Count rows in the bedsets table

        :return int: number of rows in the bedsets table
        """
        return self._count_rows(table_name=BEDSET_TABLE)

    def _drop_table(self, table_name):
        """
        Remove selected table from the database

        :param str table_name: name of the table to remove
        """
        with self.db_cursor as cur:
            cur.execute(f"DROP table IF EXISTS {table_name};")

    def drop_bedfiles_table(self):
        """
        Remove bedfiles table from the database
        """
        self._drop_table(table_name=BED_TABLE)

    def drop_bedsets_table(self):
        """
        Remove bedsets table from the database
        """
        self._drop_table(table_name=BEDSET_TABLE)

    def drop_bedset_bedfiles_table(self):
        """
        Remove bedsets table from the database
        """
        self._drop_table(table_name=REL_TABLE)

    def _create_table(self, table_name, columns):
        """
        Create a table

        :param str table_name: name of the table to create
        :param str | list[str] columns: columns definition list,
            for instance: ['name VARCHAR(50) NOT NULL']
        """
        columns = _mk_list_of_str(columns)
        with self.db_cursor as cur:
            cur.execute(f"CREATE TABLE {table_name} "
                        f"({', '.join(columns)});")
        _LOGGER.info(f"Created table '{table_name}' with "
                     f"{len(columns) + 1} columns")

    def create_bedfiles_table(self, columns):
        """
        Create a bedfiles table

        :param str | list[str] columns: columns definition list,
            for instance: ['name VARCHAR(50) NOT NULL']
        """
        self._create_table(table_name=BED_TABLE, columns=columns)

    def create_bedsets_table(self, columns):
        """
        Create a bedsets table

        :param str | list[str] columns: columns definition list,
            for instance: ['name VARCHAR(50) NOT NULL']
        """
        self._create_table(table_name=BEDSET_TABLE, columns=columns)

    def create_bedset_bedfiles_table(self):
        """
        Create a bedsets table, id column is defined by default
        """
        columns = [f"PRIMARY KEY ({REL_BEDSET_ID_KEY}, {REL_BED_ID_KEY})",
                   f"{REL_BEDSET_ID_KEY} INT NOT NULL",
                   f"{REL_BED_ID_KEY} INT NOT NULL",
                   f"FOREIGN KEY ({REL_BEDSET_ID_KEY}) REFERENCES {BEDSET_TABLE} (id)",
                   f"FOREIGN KEY ({REL_BED_ID_KEY}) REFERENCES {BED_TABLE} (id)"]
        self._create_table(table_name=REL_TABLE, columns=columns)

    def select_bedfiles_for_bedset(self, query, bedfile_col=None):
        """
        Select bedfiles that are part of a bedset that matches the query

        :param str query: bedsets table query to restrict the results with,
            for instance "name='bedset1'"
        :param list[str] | str bedfile_col: bedfile columns to include in the
            result, if none specified all columns will be included
        :return list[psycopg2.extras.DictRow]: matched bedfiles table contents
        """
        col_str = ", ".join(["f." + c for c in _mk_list_of_str(bedfile_col)]) \
            if bedfile_col else "*"
        with self.db_cursor as cur:
            cur.execute(
                f"SELECT {col_str} FROM {BED_TABLE} f "
                f"INNER JOIN {REL_TABLE} r ON r.bedfile_id = f.id "
                f"INNER JOIN {BEDSET_TABLE} s ON r.bedset_id = s.id "
                f"WHERE s.{query};"
            )
            return cur.fetchall()

    def _check_table_exists(self, table_name):
        """
        Check if the specified table exists

        :param str table_name: table name to be checked
        :return bool: whether the specified table exists
        """
        with self.db_cursor as cur:
            cur.execute(
                "SELECT EXISTS("
                "SELECT * FROM information_schema.tables "
                "WHERE table_name=%s)", (table_name,)
            )
            return cur.fetchone()[0]

    def check_bedfiles_table_exists(self):
        """
        Check if the bedfiles table exists

        :return bool: whether the bedfiles table exists
        """
        return self._check_table_exists(table_name=BED_TABLE)

    def check_bedsets_table_exists(self):
        """
        Check if the bedsets table exists

        :return bool: whether the bedsets table exists
        """
        return self._check_table_exists(table_name=BEDSET_TABLE)

    def check_bedset_bedfiles_table_exists(self):
        """
        Check if the bedset_bedfiles table exists

        :return bool: whether the bedset_bedfiles table exists
        """
        return self._check_table_exists(table_name=REL_TABLE)


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



