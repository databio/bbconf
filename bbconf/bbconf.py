import os
from logging import getLogger
from typing import Dict, List, Optional, Tuple, Union

import pipestat
import yacman
from pipestat.const import *
from pipestat.helpers import dynamic_filter, mk_list_of_str
from sqlalchemy import Column, ForeignKey, Integer, Table
from sqlalchemy.engine.row import Row
from sqlalchemy.orm import declarative_base, relationship

from .const import *
from .exceptions import *
from .helpers import *

_LOGGER = getLogger(PKG_NAME)


class BedBaseConf(dict):
    """
    This class standardizes reporting of bedstat and bedbuncher results.
    It formalizes a way for these pipelines and downstream tools
    to communicate -- the produced results can easily and reliably become an
    input for the server. The object exposes API for interacting with the
    results and is backed by a [PostgreSQL](https://www.postgresql.org/)
    database.
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
                    _LOGGER.debug(
                        f"Config lacks '{section}.{key}' key. " f"Setting to: {default}"
                    )
                    self[CONFIG_KEY][section][key] = default
        self[COMMON_DECL_BASE_KEY] = declarative_base()
        self[PIPESTATS_KEY] = {}
        self[PIPESTATS_KEY][BED_TABLE] = pipestat.PipestatManager(
            namespace=BED_TABLE,
            config=self.config,
            schema_path=BED_TABLE_SCHEMA,
            database_only=database_only,
            custom_declarative_base=self[COMMON_DECL_BASE_KEY],
        )
        self[PIPESTATS_KEY][BEDSET_TABLE] = pipestat.PipestatManager(
            namespace=BEDSET_TABLE,
            config=self.config,
            schema_path=BEDSET_TABLE_SCHEMA,
            database_only=database_only,
            custom_declarative_base=self[COMMON_DECL_BASE_KEY],
        )
        self._create_bedset_bedfiles_table()

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
        dir_key = (
            CFG_BEDBUNCHER_DIR_KEY
            if table_name == BEDSET_TABLE
            else CFG_BEDSTAT_DIR_KEY
        )
        base = (
            self.config[CFG_PATH_KEY][CFG_REMOTE_URL_BASE_KEY]
            if remote
            else self.config[CFG_PATH_KEY][CFG_PIPELINE_OUT_PTH_KEY]
        )
        if remote and not base:
            raise MissingConfigDataError(
                f"{CFG_REMOTE_URL_BASE_KEY} key value is invalid: {base}"
            )
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
        A relationship table
        """
        rel_table = Table(
            REL_TABLE,
            self[COMMON_DECL_BASE_KEY].metadata,
            Column(REL_BED_ID_KEY, Integer, ForeignKey(f"{self.bed.namespace}.id")),
            Column(
                REL_BEDSET_ID_KEY, Integer, ForeignKey(f"{self.bedset.namespace}.id")
            ),
        )

        BedORM = self.bed._get_orm(table_name=self.bed.namespace)
        BedsetORM = self.bedset._get_orm(table_name=self.bedset.namespace)

        BedORM.__mapper__.add_property(
            BEDSETS_REL_KEY,
            relationship(
                BedsetORM,
                secondary=rel_table,
                backref=BEDFILES_REL_KEY,
            ),
        )
        self[COMMON_DECL_BASE_KEY].metadata.create_all(bind=self.bed["_db_engine"])

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
        BedORM = self.bed._get_orm(self.bed.namespace)
        BedsetORM = self.bedset._get_orm(self.bedset.namespace)
        with self.bed.session as s:
            bed = s.query(BedORM).get(bedfile_id)
            bedset = s.query(BedsetORM).get(bedset_id)
            getattr(bedset, BEDFILES_REL_KEY).append(bed)
            s.commit()

    def remove_relationship(self, bedset_id, bedfile_ids=None):
        """
        Remove entries from the relationships table

        :param str bedset_id: id of the bedset to remove
        :param list[str] bedfile_ids: ids of the bedfiles to remove for the
            selected bedset. If none provided, all the relationsips for the
            selected bedset will be removed.
        """

        if not self.bed._check_table_exists(table_name=REL_TABLE):
            raise BedBaseConfError(
                f"Can't remove a relationship, '{REL_TABLE}' does not exist"
            )
        BedORM = self.bed._get_orm(self.bed.namespace)
        BedsetORM = self.bedset._get_orm(self.bedset.namespace)
        with self.bedset.session as s:
            bedset = s.query(BedsetORM).get(bedset_id)
            if bedfile_ids is None:
                getattr(bedset, BEDFILES_REL_KEY)[:] = []
            else:
                for bedfile_id in bedfile_ids:
                    bedfile = s.query(BedORM).get(bedfile_id)
                    getattr(bedset, BEDFILES_REL_KEY).remove(bedfile)
            s.commit()

    def select_bedfiles_for_bedset(
        self,
        filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = [],
        bedfile_cols: Optional[List[str]] = None,
    ) -> List[Row]:
        """
        Select bedfiles that are part of a bedset that matches the query

        :param List[str] filter_conditions:  table query to restrict the results with
        :param Union[List[str], str] bedfile_cols: bedfile columns to include in the
            result, if none specified all columns will be included
        :return List[sqlalchemy.engine.row.Row]: matched bedfiles table contents
        """
        BedORM = self.bed._get_orm(BED_TABLE)
        BedsetORM = self.bedset._get_orm(BEDSET_TABLE)
        cols = (
            [getattr(BedORM, bedfile_col) for bedfile_col in bedfile_cols]
            if bedfile_cols is not None
            else BedORM.__table__.columns
        )
        with self.bed.session as s:
            q = s.query(*cols).join(BedORM, BedsetORM.bedfiles)
            q = dynamic_filter(
                ORM=BedsetORM, query=q, filter_conditions=filter_conditions
            )
            bed_names = q.all()
        return bed_names
