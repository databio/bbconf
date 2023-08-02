import os
from logging import getLogger
from typing import List, Optional, Tuple, Union
from textwrap import indent

import yacman

import pipestat
from pipestat.helpers import dynamic_filter

from sqlmodel import SQLModel, Field

from sqlalchemy.orm import relationship
from sqlalchemy import inspect

from .const import *
from .exceptions import *
from bbconf.helpers import raise_missing_key, get_bedbase_cfg

_LOGGER = getLogger(PKG_NAME)


class BedBaseConf:
    """
    This class standardizes reporting of bedstat and bedbuncher results.
    It formalizes a way for these pipelines and downstream tools
    to communicate -- the produced results can easily and reliably become an
    input for the server. The object exposes API for interacting with the
    results and is backed by a [PostgreSQL](https://www.postgresql.org/)
    database.
    """

    def __init__(self, config_path: str = None, database_only: bool = False):
        """
        Initialize the object

        :param str config_path: path to the bedbase configuration file
        :param bool database_only: whether the database managers should not
            keep an in-memory copy of the data in the database
        """

        cfg_path = get_bedbase_cfg(config_path)
        self._config = yacman.YAMLConfigManager(filepath=cfg_path)

        if CFG_PATH_KEY not in self._config:
            raise_missing_key(CFG_PATH_KEY)

        if not self._config[CFG_PATH_KEY]:
            self._config[CFG_PATH_KEY] = {}

        if CFG_PIPELINE_OUT_PTH_KEY not in self._config[CFG_PATH_KEY]:
            raise_missing_key(CFG_PIPELINE_OUT_PTH_KEY)

        if CFG_BEDSTAT_DIR_KEY not in self._config[CFG_PATH_KEY]:
            raise_missing_key(CFG_BEDSTAT_DIR_KEY)

        if CFG_BEDBUNCHER_DIR_KEY not in self._config[CFG_PATH_KEY]:
            raise_missing_key(CFG_BEDBUNCHER_DIR_KEY)

        for section, mapping in DEFAULT_SECTION_VALUES.items():
            if section not in self._config:
                self._config[section] = {}
            for key, default in mapping.items():
                if key not in self._config[section]:
                    _LOGGER.debug(
                        f"Config lacks '{section}.{key}' key. Setting to: {default}"
                    )
                    self._config[section][key] = default

        # Create Pipestat objects and tables if they do not exist
        self.__pipestats = {
            BED_TABLE: pipestat.PipestatManager(
                config_file=cfg_path,
                schema_path=BED_TABLE_SCHEMA,
                database_only=database_only,
            ),
            BEDSET_TABLE: pipestat.PipestatManager(
                config_file=cfg_path,
                schema_path=BEDSET_TABLE_SCHEMA,
                database_only=database_only,
            ),
            # DIST_TABLE: pipestat.PipestatManager(
            #     config_file=cfg_path,
            #     schema_path=DIST_TABLE_SCHEMA,
            #     database_only=database_only,
            # ),
        }

        self._create_bedset_bedfiles_table()

    def __str__(self):
        """
        Generate string representation of the object

        :return str: string representation of the object
        """

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

        :return yacman.YAMLConfigManager: bedbase configuration file contents
        """
        return self._config

    @property
    def bed(self):
        """
        PipestatManager of the bedfiles table

        :return pipestat.PipestatManager: manager of the bedfiles table
        """
        return self.__pipestats[BED_TABLE]

    @property
    def dist(self):
        """
        PipestatManager of the distances table

        :return pipestat.PipestatManager: manager of the bedfiles table
        """
        return self.__pipestats[DIST_TABLE]

    @property
    def bedset(self):
        """
        PipestatManager of the bedsets table

        :return pipestat.PipestatManager: manager of the bedsets table
        """
        return self.__pipestats[BEDSET_TABLE]

    def _check_table_exists(self, table_name: str) -> bool:
        """
        Check if the specified table exists on the 'bed' pipestatmanager object

        :param str table_name: table name to be checked
        :return bool: whether the specified table exists
        """
        with self.bed.backend.session as s:
            return inspect(s.bind).has_table(table_name=table_name)

    def _get_output_path(self, table_name: str, remote_key: str, remote=False) -> str:
        """
        Get path to the output of the selected pipeline

        :param str table_name: name of the table that is populated by the
            pipeline to return the output path for
        :param str remote_key:
        :param bool remote: whether to use remote url base
        :return str: path to the selected pipeline output
        """
        dir_key = (
            CFG_BEDBUNCHER_DIR_KEY
            if table_name == BEDSET_TABLE
            else CFG_BEDSTAT_DIR_KEY
        )
        base = (
            self.config[CFG_REMOTE_KEY][remote_key]["prefix"]
            if remote
            else self.config[CFG_PATH_KEY][CFG_PIPELINE_OUT_PTH_KEY]
        )
        if remote and not base:
            raise MissingConfigDataError(
                f"{CFG_REMOTE_KEY} key value is invalid: {base}"
            )
        return os.path.join(base, self.config[CFG_PATH_KEY][dir_key])

    def get_bedbuncher_output_path(self, remote_key, remote=False):
        """
        Get path to the output of the bedbuncher pipeline

        :param bool remote: whether to use remote url base
        :return str: path to the bedbuncher pipeline output
        """
        return self._get_output_path(
            table_name=BEDSET_TABLE, remote_key=remote_key, remote=remote
        )

    def get_bedstat_output_path(self, remote_key, remote=False):
        """
        Get path to the output of the bedstat pipeline

        :param bool remote: whether to use remote url base
        :return str: path to the bedstat pipeline output
        """
        return self._get_output_path(
            table_name=BED_TABLE, remote_key=remote_key, remote=remote
        )

    def _create_bedset_bedfiles_table(self):
        """
        Create a relationship table
        """

        class BedFileBedSetAssociation(SQLModel, table=True):
            __tablename__ = BEDFILE_BEDSET_ASSOCIATION_TABLE_KEY
            bedfile_id: Optional[int] = Field(
                default=None,
                foreign_key=f"{self.bed.pipeline_name}__sample.id",
                primary_key=True,
            )
            bedset_id: Optional[int] = Field(
                default=None,
                foreign_key=f"{self.bedset.pipeline_name}__sample.id",
                primary_key=True,
            )

            __table_args__ = {"extend_existing": True}

        returned_model = BedFileBedSetAssociation.__table__

        self.BedfileORM.__mapper__.add_property(
            BEDSETS_REL_KEY,
            relationship(
                self.BedsetORM,
                secondary=returned_model,
                backref=BEDFILES_REL_KEY,
            ),
        )

        SQLModel.metadata.create_all(bind=self.bed.backend.db_engine_key)

    def report_relationship(self, bedset_id, bedfile_id):
        """
        Report a bedfile for bedset.

        Inserts the ID pair into the relationship table, which allows to
        manage many to many bedfile bedset relationships

        :param int bedset_id: id of the bedset to report bedfile for
        :param int bedfile_id: id of the bedfile to report
        """

        if not self._check_table_exists(
            table_name=BEDFILE_BEDSET_ASSOCIATION_TABLE_KEY
        ):
            self._create_bedset_bedfiles_table()

        with self.bed.backend.session as s:
            bed = s.query(self.BedfileORM).get(bedfile_id)
            bedset = s.query(self.BedsetORM).get(bedset_id)
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

        if not self._check_table_exists(
            table_name=BEDFILE_BEDSET_ASSOCIATION_TABLE_KEY
        ):
            raise BedBaseConfError(
                f"Can't remove a relationship, '{BEDFILE_BEDSET_ASSOCIATION_TABLE_KEY}' does not exist"
            )
        with self.bedset.backend.session as s:
            bedset = s.query(self.BedsetORM).get(bedset_id)
            if bedfile_ids is None:
                getattr(bedset, BEDFILES_REL_KEY)[:] = []
            else:
                for bedfile_id in bedfile_ids:
                    bedfile = s.query(self.BedfileORM).get(bedfile_id)
                    getattr(bedset, BEDFILES_REL_KEY).remove(bedfile)
            s.commit()

    def select_bedfiles_for_bedset(
        self,
        filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = [],
        json_filter_conditions: Optional[List[Tuple[str, str, str]]] = [],
        bedfile_cols: Optional[List[str]] = None,
    ) -> list:
        """
        Select bedfiles that are part of a bedset that matches the query

        :param List[str] filter_conditions:  table query to restrict the results with
        :param Union[List[str], str] bedfile_cols: bedfile columns to include in the
            result, if none specified all columns will be included
        :return: matched bedfiles table contents
        """
        cols = (
            [getattr(self.BedfileORM, bedfile_col) for bedfile_col in bedfile_cols]
            if bedfile_cols is not None
            else self.BedfileORM.__table__.columns
        )
        with self.bed.backend.session as session:
            statement = session.query(*cols).join(
                self.BedfileORM, self.BedsetORM.bedfiles
            )
            statement = dynamic_filter(
                ORM=self.BedsetORM,
                statement=statement,
                filter_conditions=filter_conditions,
                json_filter_conditions=json_filter_conditions,
            )
            bed_names = statement.all()

        return bed_names

    def select_unique(self, table_name, column=None):
        """
        Select unique value in given column and table

        :param str table_name: table to query in
        :param str col: column to include in the result
        :return list[psycopg2.extras.DictRow]: unique entries in the column
        """

        if table_name == "bedfile__sample":
            with self.bed.backend.session as s:
                values = self.bed.backend.select(columns=column)
        elif table_name == "bedsets__sample":
            with self.bedset.backend.session as s:
                values = self.bedset.backend.select(columns=column)
        return [i for n, i in enumerate(values) if i not in values[n + 1 :]]

    @property
    def BedfileORM(self) -> SQLModel:
        """
        return: ORM of bedfile table (SQLModelMetaclass)
        """
        return self.bed.backend.get_orm("bedfile__sample")

    @property
    def BedsetORM(self) -> SQLModel:
        """
        return: ORM of bedset table (SQLModelMetaclass)
        """
        return self.bedset.backend.get_orm("bedsets__sample")

    # def select_bedfiles_for_distance(
    #     self,
    #     terms,
    #     genome,
    #     bedfile_cols: Optional[List[str]] = None,
    #     limit: Optional[int] = None,
    # ):
    #     """
    #     Select bedfiles that are related to given search terms
    #
    #     :param List[str] terms:  search terms
    #     :param str genome: genome assembly to search in
    #     :param Union[List[str], str] bedfile_cols: bedfile columns to include in the
    #         result, if none specified all columns will be included
    #     :param int limit: max number of records to return
    #     :return List[sqlalchemy.engine.row.Row]: matched bedfiles table contents
    #     """
    #     num_terms = len(terms)
    #     if num_terms > 1:
    #         for i in range(num_terms):
    #             if i == 0:
    #                 avg = f"coalesce(R{str(i)}.score, 0.5)"
    #                 join = f"FROM distances R{str(i)}"
    #                 where = f"WHERE R{str(i)}.search_term ILIKE '{terms[i]}'"
    #             else:
    #                 avg += f" + coalesce(R{str(i)}.score, 0.5)"
    #                 join += (
    #                     " INNER JOIN distances R"
    #                     + str(i)
    #                     + " ON R"
    #                     + str(i - 1)
    #                     + ".bed_id = R"
    #                     + str(i)
    #                     + ".bed_id"
    #                 )
    #                 where += f" OR R{str(i)}.search_term ILIKE '{terms[i]}'"
    #
    #             condition = (
    #                 f"SELECT R0.bed_id AS bed_id, AVG({avg}) AS score "
    #                 f"{join} {where} GROUP BY R0.bed_id ORDER BY score ASC"
    #             )
    #             if limit:
    #                 condition += f" LIMIT {limit}"
    #
    #     else:
    #         condition = (
    #             f"SELECT bed_id, score FROM {DIST_TABLE} "
    #             f"WHERE search_term ILIKE '{terms[0]}' ORDER BY score ASC"
    #         )
    #         if limit:
    #             condition += f" LIMIT {limit}"
    #
    #     columns = [
    #         "f." + c
    #         for c in pipestat.helpers.mk_list_of_str(
    #             bedfile_cols or list(self.bed.schema.keys())
    #         )
    #     ]
    #     columns = ", ".join([c for c in columns])
    #     statement_str = (
    #         "SELECT {}, score FROM {} f INNER JOIN ({}) r ON r.bed_id = f.id "
    #         "WHERE f.genome ->> 'alias' = '" + genome + "' ORDER BY score ASC"
    #     )
    #     with self.bed.backend.session as s:
    #         res = s.execute(
    #             text(statement_str.format(columns, BED_TABLE, condition)),
    #         )
    #     res = res.mappings().all()
    #     _LOGGER.info(f"here: {res}")
    #
    #     return res
    # def report_distance(
    #     self,
    #     bed_md5sum,
    #     bed_label,
    #     search_term,
    #     score,
    # ):
    #     """
    #     Report a search term - bedfile distance.
    #
    #     Inserts a distance of the bedfile to a search term
    #
    #     :param str bed_md5sum: bedfile MD5SUM
    #     :param str bed_label: bedfile label
    #     :param str search_term: search term
    #     :param float score: associated score
    #     :rasie ValueError: if none of the BED files match the provided md5sum
    #     """
    #     # TODO: This method should be removed and the next few lines added in the clients
    #     BedORM = self.bed.get_orm(table_name=self.bed.namespace)
    #     with self.bed.session as s:
    #         bed = s.query(BedORM.id).filter(BedORM.md5sum == bed_md5sum).first()
    #     if bed is None:
    #         raise ValueError(
    #             f"None of the files in the '{self.bed.namespace}' table "
    #             f"match the md5sum: {bed_md5sum}"
    #         )
    #     values = dict(
    #         bed_id=bed.id, bed_label=bed_label, search_term=search_term, score=score
    #     )
    #     self.dist.report(values=values, record_identifier=f"{bed_md5sum}_{search_term}")
