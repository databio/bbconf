import os
from logging import getLogger
from typing import List, Optional, Tuple, Union
from textwrap import indent

import yacman

import pipestat
from pipestat.helpers import dynamic_filter

from sqlmodel import SQLModel, Field
from qdrant_client import QdrantClient

from sqlalchemy.orm import relationship
from sqlalchemy import inspect

from bbconf.const import (
    CFG_PATH_KEY,
    PKG_NAME,
    CFG_PATH_PIPELINE_OUTPUT_KEY,
    CFG_PATH_BEDSTAT_DIR_KEY,
    DEFAULT_SECTION_VALUES,
    CFG_PATH_BEDBUNCHER_DIR_KEY,
    BED_TABLE,
    BED_TABLE_SCHEMA,
    BEDSET_TABLE,
    BEDSET_TABLE_SCHEMA,
    DIST_TABLE,
    BEDFILE_BEDSET_ASSOCIATION_TABLE_KEY,
    CFG_REMOTE_KEY,
    BEDSETS_REL_KEY,
    BEDFILES_REL_KEY,
    CFG_PATH_REGION2VEC_KEY,
    CFG_PATH_VEC2VEC_KEY,
    CFG_QDRANT_KEY,
    CFG_QDRANT_PORT_KEY,
    CFG_QDRANT_API_KEY,
    CFG_QDRANT_HOST_KEY,
    CFG_QDRANT_COLLECTION_NAME_KEY,
    DEFAULT_HF_MODEL,
)
from bbconf.exceptions import MissingConfigDataError, BedBaseConfError
from bbconf.helpers import raise_missing_key, get_bedbase_cfg
# from bbconf.t2bsi import Text2BEDSearchInterface

from geniml.text2bednn import text2bednn
from sentence_transformers import SentenceTransformer


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

        self._config = self._read_config_file(cfg_path)

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
        }

        self._create_bedset_bedfiles_table()

        self._t2bsi = self._create_t2bsi_object()

    def _read_config_file(self, config_path: str) -> yacman.YAMLConfigManager:
        """
        Read configuration file and insert default values if not set

        :param config_path: configuration file path
        :return: None
        :raises: raise_missing_key (if config key is missing)
        """
        _config = yacman.YAMLConfigManager(filepath=config_path)

        if CFG_PATH_KEY not in _config:
            raise_missing_key(CFG_PATH_KEY)

        if not _config[CFG_PATH_KEY]:
            _config[CFG_PATH_KEY] = {}

        if CFG_PATH_PIPELINE_OUTPUT_KEY not in _config[CFG_PATH_KEY]:
            raise_missing_key(CFG_PATH_PIPELINE_OUTPUT_KEY)

        if CFG_PATH_BEDSTAT_DIR_KEY not in _config[CFG_PATH_KEY]:
            raise_missing_key(CFG_PATH_BEDSTAT_DIR_KEY)

        if CFG_PATH_BEDBUNCHER_DIR_KEY not in _config[CFG_PATH_KEY]:
            raise_missing_key(CFG_PATH_BEDBUNCHER_DIR_KEY)

        # Setting default values if doesn't exist in config file
        for section, mapping in DEFAULT_SECTION_VALUES.items():
            if section not in _config:
                _config[section] = {}
            for key, default in mapping.items():
                if key not in _config[section]:
                    _LOGGER.debug(
                        f"Config lacks '{section}.{key}' key. Setting to: {default}"
                    )
                    _config[section][key] = default

        if CFG_PATH_REGION2VEC_KEY not in _config[CFG_PATH_KEY]:
            _LOGGER.warning(f"Region2vec config key is missing in configuration file")
            _config[CFG_PATH_KEY][CFG_PATH_REGION2VEC_KEY] = None

        return _config

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

    def _get_output_path(
        self, table_name: str, remote_key: str, remote: bool = False
    ) -> str:
        """
        Get path to the output of the selected pipeline

        :param str table_name: name of the table that is populated by the
            pipeline to return the output path for
        :param str remote_key:
        :param bool remote: whether to use remote url base
        :return str: path to the selected pipeline output
        """
        dir_key = (
            CFG_PATH_BEDBUNCHER_DIR_KEY
            if table_name == BEDSET_TABLE
            else CFG_PATH_BEDSTAT_DIR_KEY
        )
        base = (
            self.config[CFG_REMOTE_KEY][remote_key]["prefix"]
            if remote
            else self.config[CFG_PATH_KEY][CFG_PATH_PIPELINE_OUTPUT_KEY]
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

    @property
    def t2bsi(self):
        """
        :return: object with search functions
        """
        return self._t2bsi

    def _create_t2bsi_object(self):
        """
        Create Text 2 BED search interface and return this object
        :return: Text2BEDSearchInterface object
        """
        qdrant_obj = QdrantClient(host=self._config[CFG_QDRANT_KEY][CFG_QDRANT_HOST_KEY],
                                  port=self._config[CFG_QDRANT_KEY][CFG_QDRANT_PORT_KEY],
                                  api_key=self._config[CFG_QDRANT_KEY][CFG_QDRANT_API_KEY],)

        return text2bednn.Text2BEDSearchInterface(
            nl2vec_model=SentenceTransformer(os.getenv("HF_MODEL", DEFAULT_HF_MODEL)),
            vec2vec_model=self._config[CFG_PATH_KEY][CFG_PATH_VEC2VEC_KEY],
            search_backend=qdrant_obj,
        )
