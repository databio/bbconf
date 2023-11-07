import os
from logging import getLogger
from typing import List, Optional, Tuple, Union
from textwrap import indent

import yacman
import pipestat

from sqlmodel import SQLModel, Field, select
import qdrant_client

from sqlalchemy.orm import relationship
from sqlalchemy import inspect

from bbconf.const import (
    CFG_PATH_KEY,
    CFG_PATH_PIPELINE_OUTPUT_KEY,
    CFG_PATH_BEDSTAT_DIR_KEY,
    DEFAULT_SECTION_VALUES,
    CFG_PATH_BEDBUNCHER_DIR_KEY,
    BED_TABLE,
    BED_TABLE_SCHEMA,
    BEDSET_TABLE,
    BEDSET_TABLE_SCHEMA,
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
    DEFAULT_VEC2VEC_MODEL,
    DEFAULT_REGION2_VEC_MODEL,
    DRS_ACCESS_URL,
    CFG_ACCESS_METHOD_KEY,
)
from bbconf.exceptions import *
from bbconf.helpers import raise_missing_key, get_bedbase_cfg
from bbconf.models import DRSModel, AccessMethod, AccessURL

# os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # to suppress verbose warnings tensorflow
from geniml.text2bednn import text2bednn
from geniml.search import QdrantBackend
from sentence_transformers import SentenceTransformer
from geniml.region2vec import Region2VecExModel
from geniml.io import RegionSet

_LOGGER = getLogger(__name__)


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
        _LOGGER.debug("Creating pipestat objects...")
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

        # setup t2bsi object
        self._t2bsi = None
        try:
            _LOGGER.debug("Setting up qdrant database connection...")
            if self.config[CFG_QDRANT_KEY].get(CFG_QDRANT_API_KEY, None):
                os.environ["QDRANT_API_KEY"] = self.config[CFG_QDRANT_KEY].get(
                    CFG_QDRANT_API_KEY
                )
            self._qdrant_backend = self._init_qdrant_backend()
            if self.config[CFG_PATH_KEY].get(CFG_PATH_REGION2VEC_KEY) and self.config[
                CFG_PATH_KEY
            ].get(CFG_PATH_VEC2VEC_KEY):
                self._t2bsi = self._create_t2bsi_object()
            else:
                if not self.config[CFG_PATH_KEY].get(CFG_PATH_REGION2VEC_KEY):
                    _LOGGER.debug(
                        f"{CFG_PATH_REGION2VEC_KEY} was not provided in config file! Using default.."
                    )
                if not self.config[CFG_PATH_KEY].get(CFG_PATH_VEC2VEC_KEY):
                    self.config[CFG_PATH_KEY][
                        CFG_PATH_VEC2VEC_KEY
                    ] = DEFAULT_VEC2VEC_MODEL

        except qdrant_client.http.exceptions.ResponseHandlingException as err:
            _LOGGER.error(f"error in Connection to qdrant! skipping... Error: {err}")

    def _read_config_file(self, config_path: str) -> yacman.YAMLConfigManager:
        """
        Read configuration file and insert default values if not set

        :param config_path: configuration file path
        :return: None
        :raises: raise_missing_key (if config key is missing)
        """
        _config = yacman.YAMLConfigManager(filepath=config_path).exp

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
            _LOGGER.warning("Region2vec config key is missing in configuration file")
            _config[CFG_PATH_KEY][CFG_PATH_REGION2VEC_KEY] = None

        return _config

    def search_bed_by_text(
        self, query: str
    ) -> Tuple[
        Union[List[int], List[List[int]]], Union[List[float], List[List[float]]]
    ]:
        """
        Search for bed files by text query in the qdrant database

        :param query: strign query provided by user
        :return: a list of dictionary that contains the search results in this format:
        {
            "id": <id>
            "score": <score>
            "payload": {
                <information of the vector>
            }
            "vector": [<the vector>]
        }
        """
        if self._t2bsi is None:
            raise BedBaseConfError(
                "Can't perform search, qdrant_db credentials are not provided in config file"
            )
        return self._t2bsi.nl_vec_search(query)

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
        res += "\nconfig:\n"
        res += f"{indent(str(self.config), '  ')}"
        return res

    @property
    def config(self) -> yacman.YAMLConfigManager:
        """
        Config used to initialize the object

        :return yacman.YAMLConfigManager: bedbase configuration file contents
        """
        return self._config

    @property
    def bed(self) -> pipestat.PipestatManager:
        """
        PipestatManager of the bedfiles table

        :return pipestat.PipestatManager: manager of the bedfiles table
        """
        return self.__pipestats[BED_TABLE]

    @property
    def bedset(self) -> pipestat.PipestatManager:
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

    def get_bedbuncher_output_path(self, remote_key, remote=False) -> str:
        """
        Get path to the output of the bedbuncher pipeline

        :param bool remote: whether to use remote url base
        :return str: path to the bedbuncher pipeline output
        """
        return self._get_output_path(
            table_name=BEDSET_TABLE, remote_key=remote_key, remote=remote
        )

    def get_bedstat_output_path(self, remote_key, remote=False) -> str:
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
                foreign_key=f"{self.bed.cfg['pipeline_name']}__sample.id",
                primary_key=True,
            )
            bedset_id: Optional[int] = Field(
                default=None,
                foreign_key=f"{self.bedset.cfg['pipeline_name']}__sample.id",
                primary_key=True,
            )

            __table_args__ = {"extend_existing": True}

        returned_model = BedFileBedSetAssociation.__table__

        # this will create a relationship between bedfiles and bedsets, and will have mapping in both tables (bedfiles, bedsets)
        self.BedfileORM.__mapper__.add_property(
            BEDSETS_REL_KEY,
            relationship(
                self.BedsetORM,
                secondary=returned_model,
                backref=BEDFILES_REL_KEY,
            ),
        )

        SQLModel.metadata.create_all(bind=self.bed.backend.db_engine_key)

    def report_relationship(
        self, bedset_record_id: str, bedfile_record_id: str
    ) -> None:
        """
        Report a bedfile for bedset.

        Inserts the ID pair into the relationship table, which allows to
        manage many to many bedfile bedset relationships

        :param int bedset_record_id: record identifier of the bedset to report bedfile for
        :param int bedfile_record_id: record identifier  of the bedfile to report
        """

        if not self._check_table_exists(
            table_name=BEDFILE_BEDSET_ASSOCIATION_TABLE_KEY
        ):
            self._create_bedset_bedfiles_table()

        with self.bed.backend.session as s:
            bedset_statement = select(self.BedsetORM).where(
                self.BedsetORM.record_identifier == bedset_record_id
            )
            bedset = s.exec(bedset_statement).one()

            bedfile_statement = select(self.BedfileORM).where(
                self.BedfileORM.record_identifier == bedfile_record_id
            )
            bedfile = s.exec(bedfile_statement).one()

            if not bedfile:
                raise BedBaseConfError(
                    f"Bedfile with if: {bedfile_record_id} doesn't exists. Can't add bedfile to bedset"
                )

            # add relationship
            bedset.bedfiles.append(bedfile)
            s.add(bedfile)
            s.commit()

        return None

    def remove_relationship(
        self, bedset_record_id: str, bedfile_record_id: Union[str, List[str]] = None
    ) -> None:
        """
        Remove entries from the relationships table

        :param str bedset_record_id: id of the bedset to remove
        :param list[str] bedfile_record_id: ids of the bedfiles to remove for the
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
            bedset_statement = select(self.BedsetORM).where(
                self.BedsetORM.record_identifier == bedset_record_id
            )
            bedset = s.exec(bedset_statement).one()

            if bedfile_record_id is None:
                list_of_bed = [bed for bed in bedset.bedfiles]

                for bed in list_of_bed:
                    bedset.bedfiles.remove(bed)
            else:
                if isinstance(bedfile_record_id, str):
                    bedfile_record_id = [bedset_record_id]

                for bedfile_id in bedfile_record_id:
                    bedfile_statement = select(self.BedfileORM).where(
                        self.BedfileORM.record_identifier == bedfile_id
                    )
                    bedfile = s.exec(bedfile_statement).one()

                    bedset.bedfiles.remove(bedfile)
            s.add(bedset)
            s.commit()

    # TODO: should this function be at this format? Do we want to get all the bedfiles for a bedset?
    # def select_bedfiles_from_bedset(
    #     self,
    #     filter_conditions: Optional[List[Tuple[str, str, Union[str, List[str]]]]] = [],
    #     json_filter_conditions: Optional[List[Tuple[str, str, str]]] = [],
    #     bedfile_cols: Optional[List[str]] = None,
    # ) -> list:
    #     """
    #     Select bedfiles that are part of a bedset that matches the query
    #
    #     :param List[str] filter_conditions:  table query to restrict the results with
    #     :param Union[List[str], str] bedfile_cols: bedfile columns to include in the
    #         result, if none specified all columns will be included
    #     :return: matched bedfiles table contents
    #     """
    #     cols = (
    #         [getattr(self.BedfileORM, bedfile_col) for bedfile_col in bedfile_cols]
    #         if bedfile_cols is not None
    #         else self.BedfileORM.__table__.columns
    #     )
    #     with self.bed.backend.session as session:
    #         statement = session.query(*cols).join(
    #             self.BedfileORM, self.BedsetORM.bedfiles
    #         )
    #         statement = dynamic_filter(
    #             ORM=self.BedsetORM,
    #             statement=statement,
    #             filter_conditions=filter_conditions,
    #             json_filter_conditions=json_filter_conditions,
    #         )
    #         bed_names = statement.all()
    #
    #     return bed_names

    def select_unique(self, table_name: str, column: str = None) -> List[dict]:
        """
        Select unique value in given column and table

        :param str table_name: table to query in
        :param str column: column to include in the result
        :return list[dict]: unique entries in the column
        """

        if table_name == "bedfile__sample":
            with self.bed.backend.session:
                values = self.bed.backend.select_records(columns=column)["records"]
        elif table_name == "bedsets__sample":
            with self.bedset.backend.session:
                values = self.bedset.backend.select_records(columns=column)["records"]
        else:
            raise pipestat.exceptions.SchemaError(
                f"Incorrect table name provided {table_name}"
            )

        return [i for n, i in enumerate(values) if i not in values[n + 1 :]]

    @property
    def BedfileORM(self) -> SQLModel:
        """
        return: ORM of bedfile table (SQLModelMetaclass)
        """
        return self.bed.backend.get_model("bedfile__sample")

    @property
    def BedsetORM(self) -> SQLModel:
        """
        return: ORM of bedset table (SQLModelMetaclass)
        """
        return self.bedset.backend.get_model("bedsets__sample")

    @property
    def t2bsi(self) -> text2bednn.Text2BEDSearchInterface:
        """
        :return: object with search functions
        """
        return self._t2bsi

    @property
    def qdrant_backend(self) -> QdrantBackend:
        return self._qdrant_backend

    def _init_qdrant_backend(self) -> QdrantBackend:
        """
        Create qdrant client object using credentials provided in config file
        :return: QdrantClient
        """
        return QdrantBackend(
            collection=self._config[CFG_QDRANT_KEY][CFG_QDRANT_COLLECTION_NAME_KEY],
            qdrant_host=self._config[CFG_QDRANT_KEY][CFG_QDRANT_HOST_KEY],
            qdrant_port=self._config[CFG_QDRANT_KEY][CFG_QDRANT_PORT_KEY],
        )

    def _create_t2bsi_object(self) -> text2bednn.Text2BEDSearchInterface:
        """
        Create Text 2 BED search interface and return this object
        :return: Text2BEDSearchInterface object
        """

        return text2bednn.Text2BEDSearchInterface(
            nl2vec_model=SentenceTransformer(os.getenv("HF_MODEL", DEFAULT_HF_MODEL)),
            vec2vec_model=self._config[CFG_PATH_KEY][CFG_PATH_VEC2VEC_KEY],
            search_backend=self.qdrant_backend,
        )

    def add_bed_to_qdrant(
        self,
        bed_id: str,
        bed_file: Union[str, RegionSet],
        payload: dict = None,
        region_to_vec: Region2VecExModel = None,
    ) -> None:
        """
        Convert bed file to vector and add it to qdrant database

        :param bed_id: bed file id
        :param bed_file: path to the bed file, or RegionSet object
        :param payload: additional metadata to store alongside vectors
        :param region_to_vec: initiated region to vector model. If None, new object will be created.
        :return: None
        """

        _LOGGER.info(f"Adding bed file to qdrant. bed_id: {bed_id}")
        # Convert bedfile to vector
        if isinstance(bed_file, str):
            bed_region_set = RegionSet(bed_file)
        elif isinstance(bed_file, RegionSet):
            bed_region_set = bed_file
        else:
            raise BedBaseConfError(
                "Could not add add region to qdrant. Invalid type, or path. "
            )
        if not region_to_vec:
            reg_2_vec_obj = Region2VecExModel(DEFAULT_REGION2_VEC_MODEL)
        else:
            reg_2_vec_obj = region_to_vec
        bed_embedding = reg_2_vec_obj.encode(
            bed_region_set,
            pool="mean",
        )

        # Upload bed file vector to the database
        vec_dim = bed_embedding.shape[0]
        self.qdrant_backend.load(
            ids=[bed_id],
            vectors=bed_embedding.reshape(1, vec_dim),
            payloads=[{**payload}],
        )
        return None


    def get_prefixed_uri(self, postfix: str, access_id: str) -> str:
        """
        Return uri with correct prefix (schema)

        :param postfix: postfix of the uri (or everything after uri schema)
        :param access_id: access method name
        :return: full uri path
        """
        
        try: 
            prefix = self.config[CFG_ACCESS_METHOD_KEY][access_id]["prefix"]
            return os.path.join(prefix, postfix)
        except KeyError:
            _LOGGER.error(f"Access method {access_id} is not defined.")
            raise IncorrectAccessMethodError(
                f"Access method {access_id} is not defined."
            )

    def get_thumbnail_uri(
        self,
        record_type: str,
        record_id: str,
        result_id: str,
        access_id: str
    ):
        try:
            result = self.get_result(record_type, record_id, result_id)
            return self.get_prefixed_uri(result["thumbnail_path"], "http")
        except KeyError:
            _LOGGER.error(f"Thumbnail for {record_type} {record_id} {result_id} is not defined.")
            raise MissingThumbnailError(
                f"Thumbnail for {record_type} {record_id} {result_id} is not defined."
            )


    def get_result(self, record_type: str, record_id: str, result_id: str):
        if record_type == "bed":
            result = self.bed.retrieve_one(record_id, result_id)
            # schema = self.bed.schema.original_schema
            schema = self.bed.schema._sample_level_data[result_id]
        elif record_type == "bedset":
            result = self.bedset.retrieve_one(record_id, result_id)
        
        result_type = schema.get("object_type", schema["type"])
        _LOGGER.info(f"Getting uri for {record_type} {record_id} {result_id}")
        _LOGGER.info(f"Result type: {result_type}")
        _LOGGER.info(f"Result: {result}")
        return result

    def get_object_uri(
        self,
        record_type: str,
        record_id: str,
        result_id: str,
        access_id: str
    ):
        result = self.get_result(record_type, record_id, result_id)
        return self.get_prefixed_uri(result["path"], access_id)

    
    def get_drs_metadata(self, record_type: str, record_id: str, result_id: str, base_uri: str) -> DRSModel:
        """
        Get DRS metadata for a bed- or bedset-associated file

        :param record_type: bed or bedset
        :param record_id: record identifier
        :param result_id: name of the result file to get metadata for
        :param base_uri: base uri to use for the self_uri field (server hostname of DRS broker)
        :return: DRS metadata
        """

        if record_type == "bed":
            record_metadata = self.bed.retrieve_one(record_id)
        elif record_type == "bedset":
            record_metadata = self.bedset.retrieve_one(record_id)
        _LOGGER.info(f"DRS Metadata for {record_type} {record_id} {result_id}: {record_metadata}")
        access_methods = []
        object_id = f"{record_type}.{record_id}.{result_id}"
        for access_id in self.config[CFG_ACCESS_METHOD_KEY].keys():
            access_dict = AccessMethod(
                type=access_id,
                access_id=access_id,
                access_url=AccessURL(url=self.get_object_uri(record_type, record_id, result_id, access_id)),
                region=self.config[CFG_ACCESS_METHOD_KEY][access_id].get(
                    "region", None
                ),
            )
            access_methods.append(access_dict)
        drs_dict = DRSModel(
            id=object_id,
            self_uri=f"drs://{base_uri}/{object_id}",
            size=record_metadata[result_id].get("size", "unknown"),
            created_time=record_metadata["pipestat_created_time"],
            updated_time=record_metadata["pipestat_modified_time"],
            checksums=object_id,
            access_methods=access_methods,
        )

        return drs_dict
