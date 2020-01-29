from elasticsearch import Elasticsearch
from logging import getLogger

from attmap import PathExAttMap as PXAM
import yacman

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

        def _missing_key_msg(key, value):
            _LOGGER.debug("Config lacks '{}' key. Setting to: {}".format(key, value))

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

        if CFG_DATABASE_KEY not in self or CFG_HOST_KEY not in self[CFG_DATABASE_KEY]:
            _missing_key_msg(CFG_HOST_KEY, DB_DEFAULT_HOST)
            self[CFG_DATABASE_KEY][CFG_HOST_KEY] = DB_DEFAULT_HOST

        for index_name, index_value in IDX_MAP.items():
            if CFG_DATABASE_KEY not in self or index_name not in self[CFG_DATABASE_KEY]:
                _missing_key_msg(index_name, index_value)
                self[CFG_DATABASE_KEY][index_name] = index_value

    def establish_elasticsearch_connection(self, host=None):
        """
        Establish Elasticsearch connection using the config data

        :return elasticsearch.Elasticsearch: connected client
        """
        if hasattr(self, ES_CLIENT_KEY):
            raise BedBaseConnectionError("The connection is already established: {}".
                                         format(str(self[ES_CLIENT_KEY])))
        hst = host or self[CFG_DATABASE_KEY][CFG_HOST_KEY]
        self[ES_CLIENT_KEY] = Elasticsearch([{"host": hst}])
        _LOGGER.info("Established connection with Elasticsearch: {}".format(hst))
        _LOGGER.debug("Elasticsearch info:\n{}".format(self[ES_CLIENT_KEY].info()))

    def assert_connection(self):
        """
        Check whether an Elasticsearch connection has been established

        :raise BedBaseConnectionError: if there is no active connection
        """
        if not hasattr(self, ES_CLIENT_KEY):
            raise BedBaseConnectionError("No active connection with Elasticsearch")

    def _search_index(self, index_name, query, just_data=True):
        """
        Search selected Elasticsearch index with selected query

        :param str index_name: name of the Elasticsearch index to search
        :param dict query: query to search the DB against
        :param bool just_data: whether just the hits should be returned
        :return dict | Iterable[dict]: search results
        """
        self.assert_connection()
        _LOGGER.debug("Searching index: {}\nQuery: {}".format(index_name, query))
        query = {"query": query} if "query" not in query else query
        search_results = self[ES_CLIENT_KEY].search(index=index_name, body=query)
        return [r["_source"] for r in search_results["hits"]["hits"]] \
            if just_data else search_results

    def search_bedfiles(self, query, just_data=True):
        """
        Search selected Elasticsearch bedset index with selected query

        :param dict query: query to search the DB against
        :param bool just_data: whether just the hits should be returned
        :return dict | Iterable[dict]: search results
        """
        return self._search_index(index_name=BED_INDEX, query=query, just_data=just_data)

    def search_bedsets(self, query, just_data=True):
        """
        Search selected Elasticsearch bedfiles index with selected query

        :param dict query: query to search the DB against
        :param bool just_data: whether just the hits should be returned
        :return dict | Iterable[dict]: search results
        """
        return self._search_index(index_name=BEDSET_INDEX, query=query, just_data=just_data)

    def _insert_data(self, index, data, **kwargs):
        """
        Insert data to an index in a Elasticsearch DB
        or create it and the insert in case it does not exist

        :param str index: name of the index to insert the data into
        :param dict data: data to insert
        """
        self.assert_connection()
        self[ES_CLIENT_KEY].index(index=index, body=data, **kwargs)

    def insert_bedfiles_data(self, data, **kwargs):
        """
        Insert data to the bedfile index a Elasticsearch DB
        or create it and the insert in case it does not exist

        :param dict data: data to insert
        """
        self._insert_data(index=BED_INDEX, data=data, **kwargs)

    def insert_bedsets_data(self, data, **kwargs):
        """
        Insert data to the bedset index in a Elasticsearch DB
        or create it and the insert in case it does not exist

        :param dict data: data to insert
        """
        self._insert_data(index=BEDSET_INDEX, data=data, **kwargs)

    def _get_mapping(self, index, just_data=True, **kwargs):
        """
        Get mapping definitions for the selected index

        :param str index: index to return the mappging for
        :return dict: mapping definitions
        """
        self.assert_connection()
        mapping = self[ES_CLIENT_KEY].indices.get_mapping(index, **kwargs)
        return mapping[index]["mappings"]["properties"] if just_data else mapping

    def get_bedfiles_mapping(self, just_data=True, **kwargs):
        """
        Get mapping definitions for the bedfiles index

        :return: bedfiles mapping definitions
        """
        return self._get_mapping(index=BED_INDEX, just_data=just_data, **kwargs)

    def get_bedsets_mapping(self, just_data=True, **kwargs):
        """
        Get mapping definitions for the bedsets index

        :return: besets mapping definitions
        """
        return self._get_mapping(index=BEDSET_INDEX, just_data=just_data, **kwargs)


def get_bedbase_cfg(cfg=None):
    """
    Read and create the bedbase configuration object

    :param str cfg: path to the config file.
        Optional, the bedbase config env var will be used if not provided
    :return str: configuration file path
    """
    selected_cfg = yacman.select_config(config_filepath=cfg, config_env_vars=CFG_ENV_VARS)
    if not selected_cfg:
        raise BedBaseConnectionError("You must provide a config file or set the {} "
                                     "environment variable".format("or ".join(CFG_ENV_VARS)))
    return selected_cfg



