from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConflictError
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

    def establish_elasticsearch_connection(self, host=None):
        """
        Establish Elasticsearch connection using the config data

        :return elasticsearch.Elasticsearch: connected client
        """
        if hasattr(self, ES_CLIENT_KEY):
            raise BedBaseConnectionError(
                "The connection is already established: {}".
                    format(str(self[ES_CLIENT_KEY]))
            )
        hst = host or self[CFG_DATABASE_KEY][CFG_HOST_KEY]
        self[ES_CLIENT_KEY] = Elasticsearch([{"host": hst}])
        _LOGGER.info("Established connection with Elasticsearch: {}".
                     format(hst))
        _LOGGER.debug("Elasticsearch info:\n{}".
                      format(self[ES_CLIENT_KEY].info()))

    def assert_connection(self):
        """
        Check whether an Elasticsearch connection has been established

        :raise BedBaseConnectionError: if there is no active connection
        """
        if not hasattr(self, ES_CLIENT_KEY):
            raise BedBaseConnectionError(
                "No active connection with Elasticsearch"
            )

    def _search_index(self, index_name, query, just_data=True, size=None,
                      **kwargs):
        """
        Search selected Elasticsearch index with selected query

        :param str index_name: name of the Elasticsearch index to search
        :param dict query: query to search the DB against
        :param bool just_data: whether just the hits should be returned
        :param int size: number of hits to return, all are returned by default
        :return dict | Iterable[dict] | NoneType: search results
            or None if requested index does not exist
        """
        self.assert_connection()
        if not self[ES_CLIENT_KEY].indices.exists(index_name):
            _LOGGER.warning("'{}' index does not exist".format(index_name))
            return
        _LOGGER.debug("Searching index: {}\nQuery: {}".
                      format(index_name, query))
        query = {"query": query} if "query" not in query else query
        size = size or self._count_docs(index=index_name)
        search_results = self[ES_CLIENT_KEY].search(
            index=index_name, body=query, size=size, **kwargs)
        return [r["_source"] for r in search_results["hits"]["hits"]] \
            if just_data else search_results

    def search_bedfiles(self, query, just_data=True, **kwargs):
        """
        Search selected Elasticsearch bedset index with selected query

        :param dict query: query to search the DB against
        :param bool just_data: whether just the hits should be returned
        :return dict | Iterable[dict]: search results
        """
        return self._search_index(index_name=BED_INDEX, query=query,
                                  just_data=just_data, **kwargs)

    def search_bedsets(self, query, just_data=True, **kwargs):
        """
        Search selected Elasticsearch bedfiles index with selected query

        :param dict query: query to search the DB against
        :param bool just_data: whether just the hits should be returned
        :return dict | Iterable[dict]: search results
        """
        return self._search_index(index_name=BEDSET_INDEX, query=query,
                                  just_data=just_data, **kwargs)

    def _insert_data(self, index, data, doc_id, force_update=False, **kwargs):
        """
        Insert document to an index in a Elasticsearch DB
        or create it and the insert in case it does not exist.

        Document ID argument is optional. If not provided, a random ID
        will be assigned.
        If provided the document will be inserted only if no documents with
        this ID are present in the DB. However, the document overwriting
        can be forced if needed.

        :param str index: name of the index to insert the data into
        :param str doc_id: unique identifier for the document
        :param bool force_update: whether the pre-existing document
         should be overwritten
        :param dict data: data to insert
        """
        self.assert_connection()
        if doc_id is None:
            _LOGGER.info("Inserting document to index '{}' with an "
                         "automatically-assigned ID".format(index))
            self[ES_CLIENT_KEY].index(index=index, body=data, **kwargs)
        else:
            try:
                self[ES_CLIENT_KEY].create(index=index, body=data, id=doc_id,
                                           **kwargs)
            except ConflictError:
                msg_base = "Document '{}' already exists in index '{}'"\
                    .format(doc_id, index)
                if force_update:
                    _LOGGER.info(msg_base + ". Forcing update")
                    self[ES_CLIENT_KEY].index(index=index, body=data, id=doc_id,
                                              **kwargs)
                else:
                    _LOGGER.error("Could not insert data. " + msg_base)
                    raise

    def insert_bedfiles_data(self, data, doc_id=None, **kwargs):
        """
        Insert data to the bedfile index a Elasticsearch DB
        or create it and the insert in case it does not exist.

        Document ID argument is optional. If not provided, a random ID will
        be assigned. If provided the document will be inserted only if no
        documents with this ID are present in the DB. However, the document
        overwriting can be forced if needed.

        :param dict data: data to insert
        :param str doc_id: unique identifier for the document, optional
        """
        self._insert_data(index=BED_INDEX, data=data, doc_id=doc_id, **kwargs)

    def insert_bedsets_data(self, data, doc_id=None, **kwargs):
        """
        Insert data to the bedset index in a Elasticsearch DB
        or create it and the insert in case it does not exist.

        Document ID argument is optional. If not provided, a random ID will
        be assigned.
        If provided the document will be inserted only if no documents with
        this ID are present in the DB.
        However, the document overwriting can be forced if needed.

        :param dict data: data to insert
        :param str doc_id: unique identifier for the document, optional
        """
        self._insert_data(index=BEDSET_INDEX, data=data, doc_id=doc_id,
                          **kwargs)

    def _get_mapping(self, index, just_data=True, **kwargs):
        """
        Get mapping definitions for the selected index

        :param str index: index to return the mappging for
        :return dict: mapping definitions
        """
        self.assert_connection()
        mapping = self[ES_CLIENT_KEY].indices.get_mapping(index, **kwargs)
        return mapping[index]["mappings"]["properties"] \
            if just_data else mapping

    def get_bedfiles_mapping(self, just_data=True, **kwargs):
        """
        Get mapping definitions for the bedfiles index

        :return dict: bedfiles mapping definitions
        """
        return self._get_mapping(index=BED_INDEX, just_data=just_data, **kwargs)

    def get_bedsets_mapping(self, just_data=True, **kwargs):
        """
        Get mapping definitions for the bedsets index

        :return dict: besets mapping definitions
        """
        return self._get_mapping(index=BEDSET_INDEX, just_data=just_data,
                                 **kwargs)

    def _get_doc(self, index, doc_id):
        """
        Get a document from an index by its ID

        :param str index: name of the index to search
        :param str doc_id: document ID to return
        :return Mapping: matched document
        """
        return self[ES_CLIENT_KEY].get(index=index, id=doc_id)

    def get_bedfiles_doc(self, doc_id):
        """
        Get a document from bedfiles index by its ID

        :param str doc_id: document ID to return
        :return Mapping: matched document
        """
        return self._get_doc(index=BED_INDEX, doc_id=doc_id)

    def get_bedsets_doc(self, doc_id):
        """
        Get a document from bedsets index by its ID

        :param str doc_id: document ID to return
        :return Mapping: matched document
        """
        return self._get_doc(index=BEDSET_INDEX, doc_id=doc_id)

    def _count_docs(self, index):
        """
        Get the total number of the documents in a selected index

        :param str index: index to count the documents for
        :return int | None: number of documents
        """
        self.assert_connection()
        if not self[ES_CLIENT_KEY].indices.exists(index=index):
            _LOGGER.warning("'{}' index does not exist".format(index))
            return None
        return int(self[ES_CLIENT_KEY].cat.count(
            index, params={"format": "json"})[0]['count'])

    def count_bedfiles_docs(self):
        """
        Get the total number of the documents in the bedfiles index

        :return int: number of documents
        """
        return self._count_docs(index=BED_INDEX)

    def count_bedsets_docs(self):
        """
        Get the total number of the documents in the bedsets index

        :return int: number of documents
        """
        return self._count_docs(index=BEDSET_INDEX)

    def _delete_index(self, index):
        """
        Delete selected index from Elasticsearch

        :param str index: name of the index to delete
        """
        self.assert_connection()
        self[ES_CLIENT_KEY].indices.delete(index=index)

    def delete_bedfiles_index(self):
        """
        Delete bedfiles index from Elasticsearch
        """
        self._delete_index(index=BED_INDEX)

    def delete_bedsets_index(self):
        """
        Delete bedsets index from Elasticsearch
        """
        self._delete_index(index=BEDSET_INDEX)

    def _get_all(self, index_name, just_data=False):
        """
        Convenience method for index exploration

        :param str index_name: name of the Elasticsearch index to search
        :param bool just_data: whether just the hits should be returned
        :return:
        """
        self.assert_connection()
        return self._search_index(index_name=index_name, query=QUERY_ALL,
                                  just_data=just_data)


def get_bedbase_cfg(cfg=None):
    """
    Determine path to the bedbase configuration file

    The path can be either explicitly provided
    or read from a $BEDBASE environment variable

    :param str cfg: path to the config file.
        Optional, the $BEDBASE config env var will be used if not provided
    :return str: configuration file path
    """
    selected_cfg = yacman.select_config(config_filepath=cfg,
                                        config_env_vars=CFG_ENV_VARS)
    if not selected_cfg:
        raise BedBaseConnectionError(
            "You must provide a config file or set the {} environment variable"
                .format("or ".join(CFG_ENV_VARS))
        )
    return selected_cfg



