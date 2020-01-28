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

    def get_connected_elasticsearch_client(self, host=None):
        """
        Establish Elasticsearch connection using the config data

        :return elasticsearch.Elasticsearch: connected client
        """
        hst = host or self[CFG_DATABASE_KEY][CFG_HOST_KEY]
        es = Elasticsearch([{"host": hst}])
        _LOGGER.debug("Elasticsearch info:\n{}".format(es.info()))
        _LOGGER.info("Established connection with Elasticsearch: {}".format(hst))
        return es


# config reading functions
def get_bedbase_cfg(cfg):
    """
    Read and create the bedbase configuration object

    :param str cfg: path to the config file.
        Optional, the bedbase config env var will be used if not provided
    :return bbconf.BedBaseConf: configuration object
    """
    selected_cfg = yacman.select_config(config_filepath=cfg, config_env_vars=CFG_ENV_VARS)
    assert selected_cfg is not None, "You must provide a config file or set the {} environment variable".\
        format("or ".join(CFG_ENV_VARS))
    return yacman.YacAttMap(filepath=selected_cfg)



