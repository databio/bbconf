import logging

from yacman import select_config

from .exceptions import *
from .const import *

_LOGGER = logging.getLogger(__name__)


def get_bedbase_cfg(cfg=None):
    """
    Determine path to the bedbase configuration file

    The path can be either explicitly provided
    or read from a $BEDBASE environment variable

    :param str cfg: path to the config file.
        Optional, the $BEDBASE config env var will be used if not provided
    :return str: configuration file path
    """
    selected_cfg = select_config(config_filepath=cfg, config_env_vars=CFG_ENV_VARS)
    if not selected_cfg:
        raise BedBaseConnectionError(
            f"You must provide a config file or set the "
            f"{'or '.join(CFG_ENV_VARS)} environment variable"
        )
    return selected_cfg
