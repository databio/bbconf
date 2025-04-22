import logging
import os

from yacman import select_config

from bbconf.exceptions import BedBaseConnectionError

_LOGGER = logging.getLogger(__name__)


CFG_ENV_VARS = ["BEDBASE"]


def get_bedbase_cfg(cfg: str = None) -> str:
    """
    Determine path to the bedbase configuration file

    The path can be either explicitly provided
    or read from a $BEDBASE environment variable

    :param str cfg: path to the config file.
        Optional, the $BEDBASE config env var will be used if not provided
    :return str: absolute configuration file path
    """

    _LOGGER.info(f"Loading configuration file: {cfg}")
    try:
        selected_cfg = select_config(
            config_filepath=cfg,
            config_env_vars=CFG_ENV_VARS,
        )
    except OSError:
        selected_cfg = None

    if not selected_cfg:
        raise BedBaseConnectionError(
            f"You must provide a config file or set the "
            f"{'or '.join(CFG_ENV_VARS)} environment variable"
        )
    return selected_cfg


def get_absolute_path(path: str, base_path: str) -> str:
    """
    Get absolute path to the file and create it if it doesn't exist

    :param path: path to the file (abs or relative)
    :param base_path: base path to the file (will be added to the relative path)

    :return: absolute path to the file
    """
    if not os.path.isabs(path) or not os.path.exists(path):
        return os.path.join(base_path, path)
    return path
