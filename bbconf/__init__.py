import logging

import coloredlogs

from importlib.metadata import version
from bbconf.bbagent import BedBaseAgent

from .const import PKG_NAME

__version__ = version(__package__)

__all__ = ["BedBaseAgent", "__version__"]

_LOGGER = logging.getLogger(PKG_NAME)
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BBCONF] %(message)s",
)
