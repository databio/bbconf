import logging

import coloredlogs

from bbconf.bbagent import BedBaseAgent

from ._version import __version__
from .const import PKG_NAME

__all__ = ["BedBaseAgent", "__version__"]

_LOGGER = logging.getLogger(PKG_NAME)
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BBCONF] %(message)s",
)
