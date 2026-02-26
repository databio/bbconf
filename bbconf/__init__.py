import logging

import coloredlogs

from bbconf.bbagent import BedBaseAgent

from .const import PKG_NAME

__all__ = ["BedBaseAgent"]

_LOGGER = logging.getLogger(PKG_NAME)
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BBCONF] %(message)s",
)
