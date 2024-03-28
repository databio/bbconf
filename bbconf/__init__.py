import logging
import coloredlogs

from ._version import __version__
from .const import PKG_NAME
from bbconf.bbagent import BedBaseAgent

__all__ = ["BedBaseAgent", "__version__"]

_LOGGER = logging.getLogger(PKG_NAME)
coloredlogs.install(
    logger=_LOGGER,
    datefmt="%H:%M:%S",
    fmt="[%(levelname)s] [%(asctime)s] [BBCONF] %(message)s",
)
