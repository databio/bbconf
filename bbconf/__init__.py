# Project configuration, particularly for logging.

import logmuse
from .bbconf import *
from .const import *
from ._version import __version__

__classes__ = ["BedBaseConf"]
__all__ = __classes__ + ["get_bedbase_cfg"]

logmuse.init_logger("bbconf")
