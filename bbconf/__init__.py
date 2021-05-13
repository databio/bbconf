# Project configuration, particularly for logging.

import logmuse

from ._version import __version__
from .bbconf import *
from .const import *

__classes__ = ["BedBaseConf"]
__all__ = __classes__ + ["get_bedbase_cfg"]

logmuse.init_logger("bbconf")
