# Project configuration, particularly for logging.

import logmuse
from .bbconf import *
from .const import *
from ._version import __version__

__classes__ = ["BedBaseConf"]
__all__ = __classes__ + []

logmuse.init_logger("bbconf")
