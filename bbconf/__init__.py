import logmuse

from bbconf._version import __version__
from bbconf.bbconf import *
from bbconf.const import *

__all__ = ["BedBaseConf", "get_bedbase_cfg"]

logmuse.init_logger("bbconf")
