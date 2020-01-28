import abc
from .const import DOC_URL

__all__ = ["BedBaseConfError", "MissingConfigDataError", "BedBaseConnectionError"]


class BedBaseConfError(Exception):
    """ Base exception type for this package """
    __metaclass__ = abc.ABCMeta


class MissingConfigDataError(BedBaseConfError):
    """ Exception for invalid config file. """
    def __init__(self, msg):
        spacing = " " if msg[-1] in ["?", ".", "\n"] else "; "
        suggest = "For config format documentation please see: " + DOC_URL
        super(MissingConfigDataError, self).__init__(msg + spacing + suggest)


class BedBaseConnectionError(BedBaseConfError):
    """ Error type for DB connection problems """
    pass
