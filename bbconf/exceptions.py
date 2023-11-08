import abc
from .const import DOC_URL


class BedBaseConfError(Exception):
    """Base exception type for this package"""
    __metaclass__ = abc.ABCMeta

class BadAccessMethodError(BedBaseConfError):
    """Access ID is not well defined"""
    pass
class MissingConfigDataError(BedBaseConfError):
    """Exception for invalid config file."""

    def __init__(self, msg):
        spacing = " " if msg[-1] in ["?", ".", "\n"] else "; "
        suggest = "For config format documentation please see: " + DOC_URL
        super(MissingConfigDataError, self).__init__(msg + spacing + suggest)

class BedBaseConnectionError(BedBaseConfError):
    """Error type for DB connection problems"""
    pass

class MissingThumbnailError(BedBaseConfError):
    """Error type for missing thumbnail"""
    pass

class MissingObjectError(BedBaseConfError):
    """Error type for missing object"""
    pass