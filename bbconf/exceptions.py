import abc


class BedBaseConfError(Exception):
    """Base exception type for this package"""

    __metaclass__ = abc.ABCMeta


class BadAccessMethodError(BedBaseConfError):
    """Access ID is not well defined"""

    pass


class BedBaseConnectionError(BedBaseConfError):
    """Error type for DB connection problems"""

    pass


class MissingThumbnailError(BedBaseConfError):
    """Error type for missing thumbnail"""

    pass


class MissingObjectError(BedBaseConfError):
    """Error type for missing object"""

    pass


class BEDFileNotFoundError(BedBaseConfError):
    """Error type for missing object"""

    pass
