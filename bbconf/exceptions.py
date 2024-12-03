import abc


class BedBaseConfError(Exception):
    """Base exception type for this package"""

    __metaclass__ = abc.ABCMeta


class BedbaseS3ConnectionError(BedBaseConfError):
    """connection error to s3"""

    pass


class BadAccessMethodError(BedBaseConfError):
    """Access ID is not well defined"""

    pass


class BedBaseConnectionError(BedBaseConfError):
    """Error type for DB connection problems"""

    pass


class MissingThumbnailError(BedBaseConfError):
    """Error type for missing thumbnail"""

    pass


class BedFIleExistsError(BedBaseConfError):
    """Error where files exists, and should not be overwritten"""

    pass


class MissingObjectError(BedBaseConfError):
    """Error type for missing object"""

    pass


class BEDFileNotFoundError(BedBaseConfError):
    """Error type for missing bedfile"""

    pass


class BedSetNotFoundError(BedBaseConfError):
    """Error type for missing bedset"""

    pass


class BedSetExistsError(BedBaseConfError):
    """Error type for existing bedset"""

    pass


class UniverseNotFoundError(BedBaseConfError):
    """Error type for missing universe"""

    pass


class TokenizeFileExistsError(BedBaseConfError):
    """Error type for existing tokenize file"""

    pass


class TokenizeFileNotExistError(BedBaseConfError):
    """Error type for missing tokenize file"""

    pass


class QdrantInstanceNotInitializedError(BedBaseConfError):
    """Error type for missing qdrant instance"""

    pass


class BedSetTrackHubLimitError(BedBaseConfError):
    """Limit for visualizing trackhub exceeded"""

    pass
