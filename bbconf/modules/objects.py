import datetime
import logging
from typing import Literal

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.const import PKG_NAME
from bbconf.exceptions import (
    BedBaseConfError,
    MissingObjectError,
    MissingThumbnailError,
)
from bbconf.models.bed_models import FileModel
from bbconf.models.drs_models import DRSModel
from bbconf.modules.bedfiles import BedAgentBedFile
from bbconf.modules.bedsets import BedAgentBedSet

_LOGGER = logging.getLogger(PKG_NAME)


class BBObjects:
    """ """

    def __init__(self, config: BedBaseConfig):
        """
        Initialize BBObjects.

        Args:
            config: Config object.
        """
        self.config = config
        self.bed = BedAgentBedFile(self.config)
        self.bedset = BedAgentBedSet(self.config)

    def get_thumbnail_uri(
        self,
        record_type: Literal["bed", "bedset"],
        record_id: str,
        result_id: str,
        access_id: str = "http",
    ) -> str:
        """
        Create URL to access a bed- or bedset-associated thumbnail.

        Args:
            record_type: Table name ["bed", "bedset"].
            record_id: Record identifier.
            result_id: Column name (result name).
            access_id: Access id (e.g. http, s3, etc.).

        Returns:
            String with thumbnail.
        """
        result = self._get_result(record_type, record_id, result_id)
        if result.path_thumbnail:
            return self.config.get_prefixed_uri(result.path_thumbnail, access_id)

        else:
            _LOGGER.error(
                f"Thumbnail for {record_type} {record_id} {result_id} is not defined."
            )
            raise MissingThumbnailError(
                f"Thumbnail for {record_type} {record_id} {result_id} is not defined."
            )

    def get_object_uri(
        self,
        record_type: Literal["bed", "bedset"],
        record_id: str,
        result_id: str,
        access_id: str,
    ) -> str:
        """
        Create URL to access a bed- or bedset-associated file.

        Args:
            record_type: Table name ["bed", "bedset"].
            record_id: Record identifier.
            result_id: Column name (result name).
            access_id: Access id (e.g. http, s3, etc.).

        Returns:
            URI string for the object.
        """
        result = self._get_result(record_type, record_id, result_id)
        return self.config.get_prefixed_uri(result.path, access_id)

    def _get_result(
        self,
        record_type: Literal["bed", "bedset"],
        record_id: str,
        result_id: str | list[str],
    ) -> FileModel:
        """
        Generic getter that can return a result from either bed or bedset.

        Args:
            record_type: Table name ["bed", "bedset"].
            record_id: Record identifier.
            result_id: Column name (result name). e.g. "bigbedfile", "bed_file", "open_chromatin".

        Returns:
            Pipestat result.
        """
        if record_type == "bed":
            try:
                result = self.bed.get_objects(identifier=record_id)[result_id]
            except KeyError:
                _LOGGER.error(f"Result {result_id} is not defined for bed {record_id}")
                raise MissingObjectError(
                    f"Result {result_id} is not defined for bed {record_id}"
                )
        elif record_type == "bedset":
            try:
                result = self.bedset.get_objects(identifier=record_id)[result_id]
                _LOGGER.error(f"Result {result_id} is not defined for bed {record_id}")
            except KeyError:
                raise MissingObjectError(
                    f"Result {result_id} is not defined for bed {record_id}"
                )

        else:
            raise BedBaseConfError(
                f"Record type {record_type} is not supported. Only bed and bedset are supported."
            )

        _LOGGER.info(f"Getting uri for {record_type} {record_id} {result_id}")
        _LOGGER.debug(f"Result: {result}")
        return result

    def get_drs_metadata(
        self,
        record_type: Literal["bed", "bedset"],
        record_id: str,
        result_id: str,
        base_uri: str,
    ) -> DRSModel:
        """
        Get DRS metadata for a bed- or bedset-associated file.

        Args:
            record_type: Bed or bedset.
            record_id: Record identifier.
            result_id: Name of the result file to get metadata for.
            base_uri: Base uri to use for the self_uri field (server hostname of DRS broker).

        Returns:
            DRS metadata.
        """

        object_id = f"{record_type}.{record_id}.{result_id}"
        bed_result = self.bed.get(record_id)
        created_time = bed_result.submission_date
        modified_time = bed_result.last_update_date
        record_metadata = self._get_result(
            record_type, record_id, result_id
        )  # only get result once
        if not record_metadata:
            raise MissingObjectError("Record not found")

        drs_dict = self.construct_drs_metadata(
            base_uri,
            object_id,
            record_metadata,
            created_time,
            modified_time,
        )

        return drs_dict

    def construct_drs_metadata(
        self,
        base_uri: str,
        object_id: str,
        record_metadata: FileModel,
        created_time: datetime.datetime | None = None,
        modified_time: datetime.datetime | None = None,
    ) -> DRSModel:
        """
        Construct DRS metadata object.

        Args:
            base_uri: Base uri to use for the self_uri field (server hostname of DRS broker).
            object_id: Record identifier.
            record_metadata: Metadata of the record.
            created_time: Time of creation.
            modified_time: Time of last modification.

        Returns:
            DRS metadata.
        """
        access_methods = self.config.construct_access_method_list(record_metadata.path)
        drs_dict = DRSModel(
            id=object_id,
            self_uri=f"drs://{base_uri}/{object_id}",
            size=record_metadata.size or None,
            created_time=created_time,
            updated_time=modified_time,
            checksums=object_id,
            access_methods=access_methods,
        )
        return drs_dict
