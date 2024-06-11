import datetime
import logging
from typing import List, Literal, Union

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
        :param config: config object
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
        Create URL to access a bed- or bedset-associated thumbnail

        :param record_type: table_name ["bed", "bedset"]
        :param record_id: record identifier
        :param result_id: column name (result name)
        :param access_id: access id (e.g. http, s3, etc.)
        :return: string with thumbnail
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
        Create URL to access a bed- or bedset-associated file

        :param record_type: table_name ["bed", "bedset"]
        :param record_id: record identifier
        :param result_id: column name (result name)
        :param access_id: access id (e.g. http, s3, etc.)
        :return:
        """
        result = self._get_result(record_type, record_id, result_id)
        return self.config.get_prefixed_uri(result.path, access_id)

    def _get_result(
        self,
        record_type: Literal["bed", "bedset"],
        record_id: str,
        result_id: Union[str, List[str]],
    ) -> FileModel:
        """
        Generic getter that can return a result from either bed or bedset

        :param record_type: table_name ["bed", "bedset"]
        :param record_id: record identifier
        :param result_id: column name (result name). e.g. "bigbedfile", "bed_file", "open_chromatin"
        :return: pipestat result
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
        Get DRS metadata for a bed- or bedset-associated file

        :param record_type: bed or bedset
        :param record_id: record identifier
        :param result_id: name of the result file to get metadata for
        :param base_uri: base uri to use for the self_uri field (server hostname of DRS broker)
        :return: DRS metadata
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
        created_time: datetime.datetime = None,
        modified_time: datetime.datetime = None,
    ):
        """
        Construct DRS metadata object

        :param base_uri: base uri to use for the self_uri field (server hostname of DRS broker)
        :param object_id: record identifier
        :param record_metadata: metadata of the record
        :param created_time: time of creation
        :param modified_time: time of last modification
        :return: DRS metadata
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
