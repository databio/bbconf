import logging
import os
from typing import List, Union, Literal

from bbconf.modules.bedfiles import BedAgentBedFile
from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.const import PKG_NAME
from bbconf.exceptions import (
    BadAccessMethodError,
    MissingThumbnailError,
    MissingObjectError,
    BedBaseConfError,
)

from bbconf.models.drs_models import AccessMethod, AccessURL, DRSModel
from bbconf.models.bed_models import FileModel


_LOGGER = logging.getLogger(PKG_NAME)


class BBObjects:
    """ """

    def __init__(self, config: BedBaseConfig):
        """
        :param config: config object
        """
        self.config = config
        self.bed = BedAgentBedFile(self.config)

    def _get_prefixed_uri(self, postfix: str, access_id: str) -> str:
        """
        Return uri with correct prefix (schema)

        :param postfix: postfix of the uri (or everything after uri schema)
        :param access_id: access method name, e.g. http, s3, etc.
        :return: full uri path
        """

        try:
            prefix = getattr(self.config.config.access_methods, access_id).prefix
            return os.path.join(prefix, postfix)
        except KeyError:
            _LOGGER.error(f"Access method {access_id} is not defined.")
            raise BadAccessMethodError(f"Access method {access_id} is not defined.")

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
            return self._get_prefixed_uri(result.path_thumbnail, access_id)

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
        return self._get_prefixed_uri(result.path, access_id)

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
            result = self.bed.get_objects(identifier=record_id)[result_id]
        elif record_type == "bedset":
            # result = self.bedset.retrieve_one(record_id, result_id)
            _LOGGER.error("Not implemented")
            raise BedBaseConfError("ERROR NOT IMPLEMENTED YET")

        else:
            raise BedBaseConfError(
                f"Record type {record_type} is not supported. Only bed and bedset are supported."
            )

        _LOGGER.info(f"Getting uri for {record_type} {record_id} {result_id}")
        _LOGGER.info(f"Result: {result}")
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

        access_methods = []
        object_id = f"{record_type}.{record_id}.{result_id}"
        bed_result = self.bed.get(record_id)
        created_time = bed_result.submission_date
        modified_time = bed_result.last_update_date
        record_metadata = self._get_result(
            record_type, record_id, result_id
        )  # only get result once
        if not record_metadata:
            raise BedBaseConfError(f"Record not found")

        for access_id in self.config.config.access_methods.model_dump().keys():
            access_dict = AccessMethod(
                type=access_id,
                access_id=access_id,
                access_url=AccessURL(
                    url=self._get_prefixed_uri(record_metadata.path, access_id)
                ),
                region=self.config.config.access_methods.model_dump()[access_id].get(
                    "region", None
                ),
            )
            access_methods.append(access_dict)
        drs_dict = DRSModel(
            id=object_id,
            self_uri=f"drs://{base_uri}/{object_id}",
            size=record_metadata.size or "unknown",
            created_time=created_time,
            updated_time=modified_time,
            checksums=object_id,
            access_methods=access_methods,
        )

        return drs_dict
