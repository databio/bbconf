from pathlib import Path
from typing import Union, List

from bbconf.config_parser.bedbaseconfig import BedBaseConfig

from bbconf.db_utils import POSTGRES_DIALECT, BaseEngine
from bbconf.modules.bedfiles import BedAgentBedFile
from bbconf.modules.bedsets import BedAgentBedSet
from bbconf.modules.objects import BBObjects


class BedBaseAgent(object):
    def __init__(
        self,
        config: Union[Path, str],
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.

        """

        self.config = BedBaseConfig(config)

        self.__bed = BedAgentBedFile(self.config)

        # ff = self.__bed.add("test", {"number_of_regions": 44})

        #ff

        # self.__bedset = BedAgentBedSet(self.config)
        # self.__objects = BBObjects(self.config)

    @property
    def bed(self) -> BedAgentBedFile:
        return self.__bed

    # @property
    # def bedset(self):
    #     return self.__bedset
    #
    # @property
    # def objects(self):
    #     return self.__objects

    # def get_prefixed_uri(self, postfix: str, access_id: str) -> str:
    #     """
    #     Return uri with correct prefix (schema)
    #
    #     :param postfix: postfix of the uri (or everything after uri schema)
    #     :param access_id: access method name
    #     :return: full uri path
    #     """
    #
    #     try:
    #         prefix = self.config[CFG_ACCESS_METHOD_KEY][access_id]["prefix"]
    #         return os.path.join(prefix, postfix)
    #     except KeyError:
    #         _LOGGER.error(f"Access method {access_id} is not defined.")
    #         raise BadAccessMethodError(f"Access method {access_id} is not defined.")
    #
    # def get_thumbnail_uri(
    #     self,
    #     record_type: Literal["bed", "bedset"],
    #     record_id: str,
    #     result_id: str,
    #     access_id: str = "http",
    # ) -> str:
    #     """
    #     Create URL to access a bed- or bedset-associated thumbnail
    #
    #     :param record_type: table_name ["bed", "bedset"]
    #     :param record_id: record identifier
    #     :param result_id: column name (result name)
    #     :param access_id: access id (e.g. http, s3, etc.)
    #     :return: string with thumbnail
    #     """
    #
    #     try:
    #         result = self.get_result(record_type, record_id, result_id)
    #         return self.get_prefixed_uri(result["thumbnail_path"], access_id)
    #     except KeyError:
    #         _LOGGER.error(
    #             f"Thumbnail for {record_type} {record_id} {result_id} is not defined."
    #         )
    #         raise MissingThumbnailError(
    #             f"Thumbnail for {record_type} {record_id} {result_id} is not defined."
    #         )
    #
    # def get_object_uri(
    #     self,
    #     record_type: Literal["bed", "bedset"],
    #     record_id: str,
    #     result_id: str,
    #     access_id: str,
    # ) -> str:
    #     """
    #     Create URL to access a bed- or bedset-associated file
    #
    #     :param record_type: table_name ["bed", "bedset"]
    #     :param record_id: record identifier
    #     :param result_id: column name (result name)
    #     :param access_id: access id (e.g. http, s3, etc.)
    #     :return:
    #     """
    #     result = self.get_result(record_type, record_id, result_id)
    #     return self.get_prefixed_uri(result["path"], access_id)
    #
    # def get_result(
    #     self,
    #     record_type: Literal["bed", "bedset"],
    #     record_id: str,
    #     result_id: Union[str, List[str]],
    # ) -> dict:
    #     """
    #     Generic getter that can return a result from either bed or bedset
    #
    #     :param record_type: table_name ["bed", "bedset"]
    #     :param record_id: record identifier
    #     :param result_id: column name (result name)
    #     :return: pipestat result
    #     """
    #     if record_type == "bed":
    #         result = self.bed.retrieve_one(record_id, result_id)
    #     elif record_type == "bedset":
    #         result = self.bedset.retrieve_one(record_id, result_id)
    #     else:
    #         raise BedBaseConfError(
    #             f"Record type {record_type} is not supported. Only bed and bedset are supported."
    #         )
    #
    #     _LOGGER.info(f"Getting uri for {record_type} {record_id} {result_id}")
    #     _LOGGER.info(f"Result: {result}")
    #     return result
    #
    # def get_drs_metadata(
    #     self,
    #     record_type: Literal["bed", "bedset"],
    #     record_id: str,
    #     result_id: str,
    #     base_uri: str,
    # ) -> DRSModel:
    #     """
    #     Get DRS metadata for a bed- or bedset-associated file
    #
    #     :param record_type: bed or bedset
    #     :param record_id: record identifier
    #     :param result_id: name of the result file to get metadata for
    #     :param base_uri: base uri to use for the self_uri field (server hostname of DRS broker)
    #     :return: DRS metadata
    #     """
    #
    #     access_methods = []
    #     object_id = f"{record_type}.{record_id}.{result_id}"
    #     result_ids = [result_id, "pipestat_created_time", "pipestat_modified_time"]
    #     record_metadata = self.get_result(
    #         record_type, record_id, result_ids
    #     )  # only get result once
    #     if not record_metadata:
    #         raise RecordNotFoundError("This record does not exist")
    #
    #     if not record_metadata[result_id] or not record_metadata[result_id]["path"]:
    #         raise MissingObjectError("This object does not exist")
    #
    #     path = record_metadata[result_id]["path"]
    #     for access_id in self.config[CFG_ACCESS_METHOD_KEY].keys():
    #         access_dict = AccessMethod(
    #             type=access_id,
    #             access_id=access_id,
    #             access_url=AccessURL(url=self.get_prefixed_uri(path, access_id)),
    #             region=self.config[CFG_ACCESS_METHOD_KEY][access_id].get(
    #                 "region", None
    #             ),
    #         )
    #         access_methods.append(access_dict)
    #     drs_dict = DRSModel(
    #         id=object_id,
    #         self_uri=f"drs://{base_uri}/{object_id}",
    #         size=record_metadata[result_id].get("size", "unknown"),
    #         created_time=record_metadata.get("pipestat_created_time", "unknown"),
    #         updated_time=record_metadata.get("pipestat_modified_time", "unknown"),
    #         checksums=object_id,
    #         access_methods=access_methods,
    #     )
    #
    #     return drs_dict
    #
    # def _read_config_file(self, config_path: str) -> dict:
    #     """
    #     Read configuration file
    #
    #     :param config_path: path to the configuration file
    #     :return: configuration dictionary
    #     """
