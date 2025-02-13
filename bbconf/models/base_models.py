from typing import List, Optional, Union, Dict
import datetime

from pydantic import BaseModel, ConfigDict, Field

from .drs_models import AccessMethod


class FileModel(BaseModel):
    name: str
    title: Optional[str] = None
    path: str
    path_thumbnail: Optional[Union[str, None]] = Field(None, alias="thumbnail_path")
    description: Optional[str] = None
    size: Optional[int] = None
    object_id: Optional[str] = None
    access_methods: List[AccessMethod] = None

    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class StatsReturn(BaseModel):
    bedfiles_number: int = 0
    bedsets_number: int = 0
    genomes_number: int = 0


# class UsageModel(BaseModel):
#     """
#     Usage model. Used to track usage of the bedbase.
#     """
#
#     event: str
#     bed_id: Optional[Union[str, None]] = None
#     bedset_id: Optional[Union[str, None]] = None
#     query: Optional[Union[str, None]] = None
#     file_name: Optional[Union[str, None]] = None
#
#     ipaddress: str
#     user_agent: str


class UsageModel(BaseModel):
    """
    Usage model. Used to track usage of the bedbase.
    """

    bed_meta: Union[dict, None] = Dict[str, int]
    bedset_meta: Union[dict, None] = Dict[str, int]

    bed_search: Union[dict, None] = Dict[str, int]
    bedset_search: Union[dict, None] = Dict[str, int]
    files: Union[dict, None] = Dict[str, int]

    date_from: datetime.datetime
    date_to: Union[datetime.datetime, None] = None
