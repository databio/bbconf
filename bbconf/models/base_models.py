from typing import List, Optional, Union

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
