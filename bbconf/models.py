import datetime
from typing import Optional, List, Union
import os
import pathlib

from pydantic import BaseModel, ConfigDict

from bbconf.model_parser import yaml_to_pydantic

from bbconf.const_new import DEFAULT_VEC2VEC_MODEL, DEFAULT_TEXT2VEC_MODEL, DEFAULT_REGION2_VEC_MODEL


# DRS Models
class AccessURL(BaseModel):
    url: str
    headers: Optional[dict] = None


class AccessMethod(BaseModel):
    type: str
    access_url: Optional[AccessURL] = None
    access_id: Optional[str] = None
    region: Optional[str] = None


class DRSModel(BaseModel):
    id: str
    name: Optional[str] = None
    self_uri: str
    size: str
    created_time: Optional[datetime.datetime] = None
    updated_time: Optional[datetime.datetime] = None
    checksums: str
    access_methods: List[AccessMethod]
    description: Optional[str] = None


BedFileTableModel = yaml_to_pydantic(
    "BedFile",
    os.path.join(
        pathlib.Path(__file__).parent.resolve(), "schemas", "bedfiles_schema.yaml"
    ),
)
BedSetTableModel = yaml_to_pydantic(
    "BedSet",
    os.path.join(
        pathlib.Path(__file__).parent.resolve(), "schemas", "bedsets_schema.yaml"
    ),
)
