import datetime
from typing import List, Optional, Union

from pydantic import BaseModel


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
    size: Union[int, None] = None
    created_time: Optional[datetime.datetime] = None
    updated_time: Optional[datetime.datetime] = None
    checksums: str
    access_methods: List[AccessMethod]
    description: Optional[str] = None
