import datetime
from typing import Optional, List

from pydantic import BaseModel, Field


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
    name: Optional[str]
    self_uri: str
    size: str
    created_time: Optional[datetime.datetime]
    updated_time: Optional[datetime.datetime]
    checksums: str
    access_methods: List[AccessMethod]
    description: Optional[str] = None
