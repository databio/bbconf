import datetime

from pydantic import BaseModel


# DRS Models
class AccessURL(BaseModel):
    url: str
    headers: dict | None = None


class AccessMethod(BaseModel):
    type: str
    access_url: AccessURL | None = None
    access_id: str | None = None
    region: str | None = None


class DRSModel(BaseModel):
    id: str
    name: str | None = None
    self_uri: str
    size: int | None = None
    created_time: datetime.datetime | None = None
    updated_time: datetime.datetime | None = None
    checksums: str
    access_methods: list[AccessMethod]
    description: str | None = None
