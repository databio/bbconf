from pydantic import BaseModel
from typing import List

from .bed_models import BedStats
from .base_models import FileModel


class BedSetStats(BaseModel):
    mean: BedStats = None
    sd: BedStats = None


class BedSetMetadata(BaseModel):
    id: str
    name: str
    md5sum: str
    statistics: BedSetStats = None
    plots: List[FileModel] = None
    desciption: str = None
    bed_ids: List[str] = None


class BedSetListResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[BedSetMetadata]
