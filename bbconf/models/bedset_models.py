from pydantic import BaseModel, ConfigDict
from typing import List

from .bed_models import BedStats, BedMetadata
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
    description: str = None
    bed_ids: List[str] = None


class BedSetListResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[BedSetMetadata]


class BedSetBedFiles(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[BedMetadata]


class BedSetPlots(BaseModel):
    region_commonality: FileModel = None

    model_config = ConfigDict(extra="ignore")
