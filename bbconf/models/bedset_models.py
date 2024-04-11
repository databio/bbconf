from typing import List, Union

from pydantic import BaseModel, ConfigDict

from .base_models import FileModel
from .bed_models import BedMetadata, BedStatsModel


class BedSetStats(BaseModel):
    mean: BedStatsModel = None
    sd: BedStatsModel = None


class BedSetPlots(BaseModel):
    region_commonality: FileModel = None

    model_config = ConfigDict(extra="ignore")


class BedSetMetadata(BaseModel):
    id: str
    name: str
    md5sum: str
    statistics: Union[BedSetStats, None] = None
    plots: Union[BedSetPlots, None] = None
    description: str = None
    bed_ids: List[str] = None


class BedSetListResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[BedSetMetadata]


class BedSetBedFiles(BaseModel):
    count: int
    results: List[BedMetadata]
