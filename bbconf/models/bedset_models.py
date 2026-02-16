import datetime

from pydantic import BaseModel, ConfigDict, model_validator

from .base_models import FileModel
from .bed_models import BedMetadataBasic, BedStatsModel


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
    submission_date: datetime.datetime = None
    last_update_date: datetime.datetime = None
    statistics: BedSetStats | None = None
    plots: BedSetPlots | None = None
    description: str = None
    summary: str = None
    bed_ids: list[str] = None
    author: str | None = None
    source: str | None = None


class BedSetListResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: list[BedSetMetadata]


class BedSetBedFiles(BaseModel):
    count: int
    results: list[BedMetadataBasic]


class BedSetPEP(BaseModel):
    sample_name: str
    original_name: str
    genome_alias: str | None = ""
    genome_digest: str | None = ""
    bed_compliance: str | None = ""
    data_format: str | None = ""
    description: str | None = ""
    url: str | None = ""

    @model_validator(mode="before")
    def remove_underscore_keys(cls, values):
        """
        Remove keys that start with an underscore, as these values are not sorted by sqlalchemy
        """
        return {k: v for k, v in values.items() if not k.startswith("_")}

    model_config = ConfigDict(extra="allow")
