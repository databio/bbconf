import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, model_validator

from .bed_models import BedMetadataBasic


class BedSetStats(BaseModel):
    """Collection-level aggregated statistics for a bedset.

    Replaces old mean/sd of scalar columns with full distribution aggregations.
    """

    n_files: int = 0
    composition: Optional[dict] = None
    scalar_summaries: Optional[dict] = None
    tss_histogram: Optional[dict] = None
    widths_histogram: Optional[dict] = None
    neighbor_distances: Optional[dict] = None
    gc_content: Optional[dict] = None
    region_distribution: Optional[dict] = None
    partitions: Optional[dict] = None
    chromosome_summaries: Optional[dict] = None


class BedSetMetadata(BaseModel):
    id: str
    name: str
    md5sum: str
    submission_date: datetime.datetime = None
    last_update_date: datetime.datetime = None
    statistics: Union[BedSetStats, None] = None
    description: str = None
    summary: str = None
    bed_ids: List[str] = None
    author: Union[str, None] = None
    source: Union[str, None] = None


class BedSetListResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[BedSetMetadata]


class BedSetBedFiles(BaseModel):
    count: int
    results: List[BedMetadataBasic]


class BedSetPEP(BaseModel):
    sample_name: str
    original_name: str
    genome_alias: Union[str, None] = ""
    genome_digest: Union[str, None] = ""
    bed_compliance: Union[str, None] = ""
    data_format: Union[str, None] = ""
    description: Union[str, None] = ""
    url: Union[str, None] = ""

    @model_validator(mode="before")
    def remove_underscore_keys(cls, values):
        """
        Remove keys that start with an underscore, as these values are not sorted by sqlalchemy
        """
        return {k: v for k, v in values.items() if not k.startswith("_")}

    model_config = ConfigDict(extra="allow")
