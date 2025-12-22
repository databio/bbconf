import datetime
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from .drs_models import AccessMethod


class FileModel(BaseModel):
    name: str
    title: Optional[str] = None
    path: str
    file_digest: Optional[str] = None
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


class UsageStats(BaseModel):
    # file_downloads: Dict[str, int]   # Placeholder for tracking file download statistics in the future.
    bed_metadata: Dict[str, int]
    bedset_metadata: Dict[str, int]
    bed_search_terms: Dict[str, int]
    bedset_search_terms: Dict[str, int]
    bed_downloads: Dict[str, int]


class UsageModel(BaseModel):
    """
    Usage model. Used to track usage of the bedbase.
    """

    bed_meta: Union[dict, None] = Dict[str, int]
    bedset_meta: Union[dict, None] = Dict[str, int]

    bed_search: Union[dict, None] = Dict[str, int]
    bedset_search: Union[dict, None] = Dict[str, int]
    files: Union[dict, None] = Dict[str, int]

    date_from: datetime.datetime
    date_to: Union[datetime.datetime, None] = None


class FileInfo(BaseModel):
    """
    Main information about a file used for BEDbase verse statistics.
    """

    id: str
    bed_compliance: str
    data_format: str
    mean_region_width: float
    file_size: int
    number_of_regions: int


class AllFilesInfo(BaseModel):
    """
    Information about all files. e.g. file sizes, mean region width, etc.

    """

    total: int
    files: List[FileInfo]


class BinValues(BaseModel):
    bins: List[Union[int, float, str]]
    counts: List[int]
    mean: float
    median: float


class GEOStatistics(BaseModel):
    """
    GEO statistics for files.
    """

    number_of_files: Dict[str, int]
    cumulative_number_of_files: Dict[str, int]
    file_sizes: BinValues


class FileStats(BaseModel):
    bed_compliance: Dict[str, int]
    data_format: Dict[str, int]
    file_genome: Dict[str, int]
    file_organism: Dict[str, int]
    file_assay: Dict[str, int]
    geo_status: Dict[str, int]
    bed_comments: Dict[str, int]
    mean_region_width: BinValues
    file_size: BinValues
    number_of_regions: BinValues
    geo: GEOStatistics
