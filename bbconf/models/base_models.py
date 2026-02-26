import datetime

from pydantic import BaseModel, ConfigDict, Field

from .drs_models import AccessMethod


class FileModel(BaseModel):
    name: str
    title: str | None = None
    path: str
    file_digest: str | None = None
    path_thumbnail: str | None = Field(None, alias="thumbnail_path")
    description: str | None = None
    size: int | None = None
    object_id: str | None = None
    access_methods: list[AccessMethod] | None = None

    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class StatsReturn(BaseModel):
    bedfiles_number: int = 0
    bedsets_number: int = 0
    genomes_number: int = 0


class UsageStats(BaseModel):
    # file_downloads: Dict[str, int]   # Placeholder for tracking file download statistics in the future.
    bed_metadata: dict[str, int]
    bedset_metadata: dict[str, int]
    bed_search_terms: dict[str, int]
    bedset_search_terms: dict[str, int]
    bed_downloads: dict[str, int]


class UsageModel(BaseModel):
    """
    Usage model. Used to track usage of the bedbase.
    """

    bed_meta: dict[str, int] | None = None
    bedset_meta: dict[str, int] | None = None

    bed_search: dict[str, int] | None = None
    bedset_search: dict[str, int] | None = None
    files: dict[str, int] | None = None

    date_from: datetime.datetime
    date_to: datetime.datetime | None = None


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
    files: list[FileInfo]


class BinValues(BaseModel):
    bins: list[int | float | str]
    counts: list[int]
    mean: float
    median: float


class GEOStatistics(BaseModel):
    """
    GEO statistics for files.
    """

    number_of_files: dict[str, int]
    cumulative_number_of_files: dict[str, int]
    file_sizes: BinValues


class FileStats(BaseModel):
    bed_compliance: dict[str, int]
    data_format: dict[str, int]
    file_genome: dict[str, int]
    file_organism: dict[str, int]
    file_assay: dict[str, int]
    cell_line: dict[str, int]
    geo_status: dict[str, int]
    bed_comments: dict[str, int]
    mean_region_width: BinValues
    file_size: BinValues
    number_of_regions: BinValues
    geo: GEOStatistics
