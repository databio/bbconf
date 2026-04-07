import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, model_validator

from .base_models import FileModel
from .bed_models import BedMetadataBasic, BedStatsModel


class BedSetStats(BaseModel):
    """Bedset statistics: mean/sd of scalar columns.

    Populated from bedset_means and bedset_standard_deviation database columns.
    """

    mean: BedStatsModel = None
    sd: BedStatsModel = None


class BedSetDistributions(BaseModel):
    """Collection-level aggregated distribution statistics for a bedset.

    Stored in the bedset_stats JSONB database column. Populated when
    member bed files have been processed with the gtars analysis backend.

    Only distributions that are meaningful at collection-level are kept:
    - scalar_summaries: mean ± sd + 25-bin histograms of per-file scalar values
    - tss_histogram: summed per-bin counts across files (fixed ±100 kb axis)
    - region_distribution: per-chrom bin-wise mean ± sd across files
      (requires gtars ≥ PR #248 for reference-aligned bin widths)
    - partitions: mean ± sd of per-file partition percentages

    Dropped (retained in per-file distributions blob, not aggregated):
    - widths_histogram: per-file variable-range bins aren't summable; use
      scalar_summaries.mean_region_width histogram instead
    - neighbor_distances KDE: per-file within-bedset variance is low; use
      scalar_summaries.median_neighbor_distance instead
    - gc_content KDE: per-file distribution is unimodal; use
      scalar_summaries.gc_content mean instead
    - chromosome_summaries: redundant with region_distribution
    - expected_partitions: per-file null hypothesis, not collection property
    """

    n_files: int = 0
    composition: Optional[dict] = None
    scalar_summaries: Optional[dict] = None
    tss_histogram: Optional[dict] = None
    region_distribution: Optional[dict] = None
    partitions: Optional[dict] = None


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
    distributions: BedSetDistributions | None = None
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
