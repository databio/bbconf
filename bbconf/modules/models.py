import datetime
from typing import Optional, List, Union
import os
import pathlib

from pydantic import BaseModel, ConfigDict, Field

from bbconf.model_parser import yaml_to_pydantic

from bbconf.const_new import DEFAULT_VEC2VEC_MODEL, DEFAULT_TEXT2VEC_MODEL, DEFAULT_REGION2_VEC_MODEL


class Plot(BaseModel):
    name: str = Field(alias="title")
    path: str
    path_thumbnail: Optional[str] = None
    description: Optional[str] = None


class BedPlots(BaseModel):
    chrombins: Plot = None
    gccontent: Plot = None
    partitions: Plot = None
    expected_partitions: Plot = None
    cumulative_partitions: Plot = None
    widths_histogram: Plot = None
    neighbor_distances: Plot = None
    open_chromatin: Plot = None


class Files(BaseModel):
    name: str = Field(alias="title")
    path: str
    description: Optional[str] = None


class BedFiles(BaseModel):
    bed_file: Files = Field(None, alias="bedfile")
    bigbed_file: Files = Field(None, alias="bigbedfile")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )

class BedClassification(BaseModel):
    name: str
    genome_alias: str = None
    genome_digest: str = None
    bed_type: str = None
    bed_format: str = None


class BedStats(BaseModel):
    number_of_regions: Optional[float] = None
    gc_content: Optional[float] = None
    median_tss_dist: Optional[float] = None
    mean_region_width: Optional[float] = None
    exon_frequency: Optional[float] = None
    intron_frequency: Optional[float] = None
    promoterprox_frequency: Optional[float] = None
    intergenic_frequency: Optional[float] = None
    promotercore_frequency: Optional[float] = None
    fiveutr_frequency: Optional[float] = None
    threeutr_frequency: Optional[float] = None
    fiveutr_percentage: Optional[float] = None
    threeutr_percentage: Optional[float] = None
    promoterprox_percentage: Optional[float] = None
    exon_percentage: Optional[float] = None
    intron_percentage: Optional[float] = None
    intergenic_percentage: Optional[float] = None
    promotercore_percentage: Optional[float] = None


class BedMetadata(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    submission_date: datetime.datetime = None
    last_update_date: Optional[datetime.datetime] = None
    stats: BedStats = None
    classification: BedClassification = None
    plots: BedPlots = None
    files: BedFiles = None