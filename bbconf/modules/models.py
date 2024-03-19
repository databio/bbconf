import datetime
from typing import Optional, List, Union
import os
import pathlib

from pydantic import BaseModel, ConfigDict, Field

from bbconf.model_parser import yaml_to_pydantic

from bbconf.const_new import (
    DEFAULT_VEC2VEC_MODEL,
    DEFAULT_TEXT2VEC_MODEL,
    DEFAULT_REGION2_VEC_MODEL,
)


class PlotModel(BaseModel):
    name: str = Field(alias="title")
    path: str
    path_thumbnail: Optional[Union[str, None]] = Field(None, alias="thumbnail_path")
    description: Optional[Union[str, None]] = None

    model_config = ConfigDict(populate_by_name=True)


class BedPlots(BaseModel):
    chrombins: PlotModel = None
    gccontent: PlotModel = None
    partitions: PlotModel = None
    expected_partitions: PlotModel = None
    cumulative_partitions: PlotModel = None
    widths_histogram: PlotModel = None
    neighbor_distances: PlotModel = None
    open_chromatin: PlotModel = None


class FileModel(BaseModel):
    name: str = Field(alias="title")
    path: str
    description: Optional[str] = None
    size: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class BedFiles(BaseModel):
    bed_file: FileModel = Field(None, alias="bedfile")
    bigbed_file: FileModel = Field(None, alias="bigbedfile")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="allow",
    )


class BedClassification(BaseModel):
    name: str
    genome_alias: str = None
    genome_digest: str = None
    bed_type: str = Field(
        default="bed3", pattern="^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    )
    bed_format: str = None

    model_config = ConfigDict(extra="ignore")

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

    model_config = ConfigDict(extra="ignore")


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


class BedPEPHub(BaseModel):
    sample_name: str
    genome: str
    organism: str = ""
    species_id: str = ""
    cell_type: str = ""
    cell_line: str = ""
    exp_protocol: str = Field("", description="Experimental protocol (e.g. ChIP-seq)")
    library_source: str = Field(
        "", description="Library source (e.g. genomic, transcriptomic)"
    )
    genotype: str = Field("", description="Genotype of the sample")
    target: str = Field("", description="Target of the assay (e.g. H3K4me3)")
    antibody: str = Field("", description="Antibody used in the assay")
    treatment: str = Field(
        "", description="Treatment of the sample (e.g. drug treatment)"
    )
    tissue: str = Field("", description="Tissue type")
    global_sample_id: str = Field("", description="Global sample identifier")
    global_experiment_id: str = Field("", description="Global experiment identifier")
    description: str = Field("", description="Description of the sample")

    # THIS IS NOW PART OF THE BedBase model in bbconf
    # bed_format: FILE_TYPE = FILE_TYPE.BED
    # bed_type: str = Field(
    #     default="bed3", pattern="^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    # )
