import datetime
from typing import Optional, Union, List

from pydantic import BaseModel, ConfigDict, Field


from .base_models import FileModel


class BedPlots(BaseModel):
    chrombins: FileModel = None
    gccontent: FileModel = None
    partitions: FileModel = None
    expected_partitions: FileModel = None
    cumulative_partitions: FileModel = None
    widths_histogram: FileModel = None
    neighbor_distances: FileModel = None
    open_chromatin: FileModel = None

    model_config = ConfigDict(extra="ignore")


class BedFiles(BaseModel):
    bed_file: FileModel = Field(None, alias="bedfile")
    bigbed_file: FileModel = Field(None, alias="bigbedfile")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
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
    number_of_regions: Optional[float] = Field(None, alias="regions_no")
    gc_content: Optional[float] = None
    median_tss_dist: Optional[float] = None
    mean_region_width: Optional[float] = None

    exon_frequency: Optional[float] = None
    exon_percentage: Optional[float] = None

    intron_frequency: Optional[float] = None
    intron_percentage: Optional[float] = None

    intergenic_percentage: Optional[float] = None
    intergenic_frequency: Optional[float] = None

    promotercore_frequency: Optional[float] = None
    promotercore_percentage: Optional[float] = None

    fiveutr_frequency: Optional[float] = None
    fiveutr_percentage: Optional[float] = None

    threeutr_frequency: Optional[float] = None
    threeutr_percentage: Optional[float] = None

    promoterprox_frequency: Optional[float] = None
    promoterprox_percentage: Optional[float] = None

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class BedPEPHub(BaseModel):
    sample_name: str
    genome: str = ""
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


class BedMetadata(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    submission_date: datetime.datetime = None
    last_update_date: Optional[datetime.datetime] = None
    stats: Union[BedStats, None] = None
    # classification: BedClassification = None
    plots: Union[BedPlots, None] = None
    files: Union[BedFiles, None] = None
    raw_metadata: Optional[Union[BedPEPHub, None]] = None

    genome_alias: str = None
    genome_digest: str = None
    bed_type: str = Field(
        default="bed3", pattern="^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    )
    bed_format: str = None
    full_response: bool = False


class BedListResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[BedMetadata]


class QdrantSearchResult(BaseModel):
    id: str
    payload: dict
    score: float
    metadata: Union[BedMetadata, None] = None


class BedListSearchResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[QdrantSearchResult] = None
