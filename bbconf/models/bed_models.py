import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from bbconf.const import DEFAULT_LICENSE

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
    tss_distance: FileModel = None

    model_config = ConfigDict(extra="ignore")


class BedFiles(BaseModel):
    bed_file: Union[FileModel, None] = None
    bigbed_file: Union[FileModel, None] = None

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )


class BedClassification(BaseModel):
    name: Optional[str] = None
    genome_alias: str = None
    genome_digest: Union[str, None] = None
    bed_compliance: str = Field(
        default="bed3", pattern=r"^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    )
    data_format: Union[str, None] = None
    compliant_columns: int = 3
    non_compliant_columns: int = 0

    header: Union[str, None] = None  # Header of the bed file (if any)

    model_config = ConfigDict(extra="ignore")


class BedStatsModel(BaseModel):
    number_of_regions: Optional[float] = None
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
    sample_name: str = ""
    genome: str = ""
    organism: str = ""
    species_id: str = ""
    cell_type: str = ""
    cell_line: str = ""
    assay: str = Field("", description="Experimental protocol (e.g. ChIP-seq)")
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

    model_config = ConfigDict(extra="allow", populate_by_name=True)


class StandardMeta(BaseModel):
    """
    Standardized Bed file metadata
    """

    species_name: str = Field(
        default="", description="Name of species. e.g. Homo sapiens.", alias="organism"
    )
    species_id: str = ""
    genotype: str = Field("", description="Genotype of the sample")
    phenotype: str = Field("", description="Phenotype of the sample")
    description: Union[str, None] = ""

    cell_type: str = Field(
        "",
        description="specific kind of cell with distinct characteristics found in an organism. e.g. Neurons, Hepatocytes, Adipocytes",
    )
    cell_line: str = Field(
        "",
        description="population of cells derived from a single cell and cultured in the lab for extended use, e.g. HeLa, HepG2, k562",
    )
    tissue: str = Field("", description="Tissue type")

    library_source: str = Field(
        "", description="Library source (e.g. genomic, transcriptomic)"
    )
    assay: str = Field(
        "",
        description="Experimental protocol (e.g. ChIP-seq)",
    )
    antibody: str = Field("", description="Antibody used in the assay")
    target: str = Field("", description="Target of the assay (e.g. H3K4me3)")
    treatment: str = Field(
        "", description="Treatment of the sample (e.g. drug treatment)"
    )

    global_sample_id: Union[List[str], None] = Field(
        "", description="Global sample identifier. e.g. GSM000"
    )  # excluded in training
    global_experiment_id: Union[List[str], None] = Field(
        "", description="Global experiment identifier. e.g. GSE000"
    )  # excluded in training

    original_file_name: str = Field("", description="Original file name")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        alias_generator=None,
    )

    @field_validator("global_sample_id", "global_experiment_id", mode="before")
    def ensure_list(cls, v: Union[str, List[str]]) -> List[str]:
        if isinstance(v, str):
            return [v]
        elif isinstance(v, list):
            return v
        elif isinstance(v, type(None)):
            return []
        raise ValueError("values must be a string or a list of strings")


class BedPEPHubRestrict(BedPEPHub):

    model_config = ConfigDict(extra="ignore")


class BedMetadataBasic(BedClassification):
    id: str
    name: Optional[Union[str, None]] = ""
    description: Optional[str] = None
    submission_date: datetime.datetime = None
    last_update_date: Optional[datetime.datetime] = None
    is_universe: Optional[bool] = False
    license_id: Optional[str] = DEFAULT_LICENSE
    annotation: Optional[StandardMeta] = None
    processed: Optional[bool] = True


class UniverseMetadata(BaseModel):
    construct_method: Union[str, None] = None
    bedset_id: Union[str, None] = None


class BedSetMinimal(BaseModel):
    id: str
    name: Union[str, None] = None
    description: Union[str, None] = None


class BedMetadataAll(BedMetadataBasic):
    stats: Union[BedStatsModel, None] = None
    plots: Union[BedPlots, None] = None
    files: Union[BedFiles, None] = None
    universe_metadata: Union[UniverseMetadata, None] = None
    raw_metadata: Union[BedPEPHub, BedPEPHubRestrict, None] = None
    bedsets: Union[List[BedSetMinimal], None] = None


class BedListResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[BedMetadataBasic]


class QdrantSearchResult(BaseModel):
    id: str
    payload: dict = None
    score: float = None
    metadata: Union[BedMetadataBasic, None] = None


class BedListSearchResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: List[QdrantSearchResult] = None


class TokenizedBedResponse(BaseModel):
    universe_id: str
    bed_id: str
    tokenized_bed: List[int]


class BedEmbeddingResult(BaseModel):
    identifier: str
    payload: dict
    embedding: List[float]


class TokenizedPathResponse(BaseModel):
    bed_id: str
    universe_id: str
    file_path: str
    endpoint_url: str


class RefGenValidModel(BaseModel):
    provided_genome: str
    compared_genome: Union[str, None]
    genome_digest: Union[str, None]
    xs: float = 0.0
    oobr: Union[float, None] = None
    sequence_fit: Union[float, None] = None
    assigned_points: int
    tier_ranking: int

    model_config = ConfigDict(extra="forbid")


class RefGenValidReturnModel(BaseModel):
    id: str
    provided_genome: Union[str, None] = None
    compared_genome: List[RefGenValidModel]


class VectorMetadata(BaseModel):
    id: str
    name: str
    description: str
    cell_line: str
    cell_type: str
    tissue: str
    target: str
    treatment: str
    assay: str
    genome_alias: str
    genome_digest: Union[str, None] = None
    species_name: str
    # summary: str
    # global_sample_id: str
    # original_file_name: str
    # embedding: List[float]
