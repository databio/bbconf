import datetime

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
    bed_file: FileModel | None = None
    bigbed_file: FileModel | None = None

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )


class BedClassification(BaseModel):
    name: str | None = None
    genome_alias: str | None = None
    genome_digest: str | None = None
    bed_compliance: str = Field(
        default="bed3", pattern=r"^bed(?:[3-9]|1[0-5])(?:\+|$)[0-9]?+$"
    )
    data_format: str | None = None
    compliant_columns: int = 3
    non_compliant_columns: int = 0

    header: str | None = None  # Header of the bed file (if any)

    model_config = ConfigDict(extra="ignore")


class BedStatsModel(BaseModel):
    number_of_regions: float | None = None
    gc_content: float | None = None
    median_tss_dist: float | None = None
    mean_region_width: float | None = None

    exon_frequency: float | None = None
    exon_percentage: float | None = None

    intron_frequency: float | None = None
    intron_percentage: float | None = None

    intergenic_percentage: float | None = None
    intergenic_frequency: float | None = None

    promotercore_frequency: float | None = None
    promotercore_percentage: float | None = None

    fiveutr_frequency: float | None = None
    fiveutr_percentage: float | None = None

    threeutr_frequency: float | None = None
    threeutr_percentage: float | None = None

    promoterprox_frequency: float | None = None
    promoterprox_percentage: float | None = None

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
    description: str | None = ""

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

    global_sample_id: list[str] | None = Field(
        "", description="Global sample identifier. e.g. GSM000"
    )  # excluded in training
    global_experiment_id: list[str] | None = Field(
        "", description="Global experiment identifier. e.g. GSE000"
    )  # excluded in training

    original_file_name: str = Field("", description="Original file name")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
        alias_generator=None,
    )

    @field_validator("global_sample_id", "global_experiment_id", mode="before")
    def ensure_list(cls, v: str | list[str]) -> list[str]:
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
    name: str | None = ""
    description: str | None = None
    submission_date: datetime.datetime | None = None
    last_update_date: datetime.datetime | None = None
    is_universe: bool | None = False
    license_id: str | None = DEFAULT_LICENSE
    annotation: StandardMeta | None = None
    processed: bool | None = True


class UniverseMetadata(BaseModel):
    construct_method: str | None = None
    bedset_id: str | None = None


class BedSetMinimal(BaseModel):
    id: str
    name: str | None = None
    description: str | None = None


class BedMetadataAll(BedMetadataBasic):
    stats: BedStatsModel | None = None
    plots: BedPlots | None = None
    files: BedFiles | None = None
    universe_metadata: UniverseMetadata | None = None
    raw_metadata: BedPEPHub | BedPEPHubRestrict | None = None
    bedsets: list[BedSetMinimal] | None = None


class BedListResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: list[BedMetadataBasic]


class QdrantSearchResult(BaseModel):
    id: str
    payload: dict = None
    score: float = None
    metadata: BedMetadataBasic | None = None


class BedListSearchResult(BaseModel):
    count: int
    limit: int
    offset: int
    results: list[QdrantSearchResult] | None = None


class TokenizedBedResponse(BaseModel):
    universe_id: str
    bed_id: str
    tokenized_bed: list[int]


class BedEmbeddingResult(BaseModel):
    identifier: str
    payload: dict
    embedding: list[float]


class TokenizedPathResponse(BaseModel):
    bed_id: str
    universe_id: str
    file_path: str
    endpoint_url: str


class RefGenValidModel(BaseModel):
    provided_genome: str
    compared_genome: str | None
    genome_digest: str | None
    xs: float = 0.0
    oobr: float | None = None
    sequence_fit: float | None = None
    assigned_points: int
    tier_ranking: int

    model_config = ConfigDict(extra="forbid")


class RefGenValidReturnModel(BaseModel):
    id: str
    provided_genome: str | None = None
    compared_genome: list[RefGenValidModel]


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
    genome_digest: str | None = None
    species_name: str
    # summary: str
    # global_sample_id: str
    # original_file_name: str
    # embedding: List[float]
