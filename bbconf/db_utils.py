import datetime
import logging
import os
from typing import Optional

import pandas as pd
from alembic import command
from alembic.config import Config
from sqlalchemy import (
    DDL,
    TIMESTAMP,
    BigInteger,
    ForeignKey,
    Index,
    Result,
    Select,
    String,
    UniqueConstraint,
    event,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSON, JSONB
from sqlalchemy.engine import URL, Engine, create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship
from sqlalchemy_schemadisplay import create_schema_graph

from bbconf.const import LICENSES_CSV_URL, PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


POSTGRES_DIALECT = "postgresql+psycopg"

# tables, that were created in this execution
tables_initialized: list = []


class SchemaError(Exception):
    def __init__(self):
        super().__init__(
            """
            The database schema is incorrect, can't connect to the database!"""
        )


class BIGSERIAL(BigInteger):
    pass


@compiles(BIGSERIAL, POSTGRES_DIALECT)
def compile_bigserial_pg(type_, compiler, **kw):
    return "BIGSERIAL"


@compiles(JSON, POSTGRES_DIALECT)
def compile_jsonb_pg(type_, compiler, **kw):
    return "JSONB"


@compiles(ARRAY, POSTGRES_DIALECT)
def compile_array_pg(type_, compiler, **kw):
    return "ARRAY"


class Base(DeclarativeBase):
    type_annotation_map = {datetime.datetime: TIMESTAMP(timezone=True)}


@event.listens_for(Base.metadata, "after_create")
def receive_after_create(target, connection, tables, **kw):
    """
    listen for the 'after_create' event
    """
    global tables_initialized
    if tables:
        _LOGGER.info("A table was created")
        tables_initialized = [name.fullname for name in tables]
    else:
        _LOGGER.info("A table was not created")


def deliver_update_date(context):
    return datetime.datetime.now(datetime.timezone.utc)


class Bed(Base):
    __tablename__ = "bed"

    id: Mapped[str] = mapped_column(primary_key=True, index=True)
    name: Mapped[Optional[str]]
    genome_alias: Mapped[Optional[str]]
    genome_digest: Mapped[Optional[str]]
    description: Mapped[Optional[str]]
    bed_compliance: Mapped[str] = mapped_column(default="bed3+0")
    data_format: Mapped[str] = mapped_column(default="bed_like")
    compliant_columns: Mapped[int] = mapped_column(default=3)
    non_compliant_columns: Mapped[int] = mapped_column(default=0)

    header: Mapped[Optional[str]] = mapped_column(
        nullable=True, comment="Header of the bed file, it if was provided."
    )

    indexed: Mapped[bool] = mapped_column(
        default=False, comment="Whether sample was added to qdrant"
    )
    file_indexed: Mapped[bool] = mapped_column(
        default=False,
        comment="Whether file was tokenized and added to the vector database",
    )

    submission_date: Mapped[datetime.datetime] = mapped_column(
        default=deliver_update_date
    )
    last_update_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_update_date,
        onupdate=deliver_update_date,
    )
    is_universe: Mapped[Optional[bool]] = mapped_column(default=False)

    files: Mapped[list["Files"]] = relationship(
        "Files", back_populates="bedfile", cascade="all, delete-orphan"
    )

    bedsets: Mapped[list["BedFileBedSetRelation"]] = relationship(
        "BedFileBedSetRelation", back_populates="bedfile", cascade="all, delete-orphan"
    )

    annotations: Mapped["BedMetadata"] = relationship(
        back_populates="bed", cascade="all, delete-orphan", lazy="joined"
    )

    stats: Mapped["BedStats"] = relationship(
        back_populates="bed", cascade="all, delete-orphan"
    )

    universe: Mapped["Universes"] = relationship(
        back_populates="bed", cascade="all, delete-orphan"
    )
    tokenized: Mapped["TokenizedBed"] = relationship(
        back_populates="bed", cascade="all, delete-orphan"
    )
    license_id: Mapped["str"] = mapped_column(
        ForeignKey("licenses.id", ondelete="CASCADE"), nullable=True, index=True
    )
    license_mapping: Mapped["License"] = relationship("License", back_populates="bed")

    ref_classifier: Mapped[list["GenomeRefStats"]] = relationship(
        "GenomeRefStats", back_populates="bed", cascade="all, delete-orphan"
    )
    processed: Mapped[bool] = mapped_column(
        default=False, comment="Whether the bed file was processed"
    )

    __table_args__ = (
        # Backs the genome filter (Bed.genome_alias == genome) and the
        # GROUP BY genome_alias aggregations. Historically created by hand in the
        # live DB; declared here so a fresh create_all() reproduces it exactly
        # (name and btree deduplication included).
        Index(
            "genome_alias_index",
            "genome_alias",
            postgresql_with={"deduplicate_items": "true"},
        ),
        # Backs get_recent_beds / list_beds(order_by="submission_date"):
        # ORDER BY submission_date DESC, id ASC LIMIT n.
        Index("ix_bed_submission_date", text("submission_date DESC"), text("id")),
        # Partial indexes for the background-worker backlog scans. They stay
        # small and get faster as each queue drains toward empty.
        Index("ix_bed_unprocessed", "id", postgresql_where=text("processed = false")),
        Index("ix_bed_not_indexed", "id", postgresql_where=text("indexed = false")),
        Index(
            "ix_bed_not_file_indexed",
            "id",
            postgresql_where=text("file_indexed = false"),
        ),
    )


class BedMetadata(Base):
    __tablename__ = "bed_metadata"

    species_name: Mapped[str] = mapped_column(default=None, comment="Organism name")
    species_id: Mapped[str] = mapped_column(
        default=None, nullable=True, comment="Organism taxon id"
    )

    genotype: Mapped[str] = mapped_column(
        default=None, nullable=True, comment="Genotype of the sample"
    )
    phenotype: Mapped[str] = mapped_column(
        default=None, nullable=True, comment="Phenotype of the sample"
    )

    cell_type: Mapped[str] = mapped_column(
        default=None,
        nullable=True,
        comment="Specific kind of cell with distinct characteristics found in an organism. e.g. Neurons, Hepatocytes, Adipocytes",
    )
    cell_line: Mapped[str] = mapped_column(
        default=None,
        nullable=True,
        comment="Population of cells derived from a single cell and cultured in the lab for extended use, e.g. HeLa, HepG2, k562",
    )
    tissue: Mapped[str] = mapped_column(
        default=None, nullable=True, comment="Tissue type"
    )
    library_source: Mapped[str] = mapped_column(
        default=None,
        nullable=True,
        comment="Library source (e.g. genomic, transcriptomic)",
    )
    assay: Mapped[str] = mapped_column(
        default=None, nullable=True, comment="Experimental protocol (e.g. ChIP-seq)"
    )
    antibody: Mapped[str] = mapped_column(
        default=None, nullable=True, comment="Antibody used in the assay"
    )
    target: Mapped[str] = mapped_column(
        default=None, nullable=True, comment="Target of the assay (e.g. H3K4me3)"
    )
    treatment: Mapped[str] = mapped_column(
        default=None,
        nullable=True,
        comment="Treatment of the sample (e.g. drug treatment)",
    )

    original_file_name: Mapped[Optional[str]] = mapped_column(
        nullable=True, comment="Original file name"
    )

    global_sample_id: Mapped[list] = mapped_column(
        ARRAY(String),
        default=None,
        nullable=True,
        comment="Global sample identifier. e.g. GSM000",
    )
    global_experiment_id: Mapped[list] = mapped_column(
        ARRAY(String),
        default=None,
        nullable=True,
        comment="Global experiment identifier. e.g. GSE000",
    )

    id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )

    bed: Mapped["Bed"] = relationship("Bed", back_populates="annotations")


class BedStats(Base):
    __tablename__ = "bed_stats"

    id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    number_of_regions: Mapped[Optional[float]]
    gc_content: Mapped[Optional[float]]
    median_tss_dist: Mapped[Optional[float]]
    mean_region_width: Mapped[Optional[float]]
    exon_frequency: Mapped[Optional[float]]
    intron_frequency: Mapped[Optional[float]]
    promoterprox_frequency: Mapped[Optional[float]]
    intergenic_frequency: Mapped[Optional[float]]
    promotercore_frequency: Mapped[Optional[float]]
    fiveutr_frequency: Mapped[Optional[float]]
    threeutr_frequency: Mapped[Optional[float]]
    fiveutr_percentage: Mapped[Optional[float]]
    threeutr_percentage: Mapped[Optional[float]]
    promoterprox_percentage: Mapped[Optional[float]]
    exon_percentage: Mapped[Optional[float]]
    intron_percentage: Mapped[Optional[float]]
    intergenic_percentage: Mapped[Optional[float]]
    promotercore_percentage: Mapped[Optional[float]]
    tssdist: Mapped[Optional[float]]

    distributions: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Full distribution arrays from gtars genomicdist (JSONB)",
    )

    bed: Mapped["Bed"] = relationship("Bed", back_populates="stats")

    __table_args__ = (
        # Backs the "beds missing computed stats" worker scan.
        Index(
            "ix_bed_stats_missing_regions",
            "id",
            postgresql_where=text("number_of_regions IS NULL"),
        ),
    )


class Files(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(
        nullable=False, comment="Name of the file, e.g. bed, bigBed"
    )
    file_digest: Mapped[str] = mapped_column(
        nullable=True, comment="Digest of the file. Mainly used for bed file."
    )
    title: Mapped[Optional[str]]
    type: Mapped[str] = mapped_column(
        default="file", comment="Type of the object, e.g. file, plot, ..."
    )
    path: Mapped[str]
    path_thumbnail: Mapped[str] = mapped_column(
        nullable=True, comment="Thumbnail path of the file"
    )
    description: Mapped[Optional[str]]
    size: Mapped[Optional[int]] = mapped_column(default=0, comment="Size of the file")

    bedfile_id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"), nullable=True, index=True
    )
    bedset_id: Mapped[str] = mapped_column(
        ForeignKey("bedsets.id", ondelete="CASCADE"), nullable=True, index=True
    )

    bedfile: Mapped["Bed"] = relationship("Bed", back_populates="files")
    bedset: Mapped["BedSets"] = relationship("BedSets", back_populates="files")

    __table_args__ = (
        UniqueConstraint(
            "name", "bedfile_id"
        ),  # TODO: add in the future file_digest here
        UniqueConstraint("name", "bedset_id"),
    )


class BedFileBedSetRelation(Base):
    __tablename__ = "bedfile_bedset_relation"

    bedset_id: Mapped[str] = mapped_column(
        ForeignKey("bedsets.id", ondelete="CASCADE"), primary_key=True
    )
    bedfile_id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"), primary_key=True, index=True
    )

    bedset: Mapped["BedSets"] = relationship("BedSets", back_populates="bedfiles")
    bedfile: Mapped["Bed"] = relationship("Bed", back_populates="bedsets")


class BedSets(Base):
    __tablename__ = "bedsets"

    id: Mapped[str] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(nullable=False, comment="Name of the bedset")
    description: Mapped[Optional[str]] = mapped_column(
        comment="Description of the bedset"
    )
    summary: Mapped[Optional[str]] = mapped_column(
        nullable=True, comment="Summary of the bedset"
    )
    submission_date: Mapped[datetime.datetime] = mapped_column(
        default=deliver_update_date
    )
    last_update_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_update_date,
        onupdate=deliver_update_date,
    )
    md5sum: Mapped[Optional[str]] = mapped_column(comment="MD5 sum of the bedset")

    bedset_means: Mapped[Optional[dict]] = mapped_column(
        JSON, comment="Mean values of the bedset"
    )
    bedset_standard_deviation: Mapped[Optional[dict]] = mapped_column(
        JSON, comment="Median values of the bedset"
    )
    bedset_stats: Mapped[Optional[dict]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Pre-aggregated distribution statistics from gtars (JSONB)",
    )

    bedfile_count: Mapped[int] = mapped_column(
        default=0, comment="Number of bedfiles in the bedset (denormalized count)"
    )

    bedfiles: Mapped[list["BedFileBedSetRelation"]] = relationship(
        "BedFileBedSetRelation", back_populates="bedset", cascade="all, delete-orphan"
    )
    files: Mapped[list["Files"]] = relationship("Files", back_populates="bedset")
    universe: Mapped["Universes"] = relationship("Universes", back_populates="bedset")

    author: Mapped[str] = mapped_column(nullable=True, comment="Author of the bedset")
    source: Mapped[str] = mapped_column(nullable=True, comment="Source of the bedset")

    processed: Mapped[bool] = mapped_column(
        default=False, comment="Whether the bedset was processed"
    )

    __table_args__ = (
        # Backs the "unprocessed bedsets" worker scan.
        Index(
            "ix_bedsets_unprocessed", "id", postgresql_where=text("processed = false")
        ),
        # Trigram GIN indexes for the bedset search (get_ids_list), which filters
        # on name/description with ILIKE '%query%'. A leading-wildcard ILIKE
        # cannot use a btree index at all, so pg_trgm is the only thing that
        # avoids a full table scan here. Needs the pg_trgm extension, which the
        # before_create listener below creates on first creation of this table.
        Index(
            "ix_bedsets_name_trgm",
            "name",
            postgresql_using="gin",
            postgresql_ops={"name": "gin_trgm_ops"},
        ),
        Index(
            "ix_bedsets_description_trgm",
            "description",
            postgresql_using="gin",
            postgresql_ops={"description": "gin_trgm_ops"},
        ),
    )


# Make the pg_trgm extension available before the bedsets trigram GIN indexes are
# built. Scoped to this table's creation so it runs only on first-time schema
# creation (not on every startup) and only on PostgreSQL.
event.listen(
    BedSets.__table__,
    "before_create",
    DDL("CREATE EXTENSION IF NOT EXISTS pg_trgm").execute_if(dialect="postgresql"),
)


class Universes(Base):
    __tablename__ = "universes"

    id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
    )
    method: Mapped[str] = mapped_column(
        nullable=True, comment="Method used to create the universe"
    )
    bedset_id: Mapped[str] = mapped_column(
        ForeignKey("bedsets.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )

    bed: Mapped["Bed"] = relationship("Bed", back_populates="universe")
    bedset: Mapped["BedSets"] = relationship("BedSets", back_populates="universe")
    tokenized: Mapped["TokenizedBed"] = relationship(
        "TokenizedBed",
        back_populates="universe",
    )


class TokenizedBed(Base):
    __tablename__ = "tokenized_bed"

    bed_id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
        nullable=False,
    )
    universe_id: Mapped[str] = mapped_column(
        ForeignKey("universes.id", ondelete="CASCADE"),
        primary_key=True,
        index=True,
        nullable=False,
    )
    path: Mapped[str] = mapped_column(
        nullable=False, comment="Path to the tokenized bed file"
    )

    bed: Mapped["Bed"] = relationship("Bed", back_populates="tokenized")
    universe: Mapped["Universes"] = relationship(
        "Universes",
        back_populates="tokenized",
        passive_deletes="all",
    )


class License(Base):
    __tablename__ = "licenses"

    id: Mapped[str] = mapped_column(primary_key=True, index=True)
    shorthand: Mapped[str] = mapped_column(nullable=True, comment="License shorthand")
    label: Mapped[str] = mapped_column(nullable=False, comment="License label")
    description: Mapped[str] = mapped_column(
        nullable=False, comment="License description"
    )

    bed: Mapped[list["Bed"]] = relationship("Bed", back_populates="license_mapping")


class ReferenceGenome(Base):
    __tablename__ = "reference_genomes"

    digest: Mapped[str] = mapped_column(primary_key=True, index=True)
    alias: Mapped[str] = mapped_column(
        nullable=False, comment="Name of the reference genome"
    )

    bed_reference: Mapped[list["GenomeRefStats"]] = relationship(
        "GenomeRefStats",
        back_populates="genome_object",
        cascade="all, delete-orphan",
    )


class GenomeRefStats(Base):
    __tablename__ = "genome_ref_stats"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)

    bed_id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    provided_genome: Mapped[str]
    compared_genome: Mapped[str] = mapped_column(
        nullable=False, comment="Compared Genome"
    )
    genome_digest: Mapped[str] = mapped_column(
        ForeignKey("reference_genomes.digest", ondelete="CASCADE"),
    )

    xs: Mapped[float] = mapped_column(nullable=True, default=None)
    oobr: Mapped[float] = mapped_column(nullable=True, default=None)
    sequence_fit: Mapped[float] = mapped_column(nullable=True, default=None)
    assigned_points: Mapped[int] = mapped_column(nullable=False)
    tier_ranking: Mapped[int] = mapped_column(nullable=False)

    bed: Mapped["Bed"] = relationship("Bed", back_populates="ref_classifier")

    genome_object: Mapped["ReferenceGenome"] = relationship(
        "ReferenceGenome",
        back_populates="bed_reference",
        lazy="joined",
    )

    __table_args__ = (UniqueConstraint("bed_id", "compared_genome"),)


@listens_for(Universes, "after_insert")
@listens_for(Universes, "after_update")
def add_bed_universe(mapper, connection, target):
    with Session(connection) as session:
        bed = session.scalar(select(Bed).where(Bed.id == target.id))
        bed.is_universe = True
        session.commit()


@listens_for(Universes, "after_delete")
def delete_bed_universe(mapper, connection, target):
    with Session(connection) as session:
        bed = session.scalar(select(Bed).where(Bed.id == target.id))
        bed.is_universe = False
        session.commit()


class GeoGseStatus(Base):
    __tablename__ = "geo_gse_status"

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)
    gse: Mapped[str] = mapped_column(nullable=False, comment="GSE number", unique=True)
    status: Mapped[str] = mapped_column(
        nullable=False, comment="Status of the GEO project"
    )
    submission_date: Mapped[datetime.datetime] = mapped_column(
        default=deliver_update_date, onupdate=deliver_update_date
    )
    number_of_files: Mapped[int] = mapped_column(default=0, comment="Number of files")
    number_of_success: Mapped[int] = mapped_column(
        default=0, comment="Number of success"
    )
    number_of_skips: Mapped[int] = mapped_column(default=0, comment="Number of skips")
    number_of_fails: Mapped[int] = mapped_column(default=0, comment="Number of fails")

    gsm_status_mapper: Mapped[list["GeoGsmStatus"]] = relationship(
        "GeoGsmStatus", back_populates="gse_status_mapper"
    )
    error: Mapped[str] = mapped_column(nullable=True, comment="Error message")


class GeoGsmStatus(Base):
    __tablename__ = "geo_gsm_status"

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)
    gse_status_id: Mapped[str] = mapped_column(
        ForeignKey("geo_gse_status.id", ondelete="CASCADE"), nullable=False, index=True
    )
    gsm: Mapped[str] = mapped_column(nullable=False, comment="GSM number", unique=False)
    sample_name: Mapped[str] = mapped_column()
    status: Mapped[str] = mapped_column(
        nullable=False, comment="Status of the GEO sample"
    )
    error: Mapped[str] = mapped_column(nullable=True, comment="Error message")

    source_submission_date: Mapped[datetime.datetime] = mapped_column(
        nullable=True, comment="Submission date of the source"
    )

    submission_date: Mapped[datetime.datetime] = mapped_column(
        default=deliver_update_date, onupdate=deliver_update_date
    )
    bed_id: Mapped[str] = mapped_column(
        nullable=True, index=True, comment="Bed identifier"
    )

    file_size: Mapped[int] = mapped_column(
        BigInteger, default=0, comment="Size of the file"
    )
    genome: Mapped[str] = mapped_column(nullable=True, comment="Genome")

    gse_status_mapper: Mapped["GeoGseStatus"] = relationship(
        "GeoGseStatus", back_populates="gsm_status_mapper"
    )


class UsageBedMeta(Base):
    __tablename__ = "usage_bed_meta"

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)

    bed_id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"), nullable=True, index=True
    )

    count: Mapped[int] = mapped_column(default=0, comment="Number of visits")

    date_from: Mapped[datetime.datetime] = mapped_column(comment="Date from")
    date_to: Mapped[datetime.datetime] = mapped_column(comment="Date to")


class UsageBedSetMeta(Base):
    __tablename__ = "usage_bedset_meta"

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)

    bedset_id: Mapped[str] = mapped_column(
        ForeignKey("bedsets.id", ondelete="CASCADE"), nullable=True, index=True
    )
    count: Mapped[int] = mapped_column(default=0, comment="Number of visits")

    date_from: Mapped[datetime.datetime] = mapped_column(comment="Date from")
    date_to: Mapped[datetime.datetime] = mapped_column(comment="Date to")


class UsageFiles(Base):
    __tablename__ = "usage_files"

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)
    file_path: Mapped[str] = mapped_column(nullable=False, comment="Path to the file")
    count: Mapped[int] = mapped_column(default=0, comment="Number of downloads")

    date_from: Mapped[datetime.datetime] = mapped_column(comment="Date from")
    date_to: Mapped[datetime.datetime] = mapped_column(comment="Date to")


class UsageSearch(Base):
    __tablename__ = "usage_search"

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)
    query: Mapped[str] = mapped_column(nullable=False, comment="Search query")
    type: Mapped[str] = mapped_column(
        nullable=False, comment="Type of the search. Bed/Bedset"
    )
    count: Mapped[int] = mapped_column(default=0, comment="Number of searches")

    date_from: Mapped[datetime.datetime] = mapped_column(comment="Date from")
    date_to: Mapped[datetime.datetime] = mapped_column(comment="Date to")


class BedSnapshot(Base):
    """
    Index of bulk metadata exports published to S3.

    One row per published artifact (metadata / bedsets / membership / manifest).
    The exporter writes a row after a successful upload; the /v1/bed/exports
    endpoint reads them newest-first. This is a new table, so
    Base.metadata.create_all() creates it on the next connection.
    """

    __tablename__ = "bed_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)
    file_path: Mapped[str] = mapped_column(
        nullable=False, comment="S3 object key, relative to the bucket root"
    )
    file_type: Mapped[str] = mapped_column(
        nullable=False, comment="metadata | bedsets | bedset_membership | manifest"
    )
    creation_date: Mapped[datetime.datetime] = mapped_column(
        default=deliver_update_date, comment="Build date of the export"
    )
    record_count: Mapped[Optional[int]] = mapped_column(
        nullable=True, comment="Rows actually written to the file"
    )
    file_size: Mapped[Optional[int]] = mapped_column(
        nullable=True, comment="Size of the file in bytes"
    )
    checksum: Mapped[Optional[str]] = mapped_column(
        nullable=True, comment="SHA256 of the file"
    )
    schema_version: Mapped[Optional[int]] = mapped_column(
        nullable=True, comment="Export schema version"
    )


class AnalysisFile(Base):
    """
    Registry of standalone analysis files (openSignalMatrix, models, other
    analysis inputs) stored in S3. Not tied to any bed file or bedset.

    Append-only: one row per uploaded file, so name-based lookups resolve the
    newest matching row (same model as ``bed_snapshots``). This is a new table,
    so ``Base.metadata.create_all()`` creates it on the next connection.
    """

    __tablename__ = "analysis_files"

    id: Mapped[int] = mapped_column(primary_key=True, index=True, autoincrement=True)
    name: Mapped[str] = mapped_column(
        nullable=False, index=True, comment="Logical name/key, e.g. openSignalMatrix"
    )
    file_path: Mapped[str] = mapped_column(
        nullable=False, comment="S3 object key, relative to the bucket root"
    )
    file_type: Mapped[Optional[str]] = mapped_column(
        nullable=True,
        index=True,
        comment="Category, e.g. openSignalMatrix | reference | model",
    )
    genome: Mapped[Optional[str]] = mapped_column(
        nullable=True, index=True, comment="Genome/assembly, e.g. hg38 (optional)"
    )
    description: Mapped[Optional[str]] = mapped_column(nullable=True)
    tags: Mapped[Optional[list]] = mapped_column(
        ARRAY(String), nullable=True, comment="Free-form tags"
    )
    file_size: Mapped[Optional[int]] = mapped_column(
        nullable=True, comment="Size of the file in bytes"
    )
    checksum: Mapped[Optional[str]] = mapped_column(
        nullable=True, comment="SHA256 of the file"
    )
    creation_date: Mapped[datetime.datetime] = mapped_column(
        default=deliver_update_date, comment="Upload date"
    )


class BaseEngine:
    """
    A class with base methods, that are used in several classes.
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        database: str = "bedbase",
        user: str | None = None,
        password: str | None = None,
        drivername: str = POSTGRES_DIALECT,
        dsn: str | None = None,
        echo: bool = False,
        run_migrations: bool = False,
    ):
        """
        Initialize connection to the bedbase database. You can use the basic connection parameters
        or libpq connection string.

        Args:
            host: Database server address e.g., localhost or an IP address.
            port: The port number that defaults to 5432 if it is not provided.
            database: The name of the database that you want to connect.
            user: The username used to authenticate.
            password: Password used to authenticate.
            drivername: Driver used in connection.
            dsn: Libpq connection string using the dsn parameter
                (e.g. 'postgresql://user_name:password@host_name:port/db_name').
            run_migrations: Upgrade the database to the latest Alembic revision
                (``head``) before connecting. Safe on an already-migrated or
                pre-existing database (the initial revision is idempotent).
        """
        if not dsn:
            dsn = URL.create(
                host=host,
                port=port,
                database=database,
                username=user,
                password=password,
                drivername=drivername,
            )

        if run_migrations:
            if isinstance(dsn, str):
                migration_url = dsn
            else:
                migration_url = dsn.render_as_string(hide_password=False)
            self.run_db_migration(migration_url)

        self._engine = create_engine(dsn, echo=echo)
        self.create_schema(self._engine)
        self.check_db_connection()

    def create_schema(self, engine=None):
        """
        Create sql schema in the database.

        Args:
            engine: Sqlalchemy engine [Default: None].

        Returns:
            None.
        """
        if not engine:
            engine = self._engine
        Base.metadata.create_all(engine)

        global tables_initialized
        if License.__tablename__ in tables_initialized:
            try:
                # It is weired, but tables are sometimes initialized twice. Or it says like that...
                self._upload_licenses()
            except IntegrityError:
                pass

    def delete_schema(self, engine=None) -> None:
        """
        Delete sql schema in the database.

        Args:
            engine: Sqlalchemy engine [Default: None].

        Returns:
            None.
        """
        if not engine:
            engine = self._engine
        Base.metadata.drop_all(engine)
        return None

    def run_db_migration(self, database_url: str) -> None:
        """
        Upgrade the database to the latest Alembic revision (``head``).

        The Alembic config is built programmatically so the package does not
        depend on the repo-root ``alembic.ini`` at runtime.

        Args:
            database_url: SQLAlchemy connection URL (with password) to migrate.
        """
        script_location = os.path.join(os.path.dirname(__file__), "alembic")

        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", script_location)
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)

        _LOGGER.info("Running database migrations to the latest revision...")
        command.upgrade(alembic_cfg, "head")

    def session_execute(self, statement: Select) -> Result:
        """
        Execute statement using sqlalchemy statement.

        Args:
            statement: SQL query or a SQL expression that is constructed using
                SQLAlchemy's SQL expression language.

        Returns:
            Query result represented with declarative base.
        """
        _LOGGER.debug(f"Executing statement: {statement}")
        with Session(self._engine) as session:
            query_result = session.execute(statement)

        return query_result

    @property
    def session(self):
        """
        Get a started sqlalchemy session.

        Returns:
            Started sqlalchemy session.
        """
        return self._start_session()

    @property
    def engine(self) -> Engine:
        """
        Get sqlalchemy engine.

        Returns:
            Sqlalchemy engine.
        """
        return self._engine

    def _start_session(self):
        session = Session(self.engine)
        try:
            session.execute(select(Bed).limit(1))
        except ProgrammingError:
            raise SchemaError()

        return session

    def check_db_connection(self):
        try:
            self.session_execute(select(Bed).limit(1))
        except ProgrammingError:
            raise SchemaError()

    def create_schema_graph(self, output_file: str = "schema.svg"):
        """
        Create schema graph of the database.

        Args:
            output_file: Path to the output file.

        Returns:
            None.
        """
        graph = create_schema_graph(engine=self.engine, metadata=Base.metadata)
        graph.write(output_file, format="svg", prog="dot")
        return None

    def _upload_licenses(self):
        """
        Upload licenses to the database.
        """
        # Check if licenses already exist to avoid duplicate key errors
        with Session(self.engine) as session:
            existing = session.scalar(select(License).limit(1))
            if existing:
                _LOGGER.info("Licenses already exist in the database, skipping upload.")
                return

        _LOGGER.info("Uploading licenses to the database...")
        df = pd.read_csv(LICENSES_CSV_URL)

        df.to_sql(License.__tablename__, self.engine, if_exists="append", index=False)

        _LOGGER.info("Licenses uploaded successfully!")
