import datetime
import logging
from typing import Optional, List

from sqlalchemy import (
    BigInteger,
    FetchedValue,
    Result,
    Select,
    String,
    event,
    select,
    TIMESTAMP,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.engine import URL, create_engine, Engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
)

from bbconf.const import PKG_NAME


_LOGGER = logging.getLogger(PKG_NAME)


POSTGRES_DIALECT = "postgresql+psycopg"


class SchemaError(Exception):
    def __init__(self):
        super().__init__(
            """PEP_db connection error! The schema of connected db is incorrect!"""
        )


class BIGSERIAL(BigInteger):
    pass


@compiles(BIGSERIAL, POSTGRES_DIALECT)
def compile_bigserial_pg(type_, compiler, **kw):
    return "BIGSERIAL"


@compiles(JSON, POSTGRES_DIALECT)
def compile_jsonb_pg(type_, compiler, **kw):
    return "JSONB"


class Base(DeclarativeBase):
    type_annotation_map = {datetime.datetime: TIMESTAMP(timezone=True)}


@event.listens_for(Base.metadata, "after_create")
def receive_after_create(target, connection, tables, **kw):
    """
    listen for the 'after_create' event
    """
    if tables:
        _LOGGER.info("A table was created")
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
    bed_type: Mapped[str] = mapped_column(default="bed3")
    bed_format: Mapped[str] = mapped_column(default="bed")
    indexed: Mapped[bool] = mapped_column(
        default=False, comment="Whether sample was added to qdrant"
    )
    pephub: Mapped[bool] = mapped_column(
        default=False, comment="Whether sample was added to pephub"
    )

    submission_date: Mapped[datetime.datetime] = mapped_column(
        default=deliver_update_date
    )
    last_update_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_update_date,
        onupdate=deliver_update_date,
    )

    # statistics:
    number_of_regions: Mapped[Optional[int]]
    gc_content: Mapped[Optional[int]]
    median_tss_dist: Mapped[Optional[int]]
    mean_region_width: Mapped[Optional[int]]
    exon_frequency: Mapped[Optional[int]]
    intron_frequency: Mapped[Optional[int]]
    promoterprox_frequency: Mapped[Optional[int]]
    intergenic_frequency: Mapped[Optional[int]]
    promotercore_frequency: Mapped[Optional[int]]
    fiveutr_frequency: Mapped[Optional[int]]
    threeutr_frequency: Mapped[Optional[int]]
    fiveutr_percentage: Mapped[Optional[int]]
    threeutr_percentage: Mapped[Optional[int]]
    promoterprox_percentage: Mapped[Optional[int]]
    exon_percentage: Mapped[Optional[int]]
    intron_percentage: Mapped[Optional[int]]
    intergenic_percentage: Mapped[Optional[int]]
    promotercore_percentage: Mapped[Optional[int]]
    tssdist: Mapped[Optional[int]]

    # relations:
    # plots: Mapped[List["Plots"]] = relationship("Plots", back_populates="bedfile")
    files: Mapped[List["Files"]] = relationship("Files", back_populates="bedfile")

    bedsets: Mapped[List["BedFileBedSetRelation"]] = relationship(
        "BedFileBedSetRelation", back_populates="bedfile"
    )


class Files(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(
        nullable=False, comment="Name of the file, e.g. bed, bigBed"
    )
    type: Mapped[str] = mapped_column(
        default="file", comment="Type of the object, e.g. file, plot, ..."
    )
    path: Mapped[str]
    path_thumbnail: Mapped[str] = mapped_column(
        nullable=True, comment="Thumbnail path of the file"
    )
    description: Mapped[Optional[str]]
    size: Mapped[Optional[int]] = mapped_column(default=0, comment="Size of the file")

    bedfile_id: Mapped[int] = mapped_column(
        ForeignKey("bed.id"), nullable=True, index=True
    )
    bedset_id: Mapped[int] = mapped_column(
        ForeignKey("bedsets.id"), nullable=True, index=True
    )

    bedfile: Mapped["Bed"] = relationship("Bed", back_populates="files")
    bedset: Mapped["BedSets"] = relationship("BedSets", back_populates="files")


# class Plots(Base):
#     __tablename__ = "plots"
#
#     id: Mapped[int] = mapped_column(primary_key=True)
#     name: Mapped[str] = mapped_column(nullable=False, comment="Name of the plot")
#     description: Mapped[Optional[str]] = mapped_column(
#         comment="Description of the plot"
#     )
#     path: Mapped[str] = mapped_column(comment="Path to the plot file")
#     path_thumbnail: Mapped[str] = mapped_column(
#         nullable=True, comment="Path to the thumbnail of the plot file"
#     )
#
#     bedfile_id: Mapped[int] = mapped_column(ForeignKey("bed.id"), nullable=True)
#     bedset_id: Mapped[int] = mapped_column(ForeignKey("bedsets.id"), nullable=True)
#
#     bedfile: Mapped["Bed"] = relationship("Bed", back_populates="plots")
#     bedset: Mapped["BedSets"] = relationship("BedSets", back_populates="plots")


class BedFileBedSetRelation(Base):
    __tablename__ = "bedfile_bedset_relation"
    bedset_id: Mapped[int] = mapped_column(ForeignKey("bedsets.id"), primary_key=True)
    bedfile_id: Mapped[int] = mapped_column(ForeignKey("bed.id"), primary_key=True)

    bedset: Mapped["BedSets"] = relationship("BedSets", back_populates="bedfiles")
    bedfile: Mapped["Bed"] = relationship("Bed", back_populates="bedsets")


class BedSets(Base):
    __tablename__ = "bedsets"

    id: Mapped[str] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(nullable=False, comment="Name of the bedset")
    description: Mapped[Optional[str]] = mapped_column(
        comment="Description of the bedset"
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

    bedfiles: Mapped[List["BedFileBedSetRelation"]] = relationship(
        "BedFileBedSetRelation", back_populates="bedset"
    )
    # plots: Mapped[List["Plots"]] = relationship("Plots", back_populates="bedset")
    files: Mapped[List["Files"]] = relationship("Files", back_populates="bedset")


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
        user: str = None,
        password: str = None,
        drivername: str = POSTGRES_DIALECT,
        dsn: str = None,
        echo: bool = False,
    ):
        """
        Initialize connection to the bedbase database. You can use The basic connection parameters
        or libpq connection string.

        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param drivername: driver used in
        :param dsn: libpq connection string using the dsn parameter
        (e.g. 'postgresql://user_name:password@host_name:port/db_name')
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

        self._engine = create_engine(dsn, echo=echo)
        self.create_schema(self._engine)
        self.check_db_connection()

    def create_schema(self, engine=None):
        """
        Create sql schema in the database.

        :param engine: sqlalchemy engine [Default: None]
        :return: None
        """
        if not engine:
            engine = self._engine
        Base.metadata.create_all(engine)
        return None

    def session_execute(self, statement: Select) -> Result:
        """
        Execute statement using sqlalchemy statement

        :param statement: SQL query or a SQL expression that is constructed using
            SQLAlchemy's SQL expression language
        :return: query result represented with declarative base
        """
        _LOGGER.debug(f"Executing statement: {statement}")
        with Session(self._engine) as session:
            query_result = session.execute(statement)

        return query_result

    @property
    def session(self):
        """
        :return: started sqlalchemy session
        """
        return self._start_session()

    @property
    def engine(self) -> Engine:
        """
        :return: sqlalchemy engine
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
