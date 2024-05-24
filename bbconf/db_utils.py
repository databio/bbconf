import datetime
import logging
from typing import List, Optional

from sqlalchemy import TIMESTAMP, BigInteger, ForeignKey, Result, Select, event, select
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.engine import URL, Engine, create_engine
from sqlalchemy.event import listens_for
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship
from sqlalchemy_schemadisplay import create_schema_graph

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
    is_universe: Mapped[Optional[bool]] = mapped_column(default=False)

    files: Mapped[List["Files"]] = relationship(
        "Files", back_populates="bedfile", cascade="all, delete-orphan"
    )

    bedsets: Mapped[List["BedFileBedSetRelation"]] = relationship(
        "BedFileBedSetRelation", back_populates="bedfile", cascade="all, delete-orphan"
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

    bed: Mapped["Bed"] = relationship("Bed", back_populates="stats")


class Files(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str] = mapped_column(
        nullable=False, comment="Name of the file, e.g. bed, bigBed"
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


class BedFileBedSetRelation(Base):
    __tablename__ = "bedfile_bedset_relation"

    bedset_id: Mapped[str] = mapped_column(
        ForeignKey("bedsets.id", ondelete="CASCADE"), primary_key=True
    )
    bedfile_id: Mapped[str] = mapped_column(
        ForeignKey("bed.id", ondelete="CASCADE"), primary_key=True
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
        "BedFileBedSetRelation", back_populates="bedset", cascade="all, delete-orphan"
    )
    files: Mapped[List["Files"]] = relationship("Files", back_populates="bedset")
    universe: Mapped["Universes"] = relationship("Universes", back_populates="bedset")


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
        "Universes", back_populates="tokenized"
    )


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

    def delete_schema(self, engine=None) -> None:
        """
        Delete sql schema in the database.

        :param engine: sqlalchemy engine [Default: None]
        :return: None
        """
        if not engine:
            engine = self._engine
        Base.metadata.drop_all(engine)
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

    def create_schema_graph(self, output_file: str = "schema.svg"):
        """
        Create schema graph of the database.

        :param output_file: path to the output file
        :return: None
        """
        graph = create_schema_graph(engine=self.engine, metadata=Base.metadata)
        graph.write(output_file, format="svg", prog="dot")
        return None
