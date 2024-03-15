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
from sqlalchemy.engine import URL, create_engine
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


class Bedfiles(Base):

    __tablename__ = "bedfiles"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = Mapped[Optional[str]]
    genome_alias: Mapped[Optional[str]]
    genome_digest: Mapped[Optional[str]]
    bed_type: Mapped[str] = mapped_column(default="bed3")
    bed_format: Mapped[str] = mapped_column(default="bed")
    indexed: Mapped[bool] = mapped_column(default=False, description="Whether sample was added to qdrant")
    pephub: Mapped[bool] = mapped_column(default=False, description="Whether sample was added to pephub")
    submission_date: Mapped[datetime.datetime]
    last_update_date: Mapped[Optional[datetime.datetime]] = mapped_column(
        default=deliver_update_date,  # onupdate=deliver_update_date, # This field should not be updated, while we are adding project to favorites
    )

class Files(Base):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False, description="Name of the file, e.g. bed, bigBed")
    path: Mapped[str]
