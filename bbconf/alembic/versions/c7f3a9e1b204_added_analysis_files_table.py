"""Added analysis_files table

Revision ID: c7f3a9e1b204
Revises: 845d978eac7d
Create Date: 2026-08-17 21:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "c7f3a9e1b204"
down_revision: Union[str, None] = "845d978eac7d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "analysis_files",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "name",
            sa.String(),
            nullable=False,
            comment="Logical name/key, e.g. openSignalMatrix",
        ),
        sa.Column(
            "file_path",
            sa.String(),
            nullable=False,
            comment="S3 object key, relative to the bucket root",
        ),
        sa.Column(
            "file_type",
            sa.String(),
            nullable=True,
            comment="Category, e.g. openSignalMatrix | reference | model",
        ),
        sa.Column(
            "genome",
            sa.String(),
            nullable=True,
            comment="Genome/assembly, e.g. hg38 (optional)",
        ),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column(
            "tags",
            postgresql.ARRAY(sa.String()),
            nullable=True,
            comment="Free-form tags",
        ),
        sa.Column(
            "file_size",
            sa.Integer(),
            nullable=True,
            comment="Size of the file in bytes",
        ),
        sa.Column("checksum", sa.String(), nullable=True, comment="SHA256 of the file"),
        sa.Column(
            "creation_date",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            comment="Upload date",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_analysis_files_id"), "analysis_files", ["id"], unique=False
    )
    op.create_index(
        op.f("ix_analysis_files_name"), "analysis_files", ["name"], unique=False
    )
    op.create_index(
        op.f("ix_analysis_files_file_type"),
        "analysis_files",
        ["file_type"],
        unique=False,
    )
    op.create_index(
        op.f("ix_analysis_files_genome"),
        "analysis_files",
        ["genome"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_analysis_files_genome"), table_name="analysis_files")
    op.drop_index(op.f("ix_analysis_files_file_type"), table_name="analysis_files")
    op.drop_index(op.f("ix_analysis_files_name"), table_name="analysis_files")
    op.drop_index(op.f("ix_analysis_files_id"), table_name="analysis_files")
    op.drop_table("analysis_files")
