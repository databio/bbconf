"""Add median_neighbor_distance, drop legacy tssdist

Revision ID: 037b8093e365
Revises: c7f3a9e1b204
Create Date: 2026-09-03 00:00:00.000000

``bed_stats.tssdist`` is a vestige of the TSS-distance quantity, superseded by
``median_tss_dist`` (both were created by the initial migration). It has never
had a writer: ``BedStatsModel`` has never declared the field, and the only
insert paths go through ``BedStats(**stats.model_dump())``, so no value could
reach it.

It is dropped rather than renamed because neighbor distance and TSS distance
are different measurements -- neighbor distance is the gap between consecutive
regions within a file, TSS distance is the distance from each region to the
nearest annotated TSS. Renaming would relabel any stale TSS values as neighbor
distances, which would then be averaged into bedset ``scalar_summaries``.

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "037b8093e365"
down_revision: Union[str, None] = "c7f3a9e1b204"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "bed_stats",
        sa.Column("median_neighbor_distance", sa.Float(), nullable=True),
    )
    op.drop_column("bed_stats", "tssdist")


def downgrade() -> None:
    """Downgrade schema."""
    op.add_column(
        "bed_stats",
        sa.Column("tssdist", sa.Float(), nullable=True),
    )
    op.drop_column("bed_stats", "median_neighbor_distance")
