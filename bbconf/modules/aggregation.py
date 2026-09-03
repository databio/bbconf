"""Collection-level aggregation of per-file genomic distributions.

All aggregation is pushed to SQL (PostgreSQL) using SQLAlchemy Core/ORM
constructs -- no raw ``text()`` and no per-row Python loops. Used by both
``BedAgentBedSet.create()`` and ``BedAgentBedFile.aggregate_distributions()``.
"""

import logging
import math
from typing import List, Optional

from sqlalchemy import Float, String, any_, cast, func, select, true
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from bbconf.const import PKG_NAME
from bbconf.db_utils import Bed, BedMetadata, BedStats
from bbconf.models.bedset_models import BedSetDistributions

_LOGGER = logging.getLogger(PKG_NAME)


def _ids_array(bed_ids: List[str]):
    """Match ``column`` against bed_ids as ``id = ANY(:ids)`` with a single param.

    Binding bed_ids as one PostgreSQL ``text[]`` array (rather than an expanded
    ``IN (...)`` list) keeps every statement to a single bind parameter no matter
    how many ids are passed. psycopg3 binds parameters server-side and PostgreSQL
    caps a statement at 65535 parameters, so an expanded IN list would fail for
    very large collections; the array form has no such ceiling and a flat parse
    cost. Use as ``column == _ids_array(bed_ids)``.
    """
    return any_(cast(bed_ids, ARRAY(String)))


# Number of bins when building histograms of per-file scalar means
_SCALAR_HIST_BINS = 25
# Default decimal precision for stored floats
DEFAULT_PRECISION = 3
# Above this many files, the region_distribution SQL unnest (files x chromosomes
# x bins) becomes very heavy and may be slow, exhaust work_mem, or hit a
# statement timeout. Crossing it emits a warning; it is a heuristic, not a hard
# limit (actual cost depends on how many bins gtars emits per file).
LARGE_COLLECTION_WARN_THRESHOLD = 5000

# Scalar columns aggregated into ``scalar_summaries``, as
# (output key, ORM column) pairs.
_SCALAR_COLUMNS = [
    ("number_of_regions", BedStats.number_of_regions),
    ("mean_region_width", BedStats.mean_region_width),
    ("median_tss_dist", BedStats.median_tss_dist),
    ("gc_content", BedStats.gc_content),
    ("median_neighbor_distance", BedStats.median_neighbor_distance),
]

# Genomic partition columns aggregated into ``partitions``, as
# (output key, ORM column) pairs.
_PARTITION_COLUMNS = [
    ("exon", BedStats.exon_percentage),
    ("intron", BedStats.intron_percentage),
    ("intergenic", BedStats.intergenic_percentage),
    ("promoterprox", BedStats.promoterprox_percentage),
    ("promotercore", BedStats.promotercore_percentage),
    ("fiveutr", BedStats.fiveutr_percentage),
    ("threeutr", BedStats.threeutr_percentage),
]

# Metadata columns (on ``bed_metadata``) counted for ``composition``.
# ``genome_alias`` lives on ``bed`` and is handled separately.
_METADATA_FIELDS = ["assay", "cell_type", "tissue", "target"]


def round_floats(obj, ndigits: int = DEFAULT_PRECISION):
    """Recursively round floats in nested dicts/lists."""
    if isinstance(obj, float):
        return round(obj, ndigits)
    if isinstance(obj, dict):
        return {k: round_floats(v, ndigits) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [round_floats(v, ndigits) for v in obj]
    return obj


def aggregate_collection(
    engine: Engine,
    bed_ids: List[str],
    precision: int = DEFAULT_PRECISION,
) -> BedSetDistributions:
    """Aggregate per-file distributions into collection-level stats.

    All aggregation is done in SQL. Python only reshapes query results
    into the BedSetDistributions model.

    .. warning::
        This can fail (or become very slow) on a big workload. The dominant
        cost is the ``region_distribution`` aggregation, which unnests every
        member file's per-chromosome bin arrays in SQL -- roughly
        ``files x chromosomes x bins`` intermediate rows. For very large
        collections (see ``LARGE_COLLECTION_WARN_THRESHOLD``) this may exhaust
        ``work_mem``, spill to temp disk, or trip a PostgreSQL statement
        timeout, raising an error. Callers that must tolerate this (e.g.
        ``BedAgentBedSet.create``) should wrap the call and degrade gracefully.

    :param engine: SQLAlchemy engine
    :param bed_ids: list of bed file identifiers
    :param precision: decimal places for stored floats (default 3)
    :return: BedSetDistributions with aggregated distributions
    """
    if not bed_ids:
        return BedSetDistributions(n_files=0)

    n = len(bed_ids)

    if n > LARGE_COLLECTION_WARN_THRESHOLD:
        _LOGGER.warning(
            f"Aggregating distributions for {n} files (> "
            f"{LARGE_COLLECTION_WARN_THRESHOLD}). The region_distribution SQL "
            f"aggregation is heavy at this scale and may be slow or fail "
            f"(work_mem/temp disk/statement timeout)."
        )

    with Session(engine) as session:
        composition = _aggregate_composition(session, bed_ids)
        scalar_summaries = _aggregate_scalars(session, bed_ids)
        region_distribution = _aggregate_region_distribution(session, bed_ids)
        tss_histogram = _aggregate_tss_histogram(session, bed_ids)
        partitions = _aggregate_partitions(session, bed_ids)

    stats = BedSetDistributions(
        n_files=n,
        composition=composition,
        scalar_summaries=scalar_summaries,
        tss_histogram=tss_histogram,
        region_distribution=region_distribution,
        partitions=partitions,
    )

    if precision is not None:
        stats = BedSetDistributions(**round_floats(stats.model_dump(), precision))

    return stats


# ---------------------------------------------------------------------------
# SQL aggregation helpers
# ---------------------------------------------------------------------------


def _aggregate_composition(session: Session, bed_ids: List[str]) -> Optional[dict]:
    """Count distinct values per metadata column via SQL GROUP BY."""
    result = {}

    # genome_alias lives on the bed table itself.
    genome_stmt = (
        select(Bed.genome_alias.label("val"), func.count().label("cnt"))
        .where(Bed.id == _ids_array(bed_ids), Bed.genome_alias.isnot(None))
        .group_by(Bed.genome_alias)
        .order_by(func.count().desc())
    )
    rows = session.execute(genome_stmt).all()
    if rows:
        result["genome_alias"] = {row.val: row.cnt for row in rows}

    # All other composition fields live on bed_metadata (shared PK with bed).
    for field in _METADATA_FIELDS:
        column = getattr(BedMetadata, field)
        stmt = (
            select(column.label("val"), func.count().label("cnt"))
            .where(BedMetadata.id == _ids_array(bed_ids), column.isnot(None))
            .group_by(column)
            .order_by(func.count().desc())
        )
        rows = session.execute(stmt).all()
        if rows:
            result[field] = {row.val: row.cnt for row in rows}

    return result or None


def _aggregate_scalars(session: Session, bed_ids: List[str]) -> Optional[dict]:
    """Compute mean, sd, and histogram for scalar columns in SQL.

    Uses a single query for mean/sd/min/max/count, then width_bucket
    for histogram binning.
    """
    # 1. Mean, sd, min, max, count for every scalar column in one query.
    agg_columns = []
    for key, column in _SCALAR_COLUMNS:
        agg_columns.extend(
            [
                func.avg(column).label(f"{key}_mean"),
                func.stddev(column).label(f"{key}_sd"),
                func.min(column).label(f"{key}_min"),
                func.max(column).label(f"{key}_max"),
                func.count(column).label(f"{key}_n"),
            ]
        )

    row = session.execute(
        select(*agg_columns).where(BedStats.id == _ids_array(bed_ids))
    ).one()

    result = {}
    for key, column in _SCALAR_COLUMNS:
        n = getattr(row, f"{key}_n")
        if not n:
            continue
        col_min = float(getattr(row, f"{key}_min"))
        col_max = float(getattr(row, f"{key}_max"))

        # 2. Histogram via width_bucket (PostgreSQL).
        histogram = _scalar_histogram(session, bed_ids, column, col_min, col_max, n)

        result[key] = {
            "mean": float(getattr(row, f"{key}_mean")),
            "sd": float(getattr(row, f"{key}_sd") or 0.0),
            "n": n,
            "histogram": histogram,
        }

    return result or None


def _scalar_histogram(
    session: Session,
    bed_ids: List[str],
    column,
    col_min: float,
    col_max: float,
    n: int,
) -> dict:
    """Build a histogram for a single scalar column using width_bucket."""
    num_bins = min(_SCALAR_HIST_BINS, max(3, math.ceil(math.sqrt(n))))

    if col_min == col_max:
        # All values identical -- single bin.
        return {"counts": [n], "edges": [col_min, col_max]}

    bucket = func.width_bucket(column, col_min, col_max, num_bins).label("bucket")
    stmt = (
        select(bucket, func.count().label("cnt"))
        .where(BedStats.id == _ids_array(bed_ids), column.isnot(None))
        .group_by(bucket)
        .order_by(bucket)
    )
    rows = session.execute(stmt).all()

    # width_bucket returns 1..num_bins (in-range) plus 0 (below) and
    # num_bins+1 (above/equal to hi).
    counts = [0] * num_bins
    for row in rows:
        if row.bucket == 0:
            counts[0] += row.cnt
        elif row.bucket > num_bins:
            counts[-1] += row.cnt
        else:
            counts[row.bucket - 1] += row.cnt

    step = (col_max - col_min) / num_bins
    edges = [col_min + i * step for i in range(num_bins + 1)]

    return {"counts": counts, "edges": edges}


def _aggregate_region_distribution(
    session: Session, bed_ids: List[str]
) -> Optional[dict]:
    """Aggregate per-chromosome region_distribution via SQL JSONB unnest.

    Requires that member files used gtars >= PR #248 with --chrom-sizes so that
    bin widths are reference-aligned across files (same bin_idx -> same bp
    range on a given chromosome, regardless of file).

    Returns {chrom: {mean: [...], sd: [...], n: int}} or None if no data.
    """
    region_distribution = BedStats.distributions["distributions"]["region_distribution"]

    per_file = (
        select(region_distribution.label("rd"))
        .where(
            BedStats.id == _ids_array(bed_ids),
            BedStats.distributions.isnot(None),
            region_distribution.isnot(None),
        )
        .cte("per_file")
    )

    # jsonb_each(rd) -> (key=chrom, value=counts array), one row per chromosome.
    per_chrom = (
        func.jsonb_each(per_file.c.rd).table_valued("key", "value").lateral("per_chrom")
    )
    # jsonb_array_elements_text(counts) WITH ORDINALITY -> (value, ordinality).
    elements = (
        func.jsonb_array_elements_text(per_chrom.c.value)
        .table_valued("value", with_ordinality="ordinality")
        .lateral("elements")
    )

    unnested = (
        select(
            per_chrom.c.key.label("chrom"),
            (elements.c.ordinality - 1).label("bin_idx"),
            cast(elements.c.value, Float).label("count"),
        )
        .select_from(per_file)
        .join(per_chrom, true())
        .join(elements, true())
        .cte("unnested")
    )

    stmt = (
        select(
            unnested.c.chrom,
            unnested.c.bin_idx,
            func.avg(unnested.c.count).label("mean"),
            func.coalesce(func.stddev(unnested.c.count), 0.0).label("sd"),
            func.count().label("n"),
        )
        .group_by(unnested.c.chrom, unnested.c.bin_idx)
        .order_by(unnested.c.chrom, unnested.c.bin_idx)
    )

    rows = session.execute(stmt).all()
    if not rows:
        return None

    result = {}
    for row in rows:
        entry = result.setdefault(row.chrom, {"mean": [], "sd": [], "n": int(row.n)})
        while len(entry["mean"]) <= row.bin_idx:
            entry["mean"].append(0.0)
            entry["sd"].append(0.0)
        entry["mean"][row.bin_idx] = float(row.mean)
        entry["sd"][row.bin_idx] = float(row.sd)

    return result or None


def _aggregate_tss_histogram(session: Session, bed_ids: List[str]) -> Optional[dict]:
    """Aggregate fixed-axis tss_distances histogram via SQL.

    TSS distances use a fixed 100-bin axis (+/-100 kb), so element-wise
    AVG/STDDEV across files is valid without re-binning.

    Returns {mean: [...], sd: [...], n: int, x_min, x_max, bins} or None.
    """
    tss = BedStats.distributions["distributions"]["tss_distances"]
    counts = tss["counts"]

    per_file = (
        select(
            counts.label("counts"),
            tss["x_min"].astext.label("x_min"),
            tss["x_max"].astext.label("x_max"),
            tss["bins"].astext.label("bins"),
        )
        .where(
            BedStats.id == _ids_array(bed_ids),
            BedStats.distributions.isnot(None),
            counts.isnot(None),
        )
        .cte("per_file")
    )

    elements = (
        func.jsonb_array_elements_text(per_file.c.counts)
        .table_valued("value", with_ordinality="ordinality")
        .lateral("elements")
    )

    unnested = (
        select(
            (elements.c.ordinality - 1).label("bin_idx"),
            cast(elements.c.value, Float).label("count"),
            per_file.c.x_min,
            per_file.c.x_max,
            per_file.c.bins,
        )
        .select_from(per_file)
        .join(elements, true())
        .cte("unnested")
    )

    stmt = (
        select(
            unnested.c.bin_idx,
            func.avg(unnested.c.count).label("mean"),
            func.coalesce(func.stddev(unnested.c.count), 0.0).label("sd"),
            func.count().label("n"),
            func.max(unnested.c.x_min).label("x_min"),
            func.max(unnested.c.x_max).label("x_max"),
            func.max(unnested.c.bins).label("bins"),
        )
        .group_by(unnested.c.bin_idx)
        .order_by(unnested.c.bin_idx)
    )

    rows = session.execute(stmt).all()
    if not rows:
        return None

    n_bins = len(rows)
    result = {
        "mean": [0.0] * n_bins,
        "sd": [0.0] * n_bins,
        "n": int(rows[0].n),
    }

    x_min, x_max, bins_str = rows[0].x_min, rows[0].x_max, rows[0].bins
    if x_min is not None:
        try:
            result["x_min"] = float(x_min)
            result["x_max"] = float(x_max)
            result["bins"] = int(bins_str) if bins_str else n_bins
        except (ValueError, TypeError):
            pass

    for row in rows:
        result["mean"][row.bin_idx] = float(row.mean)
        result["sd"][row.bin_idx] = float(row.sd)

    return result


def _aggregate_partitions(session: Session, bed_ids: List[str]) -> Optional[dict]:
    """Aggregate genomic partitions from the flat ``*_percentage`` columns.

    Those columns hold a fraction, not a percentage: ``regionstat.R`` stores
    ``Freq/length(query)`` and the gtars backend stores ``count/total``. Mean
    and sd come back on the same scale.
    """
    agg_columns = []
    for name, column in _PARTITION_COLUMNS:
        agg_columns.extend(
            [
                func.avg(column).label(f"{name}_mean"),
                func.coalesce(func.stddev(column), 0.0).label(f"{name}_sd"),
                func.count(column).label(f"{name}_n"),
            ]
        )

    row = session.execute(
        select(*agg_columns).where(BedStats.id == _ids_array(bed_ids))
    ).one()

    result = {}
    for name, _column in _PARTITION_COLUMNS:
        n = getattr(row, f"{name}_n")
        if not n:
            continue
        result[name] = {
            "mean": float(getattr(row, f"{name}_mean")),
            "sd": float(getattr(row, f"{name}_sd")),
            "n": int(n),
        }

    return result or None
