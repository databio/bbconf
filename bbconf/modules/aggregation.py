"""Collection-level aggregation of per-file genomic distributions.

All aggregation is pushed to SQL (PostgreSQL). No per-row Python loops.
Used by both BedAgentBedSet.create() and BedAgentBedFile.aggregate_collection().
"""

import logging
import math
from typing import Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from bbconf.const import PKG_NAME
from bbconf.models.bedset_models import BedSetDistributions

_LOGGER = logging.getLogger(PKG_NAME)

# Number of bins when building histograms of per-file scalar means
_SCALAR_HIST_BINS = 25
# Default decimal precision for stored floats
DEFAULT_PRECISION = 3


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

    :param engine: SQLAlchemy engine
    :param bed_ids: list of bed file identifiers
    :param precision: decimal places for stored floats (default 3)
    :return: BedSetDistributions with aggregated distributions
    """
    if not bed_ids:
        return BedSetDistributions(n_files=0)

    n = len(bed_ids)

    with Session(engine) as session:
        composition = _sql_aggregate_composition(session, bed_ids)
        scalar_summaries = _sql_aggregate_scalars(session, bed_ids)
        region_distribution = _sql_aggregate_region_distribution(session, bed_ids)
        tss_histogram = _sql_aggregate_tss_histogram(session, bed_ids)
        partitions = _sql_aggregate_partitions(session, bed_ids)

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


def _sql_aggregate_composition(session: Session, bed_ids: List[str]) -> Optional[dict]:
    """Count distinct values per metadata column via SQL GROUP BY."""
    fields = ["genome_alias", "assay", "cell_type", "tissue", "target"]
    result = {}

    for field in fields:
        if field == "genome_alias":
            sql = text(
                """
                SELECT genome_alias AS val, COUNT(*) AS cnt
                FROM bed
                WHERE id = ANY(:bed_ids) AND genome_alias IS NOT NULL
                GROUP BY genome_alias
                ORDER BY cnt DESC
                """
            )
        else:
            sql = text(
                f"""
                SELECT m.{field} AS val, COUNT(*) AS cnt
                FROM bed b
                JOIN bed_metadata m ON m.id = b.id
                WHERE b.id = ANY(:bed_ids) AND m.{field} IS NOT NULL
                GROUP BY m.{field}
                ORDER BY cnt DESC
                """
            )
        rows = session.execute(sql, {"bed_ids": bed_ids}).all()
        if rows:
            result[field] = {row.val: row.cnt for row in rows}

    return result if result else None


def _sql_aggregate_scalars(session: Session, bed_ids: List[str]) -> Optional[dict]:
    """Compute mean, sd, and histogram for scalar columns in SQL.

    Uses a single query for mean/sd/min/max/count, then width_bucket
    for histogram binning.
    """
    scalar_columns = [
        "number_of_regions",
        "mean_region_width",
        "median_tss_dist",
        "gc_content",
        "median_neighbor_distance",
    ]

    # 1. Mean, sd, min, max, count in one query
    agg_exprs = ", ".join(
        f"AVG({col}) AS {col}_mean, "
        f"STDDEV({col}) AS {col}_sd, "
        f"MIN({col}) AS {col}_min, "
        f"MAX({col}) AS {col}_max, "
        f"COUNT({col}) AS {col}_n"
        for col in scalar_columns
    )
    sql = text(f"SELECT {agg_exprs} FROM bed_stats WHERE id = ANY(:bed_ids)")
    row = session.execute(sql, {"bed_ids": bed_ids}).one()

    result = {}
    for col in scalar_columns:
        n = getattr(row, f"{col}_n")
        if not n:
            continue
        mean_val = float(getattr(row, f"{col}_mean"))
        sd_val = float(getattr(row, f"{col}_sd") or 0.0)
        col_min = float(getattr(row, f"{col}_min"))
        col_max = float(getattr(row, f"{col}_max"))

        # 2. Histogram via width_bucket (PostgreSQL)
        histogram = _sql_histogram(session, bed_ids, col, col_min, col_max, n)

        result[col] = {
            "mean": mean_val,
            "sd": sd_val,
            "n": n,
            "histogram": histogram,
        }

    return result if result else None


def _sql_histogram(
    session: Session,
    bed_ids: List[str],
    column: str,
    col_min: float,
    col_max: float,
    n: int,
) -> dict:
    """Build a histogram for a single scalar column using width_bucket."""
    num_bins = min(_SCALAR_HIST_BINS, max(3, math.ceil(math.sqrt(n))))

    if col_min == col_max:
        # All values identical — single bin
        return {
            "counts": [n],
            "edges": [col_min, col_max],
        }

    sql = text(
        f"""
        SELECT
            width_bucket({column}, :lo, :hi, :bins) AS bucket,
            COUNT(*) AS cnt
        FROM bed_stats
        WHERE id = ANY(:bed_ids) AND {column} IS NOT NULL
        GROUP BY bucket
        ORDER BY bucket
        """
    )
    rows = session.execute(
        sql,
        {"bed_ids": bed_ids, "lo": col_min, "hi": col_max, "bins": num_bins},
    ).all()

    # width_bucket returns 1..num_bins (in-range) plus 0 (below) and num_bins+1 (above/equal to hi)
    counts = [0] * num_bins
    for bucket, cnt in rows:
        if bucket == 0:
            counts[0] += cnt
        elif bucket > num_bins:
            counts[-1] += cnt
        else:
            counts[bucket - 1] += cnt

    # Compute edges
    step = (col_max - col_min) / num_bins
    edges = [col_min + i * step for i in range(num_bins + 1)]

    return {"counts": counts, "edges": edges}


def _sql_aggregate_region_distribution(
    session: Session, bed_ids: List[str]
) -> Optional[dict]:
    """Aggregate per-chromosome region_distribution via SQL JSONB unnest.

    Requires that member files used gtars >= PR #248 with --chrom-sizes so that
    bin widths are reference-aligned across files (same bin_idx -> same bp
    range on a given chromosome, regardless of file).

    Returns {chrom: {mean: [...], sd: [...], n: int}} or None if no data.
    """
    sql = text(
        """
        WITH per_file AS (
            SELECT distributions->'distributions'->'region_distribution' AS rd
            FROM bed_stats
            WHERE id = ANY(:bed_ids)
              AND distributions IS NOT NULL
              AND distributions->'distributions'->'region_distribution' IS NOT NULL
        ),
        unnested AS (
            SELECT
                chrom,
                ordinality - 1 AS bin_idx,
                (val)::float AS count
            FROM per_file,
                 jsonb_each(rd) AS per_chrom(chrom, counts),
                 jsonb_array_elements_text(counts) WITH ORDINALITY AS t(val, ordinality)
        )
        SELECT
            chrom,
            bin_idx,
            AVG(count) AS mean,
            COALESCE(STDDEV(count), 0.0) AS sd,
            COUNT(*) AS n
        FROM unnested
        GROUP BY chrom, bin_idx
        ORDER BY chrom, bin_idx
        """
    )

    rows = session.execute(sql, {"bed_ids": bed_ids}).all()
    if not rows:
        return None

    result: Dict[str, dict] = {}
    for chrom, bin_idx, mean_val, sd_val, n_val in rows:
        if chrom not in result:
            result[chrom] = {"mean": [], "sd": [], "n": int(n_val)}
        while len(result[chrom]["mean"]) <= bin_idx:
            result[chrom]["mean"].append(0.0)
            result[chrom]["sd"].append(0.0)
        result[chrom]["mean"][bin_idx] = float(mean_val)
        result[chrom]["sd"][bin_idx] = float(sd_val)

    return result if result else None


def _sql_aggregate_tss_histogram(
    session: Session, bed_ids: List[str]
) -> Optional[dict]:
    """Aggregate fixed-axis tss_distances histogram via SQL.

    TSS distances use a fixed 100-bin axis (+/-100 kb), so element-wise
    AVG/STDDEV across files is valid without re-binning.

    Returns {mean: [...], sd: [...], n: int, x_min, x_max, bins} or None.
    """
    sql = text(
        """
        WITH per_file AS (
            SELECT
                distributions->'distributions'->'tss_distances'->'counts' AS counts,
                distributions->'distributions'->'tss_distances'->>'x_min' AS x_min,
                distributions->'distributions'->'tss_distances'->>'x_max' AS x_max,
                distributions->'distributions'->'tss_distances'->>'bins' AS bins
            FROM bed_stats
            WHERE id = ANY(:bed_ids)
              AND distributions IS NOT NULL
              AND distributions->'distributions'->'tss_distances'->'counts' IS NOT NULL
        ),
        unnested AS (
            SELECT
                ordinality - 1 AS bin_idx,
                (val)::float AS count,
                x_min, x_max, bins
            FROM per_file,
                 jsonb_array_elements_text(counts) WITH ORDINALITY AS t(val, ordinality)
        )
        SELECT
            bin_idx,
            AVG(count) AS mean,
            COALESCE(STDDEV(count), 0.0) AS sd,
            COUNT(*) AS n,
            MAX(x_min) AS x_min,
            MAX(x_max) AS x_max,
            MAX(bins) AS bins
        FROM unnested
        GROUP BY bin_idx
        ORDER BY bin_idx
        """
    )

    rows = session.execute(sql, {"bed_ids": bed_ids}).all()
    if not rows:
        return None

    n_bins = len(rows)
    result = {
        "mean": [0.0] * n_bins,
        "sd": [0.0] * n_bins,
        "n": int(rows[0][3]),
    }
    x_min, x_max, bins_str = rows[0][4], rows[0][5], rows[0][6]
    if x_min is not None:
        try:
            result["x_min"] = float(x_min)
            result["x_max"] = float(x_max)
            result["bins"] = int(bins_str) if bins_str else n_bins
        except (ValueError, TypeError):
            pass

    for bin_idx, mean_val, sd_val, _n, _xmin, _xmax, _bins in rows:
        result["mean"][bin_idx] = float(mean_val)
        result["sd"][bin_idx] = float(sd_val)

    return result


def _sql_aggregate_partitions(session: Session, bed_ids: List[str]) -> Optional[dict]:
    """Aggregate genomic partitions from flat percentage columns.

    Uses the pre-computed *_percentage columns on bed_stats, which are
    populated by both R and gtars backends for all beds.
    """
    partition_columns = [
        ("exon", "exon_percentage"),
        ("intron", "intron_percentage"),
        ("intergenic", "intergenic_percentage"),
        ("promoterprox", "promoterprox_percentage"),
        ("promotercore", "promotercore_percentage"),
        ("fiveutr", "fiveutr_percentage"),
        ("threeutr", "threeutr_percentage"),
    ]

    agg_exprs = ", ".join(
        f"AVG({col}) * 100 AS {name}_mean, "
        f"COALESCE(STDDEV({col}) * 100, 0.0) AS {name}_sd, "
        f"COUNT({col}) AS {name}_n"
        for name, col in partition_columns
    )
    sql = text(f"SELECT {agg_exprs} FROM bed_stats WHERE id = ANY(:bed_ids)")

    row = session.execute(sql, {"bed_ids": bed_ids}).one()

    result = {}
    for name, _col in partition_columns:
        n = getattr(row, f"{name}_n")
        if not n:
            continue
        result[name] = {
            "mean_pct": float(getattr(row, f"{name}_mean")),
            "sd_pct": float(getattr(row, f"{name}_sd")),
            "n": int(n),
        }

    return result if result else None
