"""Collection-level aggregation of per-file genomic distributions.

Core logic for computing mean + sd across files for every distribution curve
stored in bed_stats.distributions JSONB. Used by both BedAgentBedSet.create()
and BedAgentBedFile.aggregate_collection().
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional

import numpy as np
from sqlalchemy import select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from bbconf.const import PKG_NAME
from bbconf.db_utils import Bed, BedMetadata, BedStats
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

    Uses SQL aggregation where distributions have stable axes (scalars,
    region_distribution with reference-aligned bin widths from gtars ≥#248,
    and fixed-axis tss_histogram). Partitions aggregation stays in Python
    (small nested JSONB).

    :param engine: SQLAlchemy engine
    :param bed_ids: list of bed file identifiers
    :param precision: decimal places for stored floats (default 3)
    :return: BedSetDistributions with aggregated distributions
    """
    if not bed_ids:
        return BedSetDistributions(n_files=0)

    n = len(bed_ids)

    with Session(engine) as session:
        # 1. Composition metadata (join Bed + BedMetadata)
        comp_rows = session.execute(
            select(
                Bed.id,
                Bed.genome_alias,
                BedMetadata.assay,
                BedMetadata.cell_type,
                BedMetadata.tissue,
                BedMetadata.target,
            )
            .outerjoin(BedMetadata, BedMetadata.id == Bed.id)
            .where(Bed.id.in_(bed_ids))
        ).all()

        # 2. Scalar columns — pulled directly from BedStats (SQL-side agg)
        scalar_rows = session.execute(
            select(
                BedStats.number_of_regions,
                BedStats.mean_region_width,
                BedStats.median_tss_dist,
                BedStats.gc_content,
                BedStats.median_neighbor_distance,
            ).where(BedStats.id.in_(bed_ids))
        ).all()

        # 3. region_distribution aggregation via SQL JSONB unnest
        region_distribution = _sql_aggregate_region_distribution(session, bed_ids)

        # 4. tss_histogram aggregation via SQL JSONB unnest
        tss_histogram = _sql_aggregate_tss_histogram(session, bed_ids)

        # 5. partitions (kept in Python — small nested JSONB)
        partitions_rows = session.execute(
            select(BedStats.distributions["partitions"])
            .where(BedStats.id.in_(bed_ids))
            .where(BedStats.distributions.isnot(None))
        ).all()

    # Assemble result
    stats = BedSetDistributions(
        n_files=n,
        composition=_aggregate_composition(comp_rows),
        scalar_summaries=_aggregate_scalars_from_columns(scalar_rows),
        tss_histogram=tss_histogram,
        region_distribution=region_distribution,
        partitions=_aggregate_partitions_from_rows(partitions_rows),
    )

    if precision is not None:
        stats = BedSetDistributions(**round_floats(stats.model_dump(), precision))

    return stats


# ---------------------------------------------------------------------------
# Private aggregation helpers
# ---------------------------------------------------------------------------


def _aggregate_composition(rows: list) -> Optional[dict]:
    """Count distinct values per metadata column."""
    if not rows:
        return None

    fields = ["genome_alias", "assay", "cell_type", "tissue", "target"]
    result = {}
    for i, field in enumerate(fields):
        counts: Dict[str, int] = defaultdict(int)
        for row in rows:
            val = row[i + 1]  # offset by 1 because row[0] is id
            if val:
                counts[val] += 1
        if counts:
            result[field] = dict(counts)

    return result if result else None


def _aggregate_scalars_from_columns(scalar_rows: list) -> Optional[dict]:
    """Compute mean/sd/histogram for per-file scalar columns.

    Input: list of tuples (number_of_regions, mean_region_width,
    median_tss_dist, gc_content, median_neighbor_distance) from BedStats.
    """
    scalar_keys = [
        "number_of_regions",
        "mean_region_width",
        "median_tss_dist",
        "gc_content",
        "median_neighbor_distance",
    ]

    per_key: Dict[str, list] = {k: [] for k in scalar_keys}
    for row in scalar_rows:
        for i, k in enumerate(scalar_keys):
            val = row[i]
            if val is not None:
                per_key[k].append(float(val))

    result = {}
    for k in scalar_keys:
        vals = per_key[k]
        if not vals:
            continue
        arr = np.array(vals)
        hist_counts, hist_edges = np.histogram(
            arr, bins=min(_SCALAR_HIST_BINS, len(vals))
        )
        result[k] = {
            "mean": float(np.mean(arr)),
            "sd": float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0,
            "n": len(vals),
            "histogram": {
                "counts": hist_counts.tolist(),
                "edges": hist_edges.tolist(),
            },
        }

    return result if result else None


def _sql_aggregate_region_distribution(
    session: Session, bed_ids: List[str]
) -> Optional[dict]:
    """Aggregate per-chromosome region_distribution via SQL JSONB unnest.

    Requires that member files used gtars ≥ PR #248 with --chrom-sizes so that
    bin widths are reference-aligned across files (same bin_idx → same bp
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

    # Reshape flat rows → {chrom: {mean: [...], sd: [...], n: int}}
    result: Dict[str, dict] = {}
    for chrom, bin_idx, mean_val, sd_val, n_val in rows:
        if chrom not in result:
            result[chrom] = {"mean": [], "sd": [], "n": int(n_val)}
        # Ensure arrays are long enough (bin_idx is 0-indexed, ordered by SQL)
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

    TSS distances use a fixed 100-bin axis (±100 kb), so element-wise summation
    across files is valid without re-binning.

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


def _aggregate_partitions_from_rows(partitions_rows: list) -> Optional[dict]:
    """Aggregate partitions from JSONB rows pulled from the DB.

    Each row is a single-element tuple containing the partitions sub-dict
    (or None). Partitions structure: {counts: [[name, count], ...], total: int}.
    """
    dists = [{"partitions": row[0]} for row in partitions_rows if row[0] is not None]
    if not dists:
        return None
    return _aggregate_partitions(dists)


def _aggregate_partitions(
    distributions_list: List[dict],
) -> Optional[dict]:
    """Aggregate genomic partitions across files as percentages."""
    file_partitions = []
    for dist in distributions_list:
        parts = dist.get("partitions")
        if not parts:
            continue
        total = parts.get("total", 0)
        counts = parts.get("counts")
        if not counts or total <= 0:
            continue
        pcts = {name: count / total * 100 for name, count in counts}
        file_partitions.append(pcts)

    if not file_partitions:
        return None

    all_cats = set()
    for fp in file_partitions:
        all_cats.update(fp.keys())

    result = {}
    for cat in sorted(all_cats):
        vals = [fp.get(cat, 0.0) for fp in file_partitions]
        arr = np.array(vals)
        result[cat] = {
            "mean_pct": float(np.mean(arr)),
            "sd_pct": float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0,
            "n": len(vals),
        }

    return result if result else None
