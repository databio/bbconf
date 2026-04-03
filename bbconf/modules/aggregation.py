"""Collection-level aggregation of per-file genomic distributions.

Core logic for computing mean + sd across files for every distribution curve
stored in bed_stats.distributions JSONB. Used by both BedAgentBedSet.create()
and BedAgentBedFile.aggregate_collection().
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional

import numpy as np
from sqlalchemy import select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from bbconf.const import PKG_NAME
from bbconf.db_utils import Bed, BedMetadata, BedStats
from bbconf.models.bedset_models import BedSetDistributions

_LOGGER = logging.getLogger(PKG_NAME)

# Number of bins for re-binned histograms and interpolated KDEs
_COMMON_GRID_SIZE = 256
# Number of bins when compressing scalar arrays to histograms
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

    :param engine: SQLAlchemy engine
    :param bed_ids: list of bed file identifiers
    :param precision: decimal places for stored floats (default 3)
    :return: BedSetDistributions with aggregated distributions
    """
    if not bed_ids:
        return BedSetDistributions(n_files=0)

    n = len(bed_ids)

    with Session(engine) as session:
        # 1. Fetch full distributions for all matching IDs
        dist_stmt = (
            select(BedStats.id, BedStats.distributions)
            .where(BedStats.id.in_(bed_ids))
            .where(BedStats.distributions.isnot(None))
        )
        dist_rows = session.execute(dist_stmt).all()

        # 2. Fetch composition metadata
        comp_stmt = (
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
        )
        comp_rows = session.execute(comp_stmt).all()

        # 3. Fallback scalars for old records without distributions
        ids_with_dist = {row[0] for row in dist_rows}
        ids_without_dist = [bid for bid in bed_ids if bid not in ids_with_dist]
        fallback_rows = []
        if ids_without_dist:
            fb_stmt = select(
                BedStats.id,
                BedStats.number_of_regions,
                BedStats.mean_region_width,
                BedStats.median_tss_dist,
                BedStats.gc_content,
            ).where(BedStats.id.in_(ids_without_dist))
            fallback_rows = session.execute(fb_stmt).all()

    # Parse distributions from JSONB
    distributions_list = []
    for _id, dist in dist_rows:
        if dist:
            distributions_list.append(dist)

    # Build the stats object
    stats = BedSetDistributions(
        n_files=n,
        composition=_aggregate_composition(comp_rows),
        scalar_summaries=_aggregate_scalars(distributions_list, fallback_rows),
        tss_histogram=_aggregate_fixed_axis_from_dists(
            distributions_list, "tss_distances"
        ),
        widths_histogram=_aggregate_variable_histogram(distributions_list, "widths"),
        neighbor_distances=_aggregate_variable_kde(
            distributions_list, "neighbor_distances"
        ),
        gc_content=_aggregate_variable_kde(distributions_list, "gc_content"),
        region_distribution=_aggregate_region_distribution(distributions_list),
        partitions=_aggregate_partitions(distributions_list),
        chromosome_summaries=_aggregate_chromosome_stats(distributions_list),
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


def _aggregate_scalars(
    distributions_list: List[dict],
    fallback_rows: list,
) -> Optional[dict]:
    """Extract scalar values from distributions, compute mean + sd + histogram."""
    scalar_keys = [
        "number_of_regions",
        "mean_region_width",
        "median_tss_dist",
        "gc_content",
    ]

    per_key: Dict[str, list] = {k: [] for k in scalar_keys}

    # From distributions JSONB
    for dist in distributions_list:
        scalars = dist.get("scalars", {})
        if scalars:
            for k in scalar_keys:
                val = scalars.get(k)
                if val is not None:
                    per_key[k].append(float(val))

    # From fallback rows (old records)
    for row in fallback_rows:
        for i, k in enumerate(scalar_keys):
            val = row[i + 1]
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


def _aggregate_fixed_axis_from_dists(
    distributions_list: List[dict],
    key: str,
) -> Optional[dict]:
    """Aggregate fixed-axis histograms (e.g. TSS distances) from distributions."""
    arrays = []
    metadata = None

    for dist in distributions_list:
        dists = dist.get("distributions", {})
        obj = dists.get(key)
        if not obj:
            continue
        counts = obj.get("counts")
        if counts is None:
            continue
        arrays.append(counts)
        if metadata is None:
            metadata = {
                "x_min": obj.get("x_min"),
                "x_max": obj.get("x_max"),
                "bins": obj.get("bins"),
            }

    if not arrays:
        return None

    return _aggregate_fixed_axis(arrays, metadata)


def _aggregate_fixed_axis(
    arrays: List[list],
    metadata: Optional[dict] = None,
) -> Optional[dict]:
    """Stack equal-length arrays, compute element-wise mean + sd."""
    if not arrays:
        return None

    lengths = [len(a) for a in arrays]
    target_len = max(set(lengths), key=lengths.count)
    filtered = [np.array(a, dtype=float) for a in arrays if len(a) == target_len]

    if not filtered:
        return None

    stacked = np.stack(filtered)
    result = {
        "mean": np.mean(stacked, axis=0).tolist(),
        "sd": (
            np.std(stacked, axis=0, ddof=1).tolist()
            if len(filtered) > 1
            else [0.0] * target_len
        ),
        "n": len(filtered),
    }
    if metadata:
        result.update(metadata)
    return result


def _aggregate_variable_histogram(
    distributions_list: List[dict],
    key: str,
) -> Optional[dict]:
    """Aggregate variable-range histograms by re-binning to a common grid."""
    histograms = []
    for dist in distributions_list:
        dists = dist.get("distributions", {})
        obj = dists.get(key)
        if not obj:
            continue
        counts = obj.get("counts")
        x_min = obj.get("x_min")
        x_max = obj.get("x_max")
        if counts is None or x_min is None or x_max is None:
            continue
        histograms.append({"counts": counts, "x_min": x_min, "x_max": x_max})

    if not histograms:
        return None

    global_x_min = min(h["x_min"] for h in histograms)
    global_x_max = max(h["x_max"] for h in histograms)

    common_edges = np.linspace(global_x_min, global_x_max, _COMMON_GRID_SIZE + 1)

    rebinned = []
    for h in histograms:
        counts = np.array(h["counts"], dtype=float)
        n_bins = len(counts)
        orig_edges = np.linspace(h["x_min"], h["x_max"], n_bins + 1)

        cdf = np.concatenate([[0.0], np.cumsum(counts)])
        total = cdf[-1]
        if total == 0:
            rebinned.append(np.zeros(_COMMON_GRID_SIZE))
            continue

        cdf_interp = np.interp(common_edges, orig_edges, cdf)
        new_counts = np.diff(cdf_interp)
        new_counts = (
            new_counts * (total / new_counts.sum())
            if new_counts.sum() > 0
            else new_counts
        )
        rebinned.append(new_counts)

    stacked = np.stack(rebinned)
    result = {
        "x_min": float(global_x_min),
        "x_max": float(global_x_max),
        "bins": _COMMON_GRID_SIZE,
        "mean": np.mean(stacked, axis=0).tolist(),
        "sd": (
            np.std(stacked, axis=0, ddof=1).tolist()
            if len(rebinned) > 1
            else [0.0] * _COMMON_GRID_SIZE
        ),
        "n": len(rebinned),
    }
    return result


def _aggregate_variable_kde(
    distributions_list: List[dict],
    key: str,
) -> Optional[dict]:
    """Aggregate variable-range KDE density curves by interpolating to common x-axis."""
    kdes = []
    for dist in distributions_list:
        dists = dist.get("distributions", {})
        obj = dists.get(key)
        if not obj:
            continue
        densities = obj.get("densities")
        x_min = obj.get("x_min")
        x_max = obj.get("x_max")
        if densities is None or x_min is None or x_max is None:
            continue
        kdes.append({"densities": densities, "x_min": x_min, "x_max": x_max})

    if not kdes:
        return None

    global_x_min = min(k["x_min"] for k in kdes)
    global_x_max = max(k["x_max"] for k in kdes)

    common_x = np.linspace(global_x_min, global_x_max, _COMMON_GRID_SIZE)

    interpolated = []
    for k in kdes:
        dens = np.array(k["densities"], dtype=float)
        orig_x = np.linspace(k["x_min"], k["x_max"], len(dens))
        interp_dens = np.interp(common_x, orig_x, dens, left=0.0, right=0.0)
        interpolated.append(interp_dens)

    stacked = np.stack(interpolated)

    result = {
        "x_min": float(global_x_min),
        "x_max": float(global_x_max),
        "n_points": _COMMON_GRID_SIZE,
        "mean": np.mean(stacked, axis=0).tolist(),
        "sd": (
            np.std(stacked, axis=0, ddof=1).tolist()
            if len(interpolated) > 1
            else [0.0] * _COMMON_GRID_SIZE
        ),
        "n": len(interpolated),
    }

    # Include per-file summary stat if available (e.g. gc_content mean)
    file_means = []
    for dist in distributions_list:
        dists = dist.get("distributions", {})
        obj = dists.get(key, {})
        m = obj.get("mean")
        if m is not None:
            file_means.append(float(m))
    if file_means:
        result["file_mean"] = {
            "mean": float(np.mean(file_means)),
            "sd": float(np.std(file_means, ddof=1)) if len(file_means) > 1 else 0.0,
        }

    return result


def _aggregate_region_distribution(
    distributions_list: List[dict],
) -> Optional[dict]:
    """Aggregate per-chromosome region distributions."""
    chrom_data: Dict[str, List[list]] = defaultdict(list)
    for dist in distributions_list:
        dists = dist.get("distributions", {})
        rd = dists.get("region_distribution")
        if not rd:
            continue
        for chrom, counts in rd.items():
            if isinstance(counts, list):
                chrom_data[chrom].append(counts)

    if not chrom_data:
        return None

    result = {}
    for chrom, arrays in chrom_data.items():
        max_len = max(len(a) for a in arrays)
        padded = []
        for a in arrays:
            arr = np.array(a, dtype=float)
            if len(arr) < max_len:
                arr = np.pad(arr, (0, max_len - len(arr)))
            padded.append(arr)

        stacked = np.stack(padded)
        result[chrom] = {
            "mean": np.mean(stacked, axis=0).tolist(),
            "sd": (
                np.std(stacked, axis=0, ddof=1).tolist()
                if len(padded) > 1
                else [0.0] * max_len
            ),
            "n": len(padded),
        }

    return result if result else None


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


def _aggregate_chromosome_stats(
    distributions_list: List[dict],
) -> Optional[dict]:
    """Aggregate chromosome-level stats across files."""
    chrom_data: Dict[str, List[dict]] = defaultdict(list)
    for dist in distributions_list:
        chrom_stats = dist.get("chromosome_stats") or dist.get("distributions", {}).get(
            "chromosome_stats"
        )
        if not chrom_stats:
            continue
        if isinstance(chrom_stats, dict):
            for chrom, entry in chrom_stats.items():
                if isinstance(entry, dict):
                    chrom_data[chrom].append(entry)
        else:
            for entry in chrom_stats:
                chrom = entry.get("chromosome")
                if chrom:
                    chrom_data[chrom].append(entry)

    if not chrom_data:
        return None

    numeric_fields = set()
    for entries in chrom_data.values():
        for entry in entries:
            for k, v in entry.items():
                if k != "chromosome" and isinstance(v, (int, float)):
                    numeric_fields.add(k)
            break
        break

    result = {}
    for chrom, entries in sorted(chrom_data.items()):
        chrom_result = {"n": len(entries)}
        for field in sorted(numeric_fields):
            vals = [e.get(field) for e in entries if e.get(field) is not None]
            if vals:
                arr = np.array(vals, dtype=float)
                chrom_result[field] = {
                    "mean": float(np.mean(arr)),
                    "sd": float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0,
                }
        result[chrom] = chrom_result

    return result if result else None
