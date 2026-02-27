import pytest
from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from bbconf.db_utils import BedSets
from bbconf.exceptions import BedbaseS3ConnectionError, BedSetNotFoundError
from bbconf.modules.aggregation import aggregate_collection
from bbconf.modules.bedsets import _old_stats_to_scalar_summaries

from .conftest import SERVICE_UNAVAILABLE
from .utils import BED_TEST_ID, BED_TEST_ID_2, BEDSET_TEST_ID, ContextManagerDBTesting


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
class TestBedset:
    def test_aggregate_collection(self, bbagent_obj):
        """Test new aggregate_collection returns valid BedSetStats."""
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            results = aggregate_collection(
                bbagent_obj.config.db_engine.engine, [BED_TEST_ID]
            )

            assert results is not None
            assert results.n_files == 1
            assert results.scalar_summaries is not None
            assert "number_of_regions" in results.scalar_summaries
            assert results.tss_histogram is not None
            assert results.widths_histogram is not None
            assert results.neighbor_distances is not None
            assert results.gc_content is not None
            assert results.region_distribution is not None
            assert results.partitions is not None
            assert results.chromosome_summaries is not None

    def test_aggregate_collection_via_bedfile_agent(self, bbagent_obj):
        """Test the thin wrapper on BedAgentBedFile."""
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            results = bbagent_obj.bed.aggregate_collection([BED_TEST_ID])

            assert results is not None
            assert results.n_files == 1
            assert results.scalar_summaries is not None

    def test_crate_bedset_all(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=False
        ):
            bbagent_obj.bedset.create(
                "testinoo",
                "test_name",
                description="this is test description",
                bedid_list=[
                    BED_TEST_ID,
                ],
                statistics=True,
                upload_pephub=False,
                no_fail=True,
            )
            with Session(bbagent_obj.config.db_engine.engine) as session:
                result = session.scalar(select(BedSets).where(BedSets.id == "testinoo"))
                assert result is not None
                assert result.name == "test_name"
                # Verify new bedset_stats column is populated
                assert result.bedset_stats is not None
                assert result.bedset_stats["n_files"] == 1
                assert result.bedset_stats["scalar_summaries"] is not None

    def test_get_metadata_full(self, bbagent_obj):
        """Test full metadata retrieval — old bedset data hits fallback path."""
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get(BEDSET_TEST_ID, full=True)

            assert result.id == BEDSET_TEST_ID
            assert result.md5sum == "bbad0000000000000000000000000000"
            # Old records use fallback: scalar_summaries from bedset_means
            assert result.statistics is not None
            assert result.statistics.n_files == 0  # unknown for old records
            assert result.statistics.scalar_summaries is not None

    def test_get_metadata_not_full(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get(BEDSET_TEST_ID, full=False)

            assert result.id == BEDSET_TEST_ID
            assert result.md5sum == "bbad0000000000000000000000000000"
            assert result.statistics is None

    def test_get_not_found(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            with pytest.raises(BedSetNotFoundError):
                bbagent_obj.bedset.get("not_uid", full=True)

    def test_get_object(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_objects(BEDSET_TEST_ID)

            assert len(result) == 1

    def test_get_stats(self, bbagent_obj):
        """Test get_statistics — old bedset data hits fallback path."""
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_statistics(BEDSET_TEST_ID)

            # Old records: fallback produces scalar_summaries from old columns
            assert result.n_files == 0
            assert result.scalar_summaries is not None

    def test_get_bedset_list(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_ids_list(limit=100, offset=0)

            assert result.count == 1
            assert result.limit == 100
            assert result.offset == 0
            assert len(result.results) == 1
            assert result.results[0].id == BEDSET_TEST_ID

    def test_get_bedset_list_offset(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_ids_list(limit=100, offset=1)

            # assert result.count == 1
            assert result.limit == 100
            assert result.offset == 1
            assert len(result.results) == 0

    def test_get_idset_list_query_found(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_ids_list(query="rando", limit=100, offset=0)

            assert result.count == 1
            assert result.limit == 100
            assert result.offset == 0
            assert len(result.results) == 1

    def test_get_idset_list_query_fail(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_ids_list(
                query="rando1", limit=100, offset=0
            )

            assert result.count == 0
            assert result.limit == 100
            assert result.offset == 0
            assert len(result.results) == 0

    def test_get_get_bedset_bedfiles(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_bedset_bedfiles(BEDSET_TEST_ID)

            assert result.count == 1
            assert len(result.results) == 1

    def test_delete(self, bbagent_obj, mocker):
        mocker.patch("bbconf.config_parser.bedbaseconfig.BedBaseConfig.delete_s3")
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            bbagent_obj.bedset.delete(BEDSET_TEST_ID)
            assert not bbagent_obj.bedset.exists(BEDSET_TEST_ID)

    def test_delete_none(self, bbagent_obj, mocker):
        mocker.patch("bbconf.config_parser.bedbaseconfig.BedBaseConfig.delete_s3")
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            bbagent_obj.bedset.delete(BEDSET_TEST_ID)
            with pytest.raises(BedSetNotFoundError):
                bbagent_obj.bedset.delete(BEDSET_TEST_ID)

    def test_delete_s3_error(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            with pytest.raises(BedbaseS3ConnectionError):
                bbagent_obj.bedset.delete(BEDSET_TEST_ID)

    def test_bedset_smoketest(self, bbagent_obj):
        """End-to-end: create bedset with stats, read back via every accessor, then delete."""
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=False
        ):
            bedset_id = "smoketest_bedset"
            bed_ids = [BED_TEST_ID]

            # --- Create ---
            bbagent_obj.bedset.create(
                identifier=bedset_id,
                name="Smoke Test",
                description="end-to-end smoketest",
                bedid_list=bed_ids,
                statistics=True,
                annotation={"author": "test", "source": "pytest", "summary": "smoke"},
            )
            assert bbagent_obj.bedset.exists(bedset_id)

            # --- get() without full ---
            meta = bbagent_obj.bedset.get(bedset_id, full=False)
            assert meta.id == bedset_id
            assert meta.name == "Smoke Test"
            assert meta.description == "end-to-end smoketest"
            assert meta.md5sum is not None
            assert meta.bed_ids == bed_ids
            assert meta.statistics is None  # not requested

            # --- get() with full ---
            meta_full = bbagent_obj.bedset.get(bedset_id, full=True)
            stats = meta_full.statistics
            assert stats is not None
            assert stats.n_files == 1
            assert stats.scalar_summaries is not None
            assert "number_of_regions" in stats.scalar_summaries
            nr = stats.scalar_summaries["number_of_regions"]
            assert "mean" in nr
            assert nr["n"] == 1
            # Distribution curves should be populated from the test fixture
            assert stats.tss_histogram is not None
            assert stats.widths_histogram is not None
            assert stats.neighbor_distances is not None
            assert stats.gc_content is not None
            assert stats.region_distribution is not None
            assert stats.partitions is not None
            assert stats.chromosome_summaries is not None

            # --- get_statistics() ---
            stats2 = bbagent_obj.bedset.get_statistics(bedset_id)
            assert stats2.n_files == stats.n_files
            assert stats2.scalar_summaries == stats.scalar_summaries

            # --- get_bedset_bedfiles() ---
            bedfiles = bbagent_obj.bedset.get_bedset_bedfiles(bedset_id)
            assert bedfiles.count == 1
            assert bedfiles.results[0].id == BED_TEST_ID

            # --- get_ids_list() ---
            listing = bbagent_obj.bedset.get_ids_list(query="Smoke", limit=10, offset=0)
            assert listing.count == 1
            assert listing.results[0].id == bedset_id

            # --- get_bedset_pep() ---
            pep = bbagent_obj.bedset.get_bedset_pep(bedset_id)
            assert pep["_config"]["name"] == bedset_id
            assert len(pep["_sample_dict"]) == 1

            # --- delete ---
            bbagent_obj.bedset.delete(bedset_id)
            assert not bbagent_obj.bedset.exists(bedset_id)

    def test_aggregate_collection_empty_ids(self, bbagent_obj):
        """aggregate_collection with empty list returns n_files=0."""
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            result = aggregate_collection(
                bbagent_obj.config.db_engine.engine, []
            )
            assert result.n_files == 0
            assert result.scalar_summaries is None
            assert result.tss_histogram is None

    def test_aggregate_collection_nonexistent_ids(self, bbagent_obj):
        """aggregate_collection with IDs not in DB returns n_files > 0 but empty distributions."""
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            result = aggregate_collection(
                bbagent_obj.config.db_engine.engine,
                ["nonexistent_id_1", "nonexistent_id_2"],
            )
            assert result.n_files == 2
            # No distributions found, so all distribution fields should be None
            assert result.tss_histogram is None
            assert result.widths_histogram is None
            assert result.neighbor_distances is None
            assert result.gc_content is None
            assert result.region_distribution is None
            assert result.partitions is None
            assert result.chromosome_summaries is None

    def test_aggregate_collection_scalar_values(self, bbagent_obj):
        """Verify scalar summary values are correct for single-file aggregation."""
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            result = aggregate_collection(
                bbagent_obj.config.db_engine.engine, [BED_TEST_ID]
            )
            nr = result.scalar_summaries["number_of_regions"]
            assert nr["mean"] == 1500.0  # from test fixture scalars
            assert nr["n"] == 1
            assert nr["sd"] == 0.0  # single file → sd=0

    def test_aggregate_two_files(self, bbagent_obj):
        """Aggregate two BED files and verify mean/sd are nontrivial.

        File 1 scalars: number_of_regions=1500, gc_content=0.45
        File 2 scalars: number_of_regions=2500, gc_content=0.55
        Expected mean: 2000, sd: ~707.1 (population sd=500, sample sd=707.1)
        """
        import math

        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, add_data_2=True
        ):
            result = aggregate_collection(
                bbagent_obj.config.db_engine.engine,
                [BED_TEST_ID, BED_TEST_ID_2],
            )

            assert result.n_files == 2

            # --- Scalars: verify actual mean/sd math ---
            nr = result.scalar_summaries["number_of_regions"]
            assert nr["n"] == 2
            assert nr["mean"] == 2000.0  # (1500 + 2500) / 2
            # sample sd with ddof=1: sqrt(((1500-2000)^2 + (2500-2000)^2) / 1) = ~707.1
            assert abs(nr["sd"] - 500 * math.sqrt(2)) < 1.0

            gc = result.scalar_summaries["gc_content"]
            assert gc["n"] == 2
            assert abs(gc["mean"] - 0.50) < 0.001  # (0.45 + 0.55) / 2

            # --- Histograms: stacked, should have mean + sd arrays ---
            tss = result.tss_histogram
            assert tss is not None
            assert tss["n"] == 2
            assert len(tss["mean"]) == tss["bins"]  # same bin count as input
            assert len(tss["sd"]) == tss["bins"]
            # sd should be nonzero (files have different count magnitudes)
            assert any(v > 0 for v in tss["sd"])

            # --- Variable histograms (widths): re-binned to common grid ---
            w = result.widths_histogram
            assert w is not None
            assert w["n"] == 2
            # Global range should span both files: min(150, 200), max(1018, 1200)
            assert w["x_min"] <= 150.0
            assert w["x_max"] >= 1200.0
            assert len(w["mean"]) == 256  # _COMMON_GRID_SIZE
            assert any(v > 0 for v in w["sd"])

            # --- KDEs: interpolated to common grid ---
            nd = result.neighbor_distances
            assert nd is not None
            assert nd["n"] == 2
            assert nd["x_min"] <= 1.2  # min of both files
            assert nd["x_max"] >= 6.5  # max of both files
            assert len(nd["mean"]) == 256
            assert any(v > 0 for v in nd["sd"])

            # --- Region distribution: per-chromosome mean/sd ---
            rd = result.region_distribution
            assert rd is not None
            assert "chr1" in rd
            assert rd["chr1"]["n"] == 2
            # chr1 file1: [10, 5, ...], file2: [20, 10, ...] → mean[0] = 15
            assert abs(rd["chr1"]["mean"][0] - 15.0) < 0.01
            assert rd["chr1"]["sd"][0] > 0  # nonzero sd

            # --- Partitions: percentage-based mean/sd ---
            p = result.partitions
            assert p is not None
            # Both files have all 7 partition categories
            assert len(p) == 7
            assert "exon" in p
            assert p["exon"]["n"] == 2
            assert p["exon"]["sd_pct"] > 0  # different totals → different pcts → nonzero sd

            # --- Chromosome summaries: mean/sd of numeric stats ---
            cs = result.chromosome_summaries
            assert cs is not None
            assert "chr1" in cs
            assert cs["chr1"]["n"] == 2
            # chr1 number_of_regions: file1=800, file2=1400 → mean=1100
            assert abs(cs["chr1"]["number_of_regions"]["mean"] - 1100.0) < 0.01
            assert cs["chr1"]["number_of_regions"]["sd"] > 0

    def test_retrieve_unprocessed(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_unprocessed()
            assert result.count == 1


class TestOldStatsToScalarSummaries:
    """Unit tests for the fallback converter (no DB needed)."""

    def test_basic_conversion(self):
        means = {"number_of_regions": 1500, "mean_region_width": 300.5, "gc_content": 0.45}
        sd = {"number_of_regions": 100, "mean_region_width": 20.0, "gc_content": 0.05}
        result = _old_stats_to_scalar_summaries(means, sd)

        assert result is not None
        assert "number_of_regions" in result
        assert result["number_of_regions"]["mean"] == 1500.0
        assert result["number_of_regions"]["sd"] == 100.0
        assert result["number_of_regions"]["n"] == 0  # unknown for old records
        assert result["gc_content"]["mean"] == 0.45
        assert result["gc_content"]["sd"] == 0.05

    def test_missing_sd(self):
        """When sd dict is None, entries should have no 'sd' key."""
        means = {"number_of_regions": 1500}
        result = _old_stats_to_scalar_summaries(means, None)

        assert result is not None
        assert result["number_of_regions"]["mean"] == 1500.0
        assert "sd" not in result["number_of_regions"]

    def test_partial_sd(self):
        """When sd dict exists but is missing a key, that entry has no 'sd'."""
        means = {"number_of_regions": 1500, "gc_content": 0.45}
        sd = {"gc_content": 0.05}  # no sd for number_of_regions
        result = _old_stats_to_scalar_summaries(means, sd)

        assert "sd" not in result["number_of_regions"]
        assert result["gc_content"]["sd"] == 0.05

    def test_empty_means(self):
        result = _old_stats_to_scalar_summaries({}, {})
        assert result is None

    def test_none_means(self):
        result = _old_stats_to_scalar_summaries(None, None)
        assert result is None

    def test_irrelevant_keys_ignored(self):
        """Keys not in the scalar_keys list are ignored."""
        means = {"number_of_regions": 100, "some_random_stat": 42}
        result = _old_stats_to_scalar_summaries(means, {})
        assert "some_random_stat" not in result
        assert "number_of_regions" in result
