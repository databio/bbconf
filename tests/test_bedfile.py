import os

import numpy as np
import pytest
from gtars.models import RegionSet as GRegionSet
from qdrant_client.models import PointIdsList
from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from bbconf.bbagent import BedBaseAgent
from bbconf.const import DEFAULT_LICENSE
from bbconf.db_utils import Bed, Files
from bbconf.exceptions import BedFIleExistsError, BEDFileNotFoundError
from bbconf.modules.bedfiles import _bed_id_from_point_id

from .conftest import DATA_PATH, SERVICE_UNAVAILABLE, get_bbagent
from .utils import BED_TEST_ID, BEDSET_TEST_ID, ContextManagerDBTesting


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
def test_bb_database():
    agent = get_bbagent()
    assert isinstance(agent, BedBaseAgent)


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
class Test_BedFile_Agent:
    def test_upload(self, bbagent_obj, example_dict, mocker):
        upload_s3_mock = mocker.patch(
            "bbconf.config_parser.bedbaseconfig.BedBaseConfig.upload_s3",
            return_value=True,
        )
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            bbagent_obj.bed.add(**example_dict)

            assert upload_s3_mock.called
            assert bbagent_obj.bed.exists(example_dict["identifier"])

    def test_upload_exists(self, bbagent_obj, example_dict, mocker):
        mocker.patch(
            "bbconf.config_parser.bedbaseconfig.BedBaseConfig.upload_s3",
            return_value=True,
        )
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            bbagent_obj.bed.add(**example_dict)
            with pytest.raises(BedFIleExistsError):
                bbagent_obj.bed.add(**example_dict)

    def test_add_nofail(self, bbagent_obj, example_dict, mocker):
        mocker.patch(
            "bbconf.config_parser.bedbaseconfig.BedBaseConfig.upload_s3",
            return_value=True,
        )

        example_dict["nofail"] = True
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=False):
            bbagent_obj.bed.add(**example_dict)
            bbagent_obj.bed.add(**example_dict)
            assert bbagent_obj.bed.exists(example_dict["identifier"])

    def test_get_all(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get(BED_TEST_ID, full=True)
            assert return_result is not None
            assert return_result.files is not None
            assert return_result.plots is not None

            assert return_result.genome_alias == "hg38"
            assert return_result.stats.number_of_regions == 1

            assert return_result.files.bed_file is not None
            assert return_result.plots.chrombins is not None
            assert return_result.license_id == DEFAULT_LICENSE

    def test_get_all_bedsets_bedfile_count(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            return_result = bbagent_obj.bed.get(BED_TEST_ID, full=True)

            assert len(return_result.bedsets) == 1
            assert return_result.bedsets[0].id == BEDSET_TEST_ID
            assert return_result.bedsets[0].bedfile_count == 1

    def test_get_all_not_found(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get(BED_TEST_ID, full=False)

            assert return_result is not None
            assert return_result.files is None
            assert return_result.plots is None
            assert return_result.stats is None

            assert return_result.genome_alias == "hg38"
            assert return_result.id == BED_TEST_ID

    def test_get_stats(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_stats(BED_TEST_ID)

            assert return_result is not None
            assert return_result.number_of_regions == 1

    def test_get_files(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_files(BED_TEST_ID)

            assert return_result is not None
            assert return_result.bed_file.path is not None

    def test_get_plots(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_plots(BED_TEST_ID)

        assert return_result is not None
        assert return_result.chrombins is not None

    def test_get_objects(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_objects(BED_TEST_ID)

        assert "bed_file" in return_result
        assert "chrombins" in return_result

    def test_get_classification(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_classification(BED_TEST_ID)

        assert return_result is not None
        assert return_result.bed_compliance == "bed6+4"

    def test_get_list(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_ids_list(limit=100, offset=0)

        assert len(return_result.results) == 1
        assert return_result.count == 1
        assert return_result.results[0].id == BED_TEST_ID
        assert return_result.limit == 100
        assert return_result.offset == 0

    def test_get_list_genome_true(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_ids_list(
                limit=100, offset=0, genome="hg38"
            )

        assert len(return_result.results) == 1
        assert return_result.count == 1
        assert return_result.results[0].id == BED_TEST_ID
        assert return_result.limit == 100
        assert return_result.offset == 0

    def test_get_list_genome_false(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_ids_list(
                limit=100, offset=0, genome="hg381"
            )

        assert len(return_result.results) == 0
        assert return_result.count == 0

    def test_get_list_bed_compliance_true(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_ids_list(
                limit=100, offset=0, bed_compliance="bed6+4"
            )

        assert len(return_result.results) == 1
        assert return_result.count == 1
        assert return_result.results[0].id == BED_TEST_ID
        assert return_result.limit == 100
        assert return_result.offset == 0

    def test_get_list_bed_compliance_false(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_ids_list(
                limit=100, offset=0, bed_compliance="bed6+5"
            )

        assert len(return_result.results) == 0
        assert return_result.count == 0

    def test_get_list_bed_offset(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_ids_list(
                limit=100,
                offset=1,
            )

        assert len(return_result.results) == 0
        # count is only computed on the first page
        assert return_result.count is None
        assert return_result.offset == 1

    def test_get_list_after_walks_every_id_once(self, bbagent_obj):
        extra_ids = [f"{i:032x}" for i in range(1, 8)]
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            with Session(bbagent_obj.config.db_engine.engine) as session:
                for bed_id in extra_ids:
                    session.add(Bed(id=bed_id, genome_alias="hg38", processed=False))
                session.commit()

            expected = sorted(extra_ids + [BED_TEST_ID])

            seen = []
            after = None
            while True:
                page = bbagent_obj.bed.get_ids_list(limit=3, after=after)
                if after is None:
                    assert page.count == len(expected)
                else:
                    assert page.count is None
                assert page.offset == 0
                if not page.results:
                    break
                seen.extend(r.id for r in page.results)
                after = page.results[-1].id

        assert seen == expected
        assert len(seen) == len(set(seen))

    def test_get_list_after_ignores_offset(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            with Session(bbagent_obj.config.db_engine.engine) as session:
                session.add(Bed(id="0" * 32, genome_alias="hg38", processed=False))
                session.commit()

            return_result = bbagent_obj.bed.get_ids_list(
                limit=100, offset=5, after="0" * 32
            )

        assert [r.id for r in return_result.results] == [BED_TEST_ID]
        assert return_result.count is None
        assert return_result.offset == 0

    def test_bed_delete(self, bbagent_obj, mocker):
        mocker.patch("bbconf.config_parser.bedbaseconfig.BedBaseConfig.delete_s3")
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            bbagent_obj.bed.delete(BED_TEST_ID)

            assert not bbagent_obj.bed.exists(BED_TEST_ID)

            with Session(bbagent_obj.config.db_engine.engine) as session:
                result = session.scalar(select(Bed).where(Bed.id == BED_TEST_ID))
                assert result is None

                result = session.scalars(select(Files))
                assert len([k for k in result]) == 0

    def test_bed_delete_not_found(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            with pytest.raises(BEDFileNotFoundError):
                bbagent_obj.bed.delete("not_found")

    def test_bed_update(self, bbagent_obj):

        # TODO: has to be expanded
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            bed_file = bbagent_obj.bed.get(BED_TEST_ID, full=True)
            # assert bed_file.annotation.model_dump(exclude_defaults=True) == {}
            assert bed_file.annotation.cell_line is None

            new_metadata = {
                "cell_line": "K562",
                "tissue": "blood",
            }
            bbagent_obj.bed.update(
                identifier=BED_TEST_ID,
                metadata=new_metadata,
                upload_qdrant=False,
                upload_s3=False,
            )

            new_bed_file = bbagent_obj.bed.get(BED_TEST_ID, full=True)

            assert new_bed_file.annotation.cell_line == "K562"

    def test_get_unprocessed(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_unprocessed(limit=100, offset=0)

            assert return_result.count == 1
            assert return_result.results[0].id == BED_TEST_ID

    def test_get_missing_plots(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_missing_plots(
                "tss_distance", limit=100, offset=0
            )

            assert return_result[0] == BED_TEST_ID


def test_bed_id_from_point_id():
    dashed = "-".join(
        [
            BED_TEST_ID[:8],
            BED_TEST_ID[8:12],
            BED_TEST_ID[12:16],
            BED_TEST_ID[16:20],
            BED_TEST_ID[20:],
        ]
    )
    assert _bed_id_from_point_id(dashed) == BED_TEST_ID
    assert _bed_id_from_point_id(BED_TEST_ID) == BED_TEST_ID


def _ml_unavailable() -> bool:
    """True unless ML models loaded and the Qdrant server answers."""
    if SERVICE_UNAVAILABLE:
        return True
    config = get_bbagent().config
    if getattr(config, "bivec_search_interface", None) is None:
        return True
    try:
        config.qdrant_client.get_collections()
    except Exception:
        return True
    return False


ML_UNAVAILABLE = _ml_unavailable()
QDRANT_TEXT_POINT_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture()
def qdrant_bed_point(bbagent_obj):
    """Load one BED vector and one matching text vector into Qdrant.

    The text vector is the real dense embedding of a metadata phrase, and its
    payload points at BED_TEST_ID, so a bivec text search can find the BED.
    """
    config = bbagent_obj.config
    qdrant_cfg = config.config.qdrant
    file_dim = config.qdrant_client.get_collection(
        qdrant_cfg.file_collection
    ).config.params.vectors.size
    config.qdrant_file_backend.load(
        ids=[BED_TEST_ID],
        vectors=np.random.default_rng(0).random((1, file_dim)),
        payloads=[{"description": "test bed"}],
    )
    text_vec = next(iter(config.dense_encoder.embed(["CTCF ChIP-seq in K562"])))
    config._qdrant_text_backend.load(
        ids=[QDRANT_TEXT_POINT_ID],
        vectors=np.array([text_vec]),
        payloads=[{"matched_files": [BED_TEST_ID]}],
    )
    yield
    config.qdrant_client.delete(
        collection_name=qdrant_cfg.file_collection,
        points_selector=PointIdsList(points=[BED_TEST_ID]),
    )
    config.qdrant_client.delete(
        collection_name=qdrant_cfg.text_collection,
        points_selector=PointIdsList(points=[QDRANT_TEXT_POINT_ID]),
    )


def _file_points_count(bbagent_obj) -> int:
    return bbagent_obj.config.qdrant_client.count(
        collection_name=bbagent_obj.config.config.qdrant.file_collection,
        exact=True,
    ).count


@pytest.mark.skipif(ML_UNAVAILABLE, reason="ML models or Qdrant server unavailable")
class TestVectorSearch:
    def test_text_to_bed_search(self, bbagent_obj, qdrant_bed_point):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.text_to_bed_search("CTCF ChIP-seq")

            assert return_result.count == 1
            assert len(return_result.results) == 1
            # Qdrant returns dashed UUIDs; search results use the plain BED id.
            assert return_result.results[0].id == BED_TEST_ID
            assert return_result.results[0].metadata.id == BED_TEST_ID

    def test_bed_to_bed_search(self, bbagent_obj, qdrant_bed_point):
        if bbagent_obj.config.b2b_search_interface is None:
            pytest.skip("Region2vec model unavailable")
        region_set = GRegionSet(
            os.path.join(DATA_PATH, "files", f"{BED_TEST_ID}.bed.gz")
        )
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.bed_to_bed_search(region_set, limit=5)

            assert len(return_result.results) == 1
            assert return_result.results[0].id == BED_TEST_ID
            assert return_result.results[0].metadata.id == BED_TEST_ID

    def test_delete_qdrant_point(self, bbagent_obj, qdrant_bed_point):
        assert _file_points_count(bbagent_obj) == 1
        bbagent_obj.bed.delete_qdrant_point(BED_TEST_ID)
        assert _file_points_count(bbagent_obj) == 0

    def test_qdrant_collections_created(self, bbagent_obj):
        """Agent init creates every configured Qdrant collection."""
        qdrant_cfg = bbagent_obj.config.config.qdrant
        for collection in (
            qdrant_cfg.file_collection,
            qdrant_cfg.text_collection,
            qdrant_cfg.hybrid_collection,
        ):
            assert bbagent_obj.config.qdrant_client.collection_exists(collection)
