import pytest
from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from bbconf.bbagent import BedBaseAgent
from bbconf.const import DEFAULT_LICENSE
from bbconf.db_utils import Bed, Files
from bbconf.exceptions import BedFIleExistsError, BEDFileNotFoundError

from .conftest import SERVICE_UNAVAILABLE, get_bbagent
from .utils import BED_TEST_ID, ContextManagerDBTesting


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

    def test_get_all(self, bbagent_obj, mocked_phc):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get(BED_TEST_ID, full=True)
            assert return_result is not None
            assert return_result.files is not None
            assert return_result.plots is not None

            # TODO: PEPhub is disabled
            # assert return_result.raw_metadata is not None

            assert return_result.genome_alias == "hg38"
            assert return_result.stats.number_of_regions == 1

            assert return_result.files.bed_file is not None
            assert return_result.plots.chrombins is not None
            assert return_result.license_id == DEFAULT_LICENSE

    def test_get_all_not_found(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get(BED_TEST_ID, full=False)

            assert return_result is not None
            assert return_result.files is None
            assert return_result.plots is None
            assert return_result.raw_metadata is None
            assert return_result.stats is None

            assert return_result.genome_alias == "hg38"
            assert return_result.id == BED_TEST_ID

    @pytest.mark.skip(
        "Skipped, because PHC is disabled"
    )  # TODO: should we disable PHC everywhere?
    def test_get_raw_metadata(self, bbagent_obj, mocked_phc):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.get_raw_metadata(BED_TEST_ID)

            assert return_result is not None
            assert return_result.sample_name == BED_TEST_ID

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
        assert return_result.count == 1
        assert return_result.offset == 1

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
            assert bed_file.annotation.cell_line == ""

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


@pytest.mark.skip("Skipped, because ML models and qdrant needed")
class TestVectorSearch:
    def test_qdrant_search(self, bbagent_obj, mocker):
        mocker.patch(
            "geniml.text2bednn.text2bednn.Text2BEDSearchInterface.nl_vec_search",
            return_value={
                "id": BED_TEST_ID,
                "payload": {"bed_id": "39b686ec08206b92b540ed434266ec9b"},
                "score": 0.2146723,
            },
        )
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            return_result = bbagent_obj.bed.text_to_bed_search("something")
        assert return_result

    def test_delete_qdrant_point(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            bbagent_obj.bed.delete_qdrant_point(BED_TEST_ID)

    def test_create_qdrant_collection(self):
        agent = BedBaseAgent(
            config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml"
        )
        ff = agent.bed.create_qdrant_collection()
        ff
        assert True
