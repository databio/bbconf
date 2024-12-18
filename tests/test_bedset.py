import os

import pytest
from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from bbconf.db_utils import BedSets
from bbconf.exceptions import BedbaseS3ConnectionError, BedSetNotFoundError

from .conftest import DATA_PATH, SERVICE_UNAVAILABLE
from .utils import BED_TEST_ID, BEDSET_TEST_ID, ContextManagerDBTesting


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
class TestBedset:
    def test_calculate_stats(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            results = bbagent_obj.bedset._calculate_statistics([BED_TEST_ID])

            assert results is not None
            assert results.sd is not None
            assert results.mean is not None

    def test_crate_bedset_all(self, bbagent_obj, mocker):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=False
        ):
            mocker.patch(
                "bbconf.config_parser.bedbaseconfig.BedBaseConfig.upload_s3",
                return_value=True,
            )
            bbagent_obj.bedset.create(
                "testinoo",
                "test_name",
                description="this is test description",
                bedid_list=[
                    BED_TEST_ID,
                ],
                plots={
                    "region_commonality": {
                        "name": "region_commonality",
                        "description": "Regions distribution over chromosomes",
                        "title": "Regions distribution over chromosomes",
                        "path": os.path.join(
                            DATA_PATH,
                            "plots/bbad85f21962bb8d972444f7f9a3a932_chrombins.pdf",
                        ),
                        "path_thumbnail": os.path.join(
                            DATA_PATH,
                            "/plots/bbad85f21962bb8d972444f7f9a3a932_chrombins.png",
                        ),
                    },
                },
                statistics=True,
                upload_s3=True,
                upload_pephub=False,
                no_fail=True,
            )
            with Session(bbagent_obj.config.db_engine.engine) as session:
                result = session.scalar(select(BedSets).where(BedSets.id == "testinoo"))
                assert result is not None
                assert result.name == "test_name"
                assert len([k for k in result.files]) == 1

    def test_get_metadata_full(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get(BEDSET_TEST_ID, full=True)

            assert result.id == BEDSET_TEST_ID
            assert result.md5sum == "bbad0000000000000000000000000000"
            assert result.statistics.sd is not None
            assert result.statistics.mean is not None
            assert result.plots is not None

    def test_get_metadata_not_full(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get(BEDSET_TEST_ID, full=False)

            assert result.id == BEDSET_TEST_ID
            assert result.md5sum == "bbad0000000000000000000000000000"
            assert result.statistics is None
            assert result.plots is None

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

    def test_get_plots(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_plots(BEDSET_TEST_ID)

            assert result is not None

    def test_get_stats(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_statistics(BEDSET_TEST_ID)

            assert result.sd is not None
            assert result.mean is not None

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
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            mocker.patch(
                "bbconf.config_parser.bedbaseconfig.BedBaseConfig.delete_s3",
                return_value=True,
            )
            bbagent_obj.bedset.delete(BEDSET_TEST_ID)

            assert not bbagent_obj.bedset.exists(BEDSET_TEST_ID)

    def test_delete_none(self, bbagent_obj, mocker):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            mocker.patch(
                "bbconf.config_parser.bedbaseconfig.BedBaseConfig.delete_s3",
                return_value=True,
            )
            bbagent_obj.bedset.delete(BEDSET_TEST_ID)

            with pytest.raises(BedSetNotFoundError):
                bbagent_obj.bedset.delete(BEDSET_TEST_ID)

    def test_delete_s3_error(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            with pytest.raises(BedbaseS3ConnectionError):
                bbagent_obj.bedset.delete(BEDSET_TEST_ID)

    def test_retrieve_unprocessed(self, bbagent_obj):
        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            result = bbagent_obj.bedset.get_unprocessed()
            assert result.count == 1
