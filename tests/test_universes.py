import pytest

from bbconf.exceptions import BEDFileNotFoundError

from .conftest import SERVICE_UNAVAILABLE
from .utils import BED_TEST_ID, ContextManagerDBTesting


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
class TestUniverses:

    def test_add(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            bbagent_obj.bed.add_universe(
                BED_TEST_ID, bedset_id=None, construct_method="hp31"
            )

            assert bbagent_obj.bed.exists(BED_TEST_ID)
            assert bbagent_obj.bed.exists_universe(BED_TEST_ID)
            universe_meta = bbagent_obj.bed.get(BED_TEST_ID)
            assert universe_meta is not None
            assert universe_meta.is_universe is True

    def test_delete_universe(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            bbagent_obj.bed.add_universe(
                BED_TEST_ID, bedset_id=None, construct_method="hp31"
            )

            assert bbagent_obj.bed.exists_universe(BED_TEST_ID)

            bbagent_obj.bed.delete_universe(BED_TEST_ID)

            assert not bbagent_obj.bed.exists_universe(BED_TEST_ID)

    def test_add_universe_error(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            with pytest.raises(BEDFileNotFoundError):
                bbagent_obj.bed.add_universe(
                    "not_f", bedset_id=None, construct_method="hp31"
                )

    def test_add_get_tokenized(self, bbagent_obj, mocker):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            bbagent_obj.bed.add_universe(
                BED_TEST_ID, bedset_id=None, construct_method="hp31"
            )

            assert bbagent_obj.bed.exists_universe(BED_TEST_ID)

            saved_path = "test/1/1"

            zarr_mock = mocker.patch(
                "bbconf.modules.bedfiles.BedAgentBedFile._add_zarr_s3",
                return_value=saved_path,
            )

            bbagent_obj.bed.add_tokenized(
                bed_id=BED_TEST_ID, universe_id=BED_TEST_ID, token_vector=[1, 2, 3]
            )

            zarr_path = bbagent_obj.bed._get_tokenized_path(
                BED_TEST_ID, universe_id=BED_TEST_ID
            )

            assert zarr_mock.called
            assert f"s3://bedbase/{saved_path}" == zarr_path

    def test_get_tokenized(self, bbagent_obj, mocked_phc):
        # how to test it?
        ...
