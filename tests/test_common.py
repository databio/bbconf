import pytest

from bbconf.const import DEFAULT_LICENSE
from bbconf.models.base_models import UsageModel
from bbconf.exceptions import BedBaseConfError

from .conftest import SERVICE_UNAVAILABLE
from .utils import ContextManagerDBTesting, BED_TEST_ID, BEDSET_TEST_ID


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
def test_get_stats(bbagent_obj):
    with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True, bedset=True):
        return_result = bbagent_obj.get_stats()

        assert return_result
        assert return_result.bedfiles_number == 1
        assert return_result.bedsets_number == 1
        assert return_result.genomes_number == 1


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
def test_get_licenses(bbagent_obj):
    return_result = bbagent_obj.list_of_licenses

    assert return_result
    assert DEFAULT_LICENSE in return_result


#
# @pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
# class TestAddUsage:
#     def test_add_bed_search(self, bbagent_obj):
#
#         usage = UsageModel(
#             event="bed_search",
#             query="test",
#             ipaddress="123.09.09.123",
#             user_agent="test-agent",
#         )
#
#         with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
#             bbagent_obj.add_usage(usage)
#
#     def test_add_bedset_search(self, bbagent_obj):
#
#         usage = UsageModel(
#             event="bedset_search",
#             query="test",
#             ipaddress="12345",
#             user_agent="test-agent",
#         )
#
#         with ContextManagerDBTesting(
#             config=bbagent_obj.config, add_data=True, bedset=True
#         ):
#             bbagent_obj.add_usage(usage)
#
#     def test_add_bedset_meta(self, bbagent_obj):
#         usage = UsageModel(
#             event="bedset_metadata",
#             query=None,
#             bedset_id=BEDSET_TEST_ID,
#             ipaddress="1234",
#             user_agent="test-agent",
#         )
#
#         with ContextManagerDBTesting(
#             config=bbagent_obj.config, add_data=True, bedset=True
#         ):
#             bbagent_obj.add_usage(usage)
#
#     def test_add_bed_meta(self, bbagent_obj):
#         usage = UsageModel(
#             event="bed_metadata",
#             query=None,
#             bed_id=BED_TEST_ID,
#             ipaddress="1234",
#             user_agent="test-agent",
#         )
#
#         with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
#             bbagent_obj.add_usage(usage)
#
#     def test_add_bedset_meta_error(self, bbagent_obj):
#         usage = UsageModel(
#             event="bedset_metadata",
#             query=None,
#             bedset_id="error",
#             ipaddress="1234",
#             user_agent="test-agent",
#         )
#
#         with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
#
#             with pytest.raises(BedBaseConfError):
#                 bbagent_obj.add_usage(usage)
#
#     def test_add_incorrect_event(self, bbagent_obj):
#         usage = UsageModel(
#             event="bed_metadata",
#             query=None,
#             bed_id="error",
#             ipaddress="1234",
#             user_agent="test-agent",
#         )
#
#         with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
#             with pytest.raises(BedBaseConfError):
#                 bbagent_obj.add_usage(usage)
#
#     def test_add_bed_file(self, bbagent_obj):
#         usage = UsageModel(
#             event="bed_files",
#             query=None,
#             bed_id=BED_TEST_ID,
#             file_name="test_file",
#             ipaddress="1234",
#             user_agent="test-agent",
#         )
#
#         with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
#             bbagent_obj.add_usage(usage)
