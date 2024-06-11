from bbconf.const import DEFAULT_LICENSE

import pytest
from .conftest import SERVICE_UNAVAILABLE
from .utils import ContextManagerDBTesting


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
def test_get_stats(bbagent_obj):
    with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True, bedset=True):
        return_result = bbagent_obj.get_stats

        assert return_result
        assert return_result.bedfiles_number == 1
        assert return_result.bedsets_number == 1
        assert return_result.genomes_number == 1


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
def test_get_licenses(bbagent_obj):
    return_result = bbagent_obj.list_of_licenses

    assert return_result
    assert DEFAULT_LICENSE in return_result
