import datetime

import pytest

from bbconf.const import DEFAULT_LICENSE
from bbconf.exceptions import BedBaseConfError
from bbconf.models.base_models import UsageModel

from .conftest import SERVICE_UNAVAILABLE
from .utils import BED_TEST_ID, BEDSET_TEST_ID, ContextManagerDBTesting


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


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
class TestAddUsage:
    def test_add_usages(self, bbagent_obj):
        usage = UsageModel(
            bed_meta={BED_TEST_ID: 3},
            bedset_meta={BEDSET_TEST_ID: 4},
            bed_search={"ff": 2},
            bedset_search={"ase": 1},
            files={"bin.bed.gz": 432},
            date_from=datetime.datetime.now(),
            date_to=datetime.datetime.now(),
        )

        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            bbagent_obj.add_usage(usage)
