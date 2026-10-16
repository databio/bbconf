import datetime

import pytest

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.config_parser.models import ConfigDB, ConfigFile, ConfigPath, ConfigServer
from bbconf.const import DEFAULT_LICENSE
from bbconf.exceptions import BadAccessMethodError
from bbconf.models.base_models import UsageModel

from .conftest import SERVICE_UNAVAILABLE
from .utils import BED_TEST_ID, BEDSET_TEST_ID, ContextManagerDBTesting


def _bbconfig_without_access_methods() -> BedBaseConfig:
    """Build a BedBaseConfig with no DB/qdrant/ML setup, just a config file."""
    bbconfig = object.__new__(BedBaseConfig)
    bbconfig._config = ConfigFile(
        database=ConfigDB(host="localhost", user="postgres", password="docker"),
        server=ConfigServer(),
        path=ConfigPath(),
        access_methods=None,
    )
    return bbconfig


class TestAccessMethodsMissing:
    """access_methods is optional in the config; these should not raise AttributeError."""

    def test_construct_access_method_list_returns_empty(self):
        bbconfig = _bbconfig_without_access_methods()
        assert bbconfig.config.access_methods is None
        assert bbconfig.construct_access_method_list("some/rel/path") == []

    def test_get_prefixed_uri_raises_bad_access_method_error(self):
        bbconfig = _bbconfig_without_access_methods()
        with pytest.raises(BadAccessMethodError):
            bbconfig.get_prefixed_uri("some/rel/path", "http")


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
def test_get_stats(bbagent_obj):
    with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True, bedset=True):
        return_result = bbagent_obj.get_stats()

        assert return_result
        assert return_result.bedfiles_number == 1
        assert return_result.bedsets_number == 1
        assert return_result.genomes_number == 1


@pytest.mark.skipif(SERVICE_UNAVAILABLE, reason="Database is not available")
def test_get_detailed_stats(bbagent_obj):
    with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True, bedset=True):
        return_result = bbagent_obj.get_detailed_stats()

        assert return_result
        assert return_result.number_of_regions.mean == 1
        assert return_result.mean_region_width.mean == 3


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

    def test_get_usage(self, bbagent_obj):
        now = datetime.datetime.now()
        usage = UsageModel(
            bed_meta={BED_TEST_ID: 3},
            bedset_meta={},
            bed_search={"ff": 2, "": 9},
            bedset_search={"ase": 1},
            files={"files/a/abc.bed.gz": 5},
            date_from=now,
            date_to=now,
        )

        with ContextManagerDBTesting(
            config=bbagent_obj.config, add_data=True, bedset=True
        ):
            bbagent_obj.add_usage(usage)

            bed = bbagent_obj.get_usage("bed_meta")
            assert [(r.key, r.count) for r in bed.results] == [(BED_TEST_ID, 3)]
            # empty search keys are dropped; search type is filtered
            search = bbagent_obj.get_usage("bed_search")
            assert [r.key for r in search.results] == ["ff"]
            # file paths are shortened to the bed id
            assert bbagent_obj.get_usage("files").results[0].key == "abc"
            # date range excludes rows outside the window
            later = now + datetime.timedelta(days=1)
            assert bbagent_obj.get_usage("bed_meta", date_from=later).count == 0
            assert bbagent_obj.get_usage("bed_meta", offset=1).count == 0

            with pytest.raises(ValueError):
                bbagent_obj.get_usage("nope")
            with pytest.raises(ValueError):
                bbagent_obj.get_usage("bed_meta", limit=0)
