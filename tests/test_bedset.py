from bbconf.bbagent import BedBaseAgent
from bbconf.exceptions import BedFIleExistsError, BEDFileNotFoundError
from bbconf.db_utils import Bed, Files

from sqlalchemy.orm import Session
from sqlalchemy.sql import select

from unittest.mock import Mock
import pytest

from .utils import ContextManagerDBTesting
from .conftest import get_bbagent

from .utils import BED_TEST_ID


class TestBedset:

    def test_calculate_stats(self, bbagent_obj):
        with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
            results = bbagent_obj.bedset._calculate_statistics([BED_TEST_ID])

            assert results is not None
            assert results.sd is not None
            assert results.mean is not None


    def test_crate_bedset_all(self):
        agent = BedBaseAgent(config=config)
        agent.bedset.create(
            "testinoo",
            "test_name",
            description="this is test description",
            bedid_list=[
                "bbad85f21962bb8d972444f7f9a3a932",
            ],
            statistics=True,
            upload_pephub=True,
            no_fail=True,
        )
        assert True

    def test_get_idset(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bedset.get("test")
        print(ff)
        assert ff != None

    def test_get_idset_list(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bedset.get_ids_list()
        print(ff)
        assert ff != None

#
#
# def test_get_stats(bbagent_obj):
#     with ContextManagerDBTesting(config=bbagent_obj.config, add_data=True):
#         return_result = bbagent_obj.bed.get_stats()
#
#         assert
#
#         assert return_result is not None
#         assert return_result.number_of_regions == 1
#     agent = BedBaseAgent(config=config)
#     ff = agent.get_stats("91b2754c8ff01769bacfc80e6923c46e")
#     print(ff)
#     assert ff != None
