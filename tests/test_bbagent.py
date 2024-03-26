import warnings
from bbconf.bbagent import BedBaseAgent
import os

from sqlalchemy.exc import OperationalError
from .conftest import get_bbagent

from unittest.mock import Mock


def test_bb_database():
    agent = get_bbagent()
    assert isinstance(agent, BedBaseAgent)


class Test_BedFile_Agent:

    def test_upload(self, bbagent_obj, example_dict, mocker):
        upload_s3_mock = mocker.patch(
            "bbconf.modules.bedfiles.BedAgentBedFile._upload_s3",
            return_value=True,
        )
        bbagent_obj.bed.get_ids_list()
        bbagent_obj.bed.add(**example_dict)

    def test_get_all(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.get("91b2754c8ff01769bacfc80e6923c46e", full=True)
        print(ff)
        assert ff != None

    def test_get_raw_metadata(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.get_raw_metadata("91b2754c8ff01769bacfc80e6923c46e")
        print(ff)
        assert ff != None

    def test_get_stats(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.get_stats("91b2754c8ff01769bacfc80e6923c46e")
        print(ff)
        assert ff != None

    def test_get_files(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.get_files("91b2754c8ff01769bacfc80e6923c46e")
        print(ff)
        assert ff != None

    def test_get_plots(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.get_plots("91b2754c8ff01769bacfc80e6923c46e")
        print(ff)
        assert ff != None

    def test_get_objects(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.get_objects("91b2754c8ff01769bacfc80e6923c46e")
        print(ff)

    def test_get_list(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.get_ids_list()
        print(ff)
        assert ff != None

    def test_qdrant_search(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.text_to_bed_search("asf")
        print(ff)
        assert ff != None

    def test_qdrant_reindex(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.reindex_qdrant()
        ff
        assert True

    def test_delete_qdrant_point(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bed.delete_qdrant_point("91b2754c8ff01769bacfc80e6923c46e")
        ff
        assert True

    def test_bed_delete(self):
        agent = BedBaseAgent(
            config="/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml"
        )
        ff = agent.bed.delete("ec8179a55ab7e649e15762d0e8cb5378")
        # print(ff)
        # assert ff != None
        pass

    def test_bed_update(self):
        # agent = BedBaseAgent(config=config)
        # ff = agent.bed.update("91b2754c8ff01769bacfc80e6923c46e", {"number_of_regions": 44})
        # print(ff)
        # assert ff != None
        pass


def test_get_stats():
    agent = BedBaseAgent(config=config)
    ff = agent.get_stats("91b2754c8ff01769bacfc80e6923c46e")
    print(ff)
    assert ff != None


class TestObjects:
    def test_object_path(self):
        agent = BedBaseAgent(config=config)
        ff = agent.objects.get_object_uri(
            "bed", "bbad85f21962bb8d972444f7f9a3a932", "bed_file", "http"
        )
        ff

    def test_object_path_thumbnail_error(self):
        agent = BedBaseAgent(config=config)
        # TODO: it should raise issue:
        ff = agent.objects.get_thumbnail_uri(
            "bed", "bbad85f21962bb8d972444f7f9a3a932", "bed_file", "http"
        )
        ff

    def test_object_path_thumbnail(self):
        agent = BedBaseAgent(config=config)
        ff = agent.objects.get_thumbnail_uri(
            "bed", "bbad85f21962bb8d972444f7f9a3a932", "widths_histogram", "http"
        )
        ff

    def test_object_metadata(self):
        agent = BedBaseAgent(config=config)
        ff = agent.objects.get_drs_metadata(
            "bed", "bbad85f21962bb8d972444f7f9a3a932", "widths_histogram", "localhost"
        )
        ff


class TestBedset:

    def test_clalculate_stats(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bedset._calculate_statistics(["91b2754c8ff01769bacfc80e6923c46e"])
        ff
        assert ff != None

    def test_crate_bedset_all(self):
        agent = BedBaseAgent(config=config)
        agent.bedset.create(
            "testinoo",
            "test_name",
            description="this is test description",
            bedid_list=[
                "bbad85f21962bb8d972444f7f9a3a932",
                "0dcdf8986a72a3d85805bbc9493a1302",
                "db69691a3fee81a96c5dad57ca124fd8",
            ],
            statistics=True,
            # plots={"test": "test"},
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
