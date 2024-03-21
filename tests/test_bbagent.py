import warnings
from bbconf.bbagent import BedBaseAgent

from sqlalchemy.exc import OperationalError

# from .conftest import DNS

DNS = "postgresql+psycopg://postgres:docker@localhost:5432/bedbase"

config = "/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml"


def db_setup():
    # Check if the database is setup
    try:
        BedBaseAgent(config=config)
    except OperationalError:
        warnings.warn(
            UserWarning(
                f"Skipping tests, because DB is not setup. {DNS}. To setup DB go to README.md"
            )
        )
        return False
    return True


def test_bb_database():
    assert db_setup()


class Test_BedFile_Agent:

    def test_upload(self):
        agent = BedBaseAgent(config=config)
        agent.bed.add()

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
        # agent = BedBaseAgent(config=config)
        # ff = agent.bed.delete("91b2754c8ff01769bacfc80e6923c46e")
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
            "bed", "91b2754c8ff01769bacfc80e6923c46e", "bed_file", "http"
        )
        ff

    def test_object_path_thumbnail_error(self):
        agent = BedBaseAgent(config=config)
        # TODO: it should raise issue:
        ff = agent.objects.get_thumbnail_uri(
            "bed", "91b2754c8ff01769bacfc80e6923c46e", "bed_file", "http"
        )
        ff

    def test_object_path_thumbnail(self):
        agent = BedBaseAgent(config=config)
        # TODO: it should raise issue:
        ff = agent.objects.get_thumbnail_uri(
            "bed", "91b2754c8ff01769bacfc80e6923c46e", "widths_histogram", "http"
        )
        ff

    def test_object_metadata(self):
        agent = BedBaseAgent(config=config)
        ff = agent.objects.get_drs_metadata(
            "bed", "91b2754c8ff01769bacfc80e6923c46e", "widths_histogram", "localhost"
        )
        ff


class TestBedset:

    def test_clalculate_stats(self):
        agent = BedBaseAgent(config=config)
        ff = agent.bedset._calculate_statistics(["91b2754c8ff01769bacfc80e6923c46e"])
        ff
        assert ff != None

    def test_upload_all(self):
        agent = BedBaseAgent(config=config)
        agent.bedset.create(
            "test",
            "test_name",
            description="test",
            bedid_list=["91b2754c8ff01769bacfc80e6923c46e"],
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
