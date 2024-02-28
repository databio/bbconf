""" Tests for BedBaseConf database features """

import pytest
from pipestat import PipestatManager
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel, create_engine
from sqlmodel.main import default_registry
import os
import warnings
import sqlalchemy

from bbconf import BedBaseConf, get_bedbase_cfg
from bbconf.exceptions import MissingConfigDataError


DB_URL = "postgresql+psycopg://postgres:dockerpassword@127.0.0.1:5432/pipestat-test"
DATA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
pytest_db_skip_reason = "Database is not set up... To run this test, set up the database. Go to test/README.md for more information."


def db_setup():
    # Check if the database is setup
    try:
        BedBaseConf(os.path.join(DATA_PATH, "config.yaml"))
    except sqlalchemy.exc.OperationalError:
        warnings.warn(UserWarning(f"{pytest_db_skip_reason}"))
        return False
    return True


class ContextManagerDBTesting:
    """
    Creates context manager to connect to database at db_url and drop everything from the database upon exit to ensure
    the db is empty for each new test.
    """

    def __init__(self, db_url):
        self.db_url = db_url

    def __enter__(self):
        self.engine = create_engine(self.db_url, echo=True)
        self.connection = self.engine.connect()
        return self.connection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        SQLModel.metadata.drop_all(self.engine)
        default_registry.dispose()
        self.connection.close()


@pytest.mark.skipif(
    not db_setup(),
    reason=pytest_db_skip_reason,
)
class TestAll:
    def test_invalid_config(self, invalid_cfg_pth):
        with ContextManagerDBTesting(DB_URL):
            with pytest.raises(MissingConfigDataError):
                BedBaseConf(get_bedbase_cfg(cfg=invalid_cfg_pth))

    def test_tables_creation(self, cfg_pth):
        # with ContextManagerDBTesting(DB_URL):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
        for table in ["bed", "bedset"]:
            assert isinstance(getattr(bbc, table), PipestatManager)

    def test_data_insert(self, cfg_pth, test_data_bed, test_data_bedset):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            # bedfiles table
            ori_cnt = bbc.bed.record_count
            bbc.bed.report(record_identifier="bed1", values=test_data_bed)
            assert ori_cnt + 1 == bbc.bed.record_count
            # bedsets table
            ori_cnt = bbc.bedset.record_count
            bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
            assert ori_cnt + 1 == bbc.bedset.record_count

    def test_nonunique_digest_insert_error(
        self, cfg_pth, test_data_bed, test_data_bedset
    ):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(record_identifier="bed1", values=test_data_bed)
            assert not bbc.bed.report(record_identifier="bed1", values=test_data_bed)
            bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
            assert not bbc.bedset.report(
                record_identifier="bedset1", values=test_data_bedset
            )

    def test_reporting_relationships(self, cfg_pth, test_data_bed, test_data_bedset):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(record_identifier="bed1", values=test_data_bed)
            bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
            bbc.report_relationship(
                bedfile_record_id="bed1", bedset_record_id="bedset1"
            )

    # def test_cant_remove_record_if_in_reltable(
    #     self, cfg_pth, test_data_bed, test_data_bedset
    # ):
    #     with ContextManagerDBTesting(DB_URL):
    #         bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
    #         bbc.bed.report(record_identifier="bed1", values=test_data_bed)
    #         bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
    #         bbc.report_relationship(
    #             bedfile_record_id="bed1", bedset_record_id="bedset1"
    #         )
    #         with pytest.raises(IntegrityError):
    #             bbc.bed.remove(record_identifier="bed1")
    #         with pytest.raises(IntegrityError):
    #             bbc.bedset.remove(record_identifier="bedset1")

    def test_select(self, cfg_pth, test_data_bed, test_data_bedset):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(record_identifier="bed1", values=test_data_bed)
            bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
            bbc.report_relationship(
                bedfile_record_id="bed1", bedset_record_id="bedset1"
            )

            unique_bedfiles = bbc.select_unique(table_name="bedfile__sample")
            assert unique_bedfiles[0]["record_identifier"] == "bed1"
            unique_bedsets = bbc.select_unique(table_name="bedsets__sample")
            assert unique_bedsets[0]["record_identifier"] == "bedset1"

    def test_removal(self, cfg_pth, test_data_bed, test_data_bedset):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(record_identifier="bed1", values=test_data_bed)
            bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
            bbc.report_relationship(
                bedfile_record_id="bed1",
                bedset_record_id="bedset1",
            )
            bbc.remove_relationship(
                bedset_record_id="bedset1", bedfile_record_id=["bed1"]
            )
            ori_cnt = bbc.bed.record_count
            bbc.bed.remove(record_identifier="bed1")
            assert ori_cnt - 1 == bbc.bed.record_count
            ori_cnt = bbc.bedset.record_count
            bbc.bedset.remove(record_identifier="bedset1")
            assert ori_cnt - 1 == bbc.bedset.record_count

    def test_config_variables_are_set(self, cfg_pth, test_data_bed, test_data_bedset):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))

            print(bbc.config["qdrant"]["host"])
            assert bbc.config["qdrant"]["host"] == "localhost"
            assert bbc.config["path"]["region2vec"] is not None
            assert bbc.config["database"]["host"] in ["localhost", "127.0.0.1"]

    def test_select_bedfiles_from_bedset(
        self, cfg_pth, test_data_bed, test_data_bedset
    ):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(record_identifier="bed1", values=test_data_bed)
            bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
            bbc.report_relationship(
                bedfile_record_id="bed1", bedset_record_id="bedset1"
            )
            result = bbc.select_bedfiles_from_bedset(bedset_record_id="bedset1")
        assert result[0]["record_identifier"] == "bed1"

    def test_select_bedfiles_from_bedset_with_metadata(
        self, cfg_pth, test_data_bed, test_data_bedset
    ):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(record_identifier="bed1", values=test_data_bed)
            bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
            bbc.report_relationship(
                bedfile_record_id="bed1", bedset_record_id="bedset1"
            )
            result = bbc.select_bedfiles_from_bedset(
                bedset_record_id="bedset1", metadata=True
            )
        assert result[0]["name"] == "test_string"

    @pytest.mark.skipif(True, reason="not implemented")
    def test_get_bed_drs_metadata(self):
        # TODO: add test
        assert True
