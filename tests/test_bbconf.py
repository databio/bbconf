""" Tests for BedBaseConf database features """

import pytest
from pipestat import PipestatManager
from sqlalchemy.exc import IntegrityError

from bbconf import BedBaseConf, get_bedbase_cfg
from bbconf.exceptions import *
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.main import default_registry

DB_URL = "postgresql+psycopg2://postgres:pipestat-password@127.0.0.1:5432/pipestat-test"


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


class TestAll:
    def test_invalid_config(self, invalid_cfg_pth):
        with ContextManagerDBTesting(DB_URL):
            with pytest.raises(MissingConfigDataError):
                BedBaseConf(get_bedbase_cfg(cfg=invalid_cfg_pth))

    def test_tables_creation(self, cfg_pth):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            for table in ["bed", "bedset"]:
                assert isinstance(getattr(bbc, table), PipestatManager)

    def test_data_insert(self, cfg_pth, test_data_bed, test_data_bedset):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            # bedfiles table
            ori_cnt = bbc.bed.record_count
            bbc.bed.report(sample_name="bed1", values=test_data_bed)
            assert ori_cnt + 1 == bbc.bed.record_count
            # bedsets table
            ori_cnt = bbc.bedset.record_count
            bbc.bedset.report(sample_name="bedset1", values=test_data_bedset)
            assert ori_cnt + 1 == bbc.bedset.record_count

    def test_nonunique_digest_insert_error(
        self, cfg_pth, test_data_bed, test_data_bedset
    ):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(sample_name="bed1", values=test_data_bed)
            assert not bbc.bed.report(sample_name="bed1", values=test_data_bed)
            bbc.bedset.report(sample_name="bedset1", values=test_data_bedset)
            assert not bbc.bedset.report(sample_name="bedset1", values=test_data_bedset)

    def test_reporting_relationships(self, cfg_pth, test_data_bed, test_data_bedset):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(sample_name="bed1", values=test_data_bed)
            bed_id = bbc.bed.retrieve(sample_name="bed1", result_identifier="id")
            bbc.bedset.report(sample_name="bedset1", values=test_data_bedset)
            bedset_id = bbc.bedset.retrieve(
                sample_name="bedset1", result_identifier="id"
            )
            # TODO build relationship table
            bbc.report_relationship(bedfile_id=bed_id, bedset_id=bedset_id)

    def test_cant_remove_record_if_in_reltable(self, cfg_pth):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            with pytest.raises(IntegrityError):
                bbc.bed.remove(sample_name="bed1")
            with pytest.raises(IntegrityError):
                bbc.bedset.remove(sample_name="bedset1")

    def test_removal(self, cfg_pth, test_data_bed, test_data_bedset):
        with ContextManagerDBTesting(DB_URL):
            bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
            bbc.bed.report(sample_name="bed1", values=test_data_bed)
            bbc.bedset.report(sample_name="bedset1", values=test_data_bedset)
            bedset_id = bbc.bedset.retrieve(
                sample_name="bedset1", result_identifier="id"
            )
            bed_id = bbc.bed.retrieve(sample_name="bed1", result_identifier="id")
            bbc.remove_relationship(bedset_id=bedset_id, bedfile_ids=[bed_id])
            ori_cnt = bbc.bed.record_count
            bbc.bed.remove(sample_name="bed1")
            assert ori_cnt - 1 == bbc.bed.record_count
            ori_cnt = bbc.bedset.record_count
            bbc.bedset.remove(sample_name="bedset1")
            assert ori_cnt - 1 == bbc.bedset.record_count
