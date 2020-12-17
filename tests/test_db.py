""" Tests for BedBaseConf database features """

import pytest
from psycopg2.errors import ForeignKeyViolation
from bbconf import BedBaseConf
from bbconf.exceptions import *
from .conftest import cfg_pth, cfg_pth
from bbconf import get_bedbase_cfg
from bbconf.const import *
from pipestat.exceptions import PipestatDatabaseError
from pipestat import PipestatManager

from psycopg2.extensions import connection


class TestDBTables:
    def test_invalid_config(self, invalid_cfg_pth):
        with pytest.raises(MissingConfigDataError):
            BedBaseConf(get_bedbase_cfg(cfg=invalid_cfg_pth))

    def test_tables_creation(self, cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
        for table in ["bed", "bedset"]:
            assert isinstance(getattr(bbc, table), PipestatManager)

    def test_data_insert(self, cfg_pth, test_data_bed, test_data_bedset):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
        # bedfiles table
        ori_cnt = bbc.bed.record_count
        bbc.bed.report(record_identifier="bed1", values=test_data_bed)
        assert ori_cnt + 1 == bbc.bed.record_count
        # bedsets table
        ori_cnt = bbc.bedset.record_count
        bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)
        assert ori_cnt + 1 == bbc.bedset.record_count

    def test_nonunique_digest_insert_error(self, cfg_pth, test_data_bed, test_data_bedset):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
        assert not bbc.bed.report(record_identifier="bed1", values=test_data_bed)
        assert not bbc.bedset.report(record_identifier="bedset1", values=test_data_bedset)

    def test_reporting_relationships(self, cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
        bed_id = bbc.bed.retrieve(
            record_identifier="bed1", result_identifier="id")
        bedset_id = bbc.bedset.retrieve(
            record_identifier="bedset1", result_identifier="id")
        bbc.report_bedfile_for_bedset(bedfile_id=bed_id, bedset_id=bedset_id)

    def test_cant_remove_record_if_in_reltable(self, cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
        with pytest.raises(ForeignKeyViolation):
            bbc.bed.remove(record_identifier="bed1")
        with pytest.raises(ForeignKeyViolation):
            bbc.bedset.remove(record_identifier="bedset1")

    # def test_removal(self, cfg_pth):
    #     bbc = BedBaseConf(get_bedbase_cfg(cfg=cfg_pth))
    #     ori_cnt = bbc.bed.record_count
    #     bbc.bed.remove(record_identifier="bed1")
    #     assert ori_cnt - 1 == bbc.bed.record_count
    #     ori_cnt = bbc.bedset.record_count
    #     bbc.bedset.remove(record_identifier="bedset1")
    #     assert ori_cnt - 1 == bbc.bedset.record_count
