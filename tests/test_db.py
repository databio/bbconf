""" Tests for BedBaseConf database features """

import pytest
from psycopg2 import Error as psycopg2Error
from psycopg2.errors import UniqueViolation
from bbconf import BedBaseConf
from .conftest import min_cfg_pth, cfg_pth
from bbconf import get_bedbase_cfg
from bbconf.const import *
from bbconf.exceptions import BedBaseConnectionError

from psycopg2.extensions import connection


class TestDBConnection:
    def test_connection_checker(self, min_cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        assert not bbc.check_connection()
        bbc.establish_postgres_connection()
        assert bbc.check_connection()

    def test_db_basic(self, min_cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        assert bbc.establish_postgres_connection()
        assert isinstance(bbc[PG_CLIENT_KEY], connection)

    def test_connection_overwrite_error(self, min_cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        bbc.establish_postgres_connection()
        with pytest.raises(BedBaseConnectionError):
            bbc.establish_postgres_connection()

    @pytest.mark.parametrize("suppress", [True, False])
    def test_connection_error(self, min_cfg_pth, suppress):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        bbc[CFG_DATABASE_KEY][CFG_HOST_KEY] = "bogus_host"
        if suppress:
            assert not bbc.establish_postgres_connection(suppress=suppress)
        else:
            with pytest.raises(psycopg2Error):
                bbc.establish_postgres_connection(suppress=suppress)

    def test_connection_closing(self, min_cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        bbc.establish_postgres_connection()
        bbc.close_postgres_connection()
        assert not bbc.check_connection()

    def test_connection_closing_closed(self, min_cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        with pytest.raises(BedBaseConnectionError):
            bbc.close_postgres_connection()


class TestDBTables:
    def test_tables_creation_and_dropping(self, min_cfg_pth, test_columns):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        # remove any existing tables
        bbc.drop_bedset_bedfiles_table()
        bbc.drop_bedfiles_table()
        bbc.drop_bedsets_table()
        # create and test
        bbc.create_bedfiles_table(columns=test_columns)
        assert bbc.check_bedfiles_table_exists()
        bbc.create_bedsets_table(columns=test_columns)
        assert bbc.check_bedsets_table_exists()
        bbc.create_bedset_bedfiles_table()
        assert bbc.check_bedset_bedfiles_table_exists()

    def test_data_insert(self, min_cfg_pth, test_data):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        # bedfiles table
        ori_cnt = bbc.count_bedfiles()
        bbc.insert_bedfile_data(values=test_data)
        assert ori_cnt + 1 == bbc.count_bedfiles()
        # bedsets table
        ori_cnt = bbc.count_bedsets()
        bbc.insert_bedset_data(values=test_data)
        assert ori_cnt + 1 == bbc.count_bedsets()

    def test_nonunique_digest_insert_error(self, min_cfg_pth, test_data_unique):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        bbc.insert_bedfile_data(values=test_data_unique)
        with pytest.raises(UniqueViolation):
            bbc.insert_bedfile_data(values=test_data_unique)

    @pytest.mark.parametrize(["columns", "condition", "match"], [
        ("id", "test='test_string'", True),
        ("test", "test='test_string'", True),
        ("id", "test_json->'test_key1'->>'test_key2'='test_val'", True),
        ("id", "test='test_string_xxx'", False),
        ("id", "test_json->'test_key1_xxx'->>'test_key2'='test_val'", False),
        ("id", "test_json->'test_key1'->>'test_key2'='test_val_xxx'", False)

    ])
    def test_data_select(self, min_cfg_pth, columns, condition, match):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        hits = bbc.select(
            table_name=BED_TABLE,
            condition=condition,
            columns=columns
        )
        if match:
            assert len(hits) > 0
        else:
            assert len(hits) == 0

