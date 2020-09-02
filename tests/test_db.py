""" Tests for BedBaseConf database features """

from bbconf import BedBaseConf
from .conftest import min_cfg_pth, cfg_pth
from bbconf import get_bedbase_cfg
from bbconf.const import PG_CLIENT_KEY

from psycopg2.extensions import connection


class TestDB:
    def test_db_basic(self, min_cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        assert isinstance(bbc, BedBaseConf)
        assert bbc.establish_postgres_connection()
        assert isinstance(bbc[PG_CLIENT_KEY], connection)
