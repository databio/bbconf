""" Tests for BedBaseConf constructor """

from bbconf import BedBaseConf
from .conftest import min_cfg_pth, cfg_pth
from bbconf import get_bedbase_cfg


class TestInitialize:
    def test_init_basic(self, min_cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        assert isinstance(bbc, BedBaseConf)
