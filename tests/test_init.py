""" Tests for BedBaseConf constructor """

import pytest
from bbconf import BedBaseConf
from .conftest import min_cfg_pth, cfg_pth
from bbconf import get_bedbase_cfg
from bbconf.const import CFG_ENV_VARS
from bbconf.exceptions import MissingConfigDataError


class TestInitialize:
    def test_init_basic(self, min_cfg_pth):
        bbc = BedBaseConf(get_bedbase_cfg(cfg=min_cfg_pth))
        assert isinstance(bbc, BedBaseConf)

    def test_env_var(self, min_cfg_pth, monkeypatch):
        monkeypatch.setenv(CFG_ENV_VARS[0], min_cfg_pth)
        bbc = BedBaseConf(get_bedbase_cfg())
        assert isinstance(bbc, BedBaseConf)

    def test_missing_config_data(self, invalid_cfg_pth):
        with pytest.raises(MissingConfigDataError):
            BedBaseConf(get_bedbase_cfg(cfg=invalid_cfg_pth))