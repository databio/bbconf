""" Test suite shared objects and setup """
import pytest
import os


@pytest.fixture
def test_data_bed():
    s = "test_string"
    return {"name": s, "md5sum": s, "bedfile": {"path": s, "title": s}, "regions_no": 1}


@pytest.fixture
def test_data_bedset():
    s = "test_string"
    return {"name": s, "md5sum": s, "bedset_tar_archive_path": {"path": s, "title": s}}


@pytest.fixture
def data_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


@pytest.fixture
def cfg_pth(data_path):
    return os.path.join(data_path, "config.yaml")


@pytest.fixture
def invalid_cfg_pth(data_path):
    return os.path.join(data_path, "config_invalid.yaml")
