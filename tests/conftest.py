""" Test suite shared objects and setup """
import pytest
import os


@pytest.fixture
def data_path():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


@pytest.fixture
def min_cfg_pth(data_path):
    return os.path.join(data_path, "config_min.yaml")


@pytest.fixture
def cfg_pth(data_path):
    return os.path.join(data_path, "config.yaml")


@pytest.fixture
def invalid_cfg_pth(data_path):
    return os.path.join(data_path, "config_invalid.yaml")
