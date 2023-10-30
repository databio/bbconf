# This provides a short script to explore bbconf interactively
# First you have to set the env vars with something like:
# source ../bedbase.org/environment/production.env

from bbconf import BedBaseConf

import logging
import sys


def set_up_interactive_logger(package=None) -> logging.Logger:
    """Set up a logger for interactive testing"""
    _LOGGER = logging.getLogger(package)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    stream.setLevel(logging.DEBUG)
    _LOGGER.addHandler(stream)
    _LOGGER.setLevel(logging.DEBUG)
    return _LOGGER


_LOGGER = set_up_interactive_logger()
_LOGGER.debug("Test log message")


bbc = BedBaseConf("../bedbase.org/config/bedbase.yaml")

bbc.bed.retrieve("78c0e4753d04b238fc07e4ebe5a02984")

bbc.bed.retrieve("78c0e4753d04b238fc07e4ebe5a02984", "bedfile")

bbc.bed.retrieve("78c0e4753d04b238fc07e4ebe5a02984", result_identifier="bedfile")
