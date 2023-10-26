from bbconf import BedBaseConf

import logging
import sys

log = logging.getLogger()
log.setLevel(logging.DEBUG)
stream = logging.StreamHandler(sys.stdout)
stream.setLevel(logging.DEBUG)
log.addHandler(stream)
logging.getLogger("bbconf").setLevel(logging.DEBUG)

bbc = BedBaseConf("bedbase.org/config/bedbase2.yaml")

bbc.bed.retrieve("78c0e4753d04b238fc07e4ebe5a02984")

bbc.bed.retrieve("78c0e4753d04b238fc07e4ebe5a02984", "bedfile")

bbc.bed.retrieve("78c0e4753d04b238fc07e4ebe5a02984", result_identifier="bedfile")
