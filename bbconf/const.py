"""
Constant variables shared among packages that constitute bedbase project
"""

BED_INDEX = "bedfiles"
BEDSET_INDEX = "bedsets"

SEARCH_TERMS = ['cellType', 'cellTypeSubtype', 'antibody', 'mappingGenome',
                'description', 'tissue', 'species', 'protocol', 'genome']
RAW_BEDFILE_KEY = "raw_bedfile"

__all__ = ["BED_INDEX", "SEARCH_TERMS", "RAW_BEDFILE_KEY"]
