import os
import pytest
from bbconf.bbagent import BedBaseAgent
from bbconf.db_utils import Bed

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

CONFIG_PATH = os.path.join(
    TESTS_DIR,
    "config_test.yaml",
)
DATA_PATH = os.path.join(
    TESTS_DIR,
    "data",
)


def get_bbagent():
    return BedBaseAgent(config=CONFIG_PATH)


@pytest.fixture(scope="function")
def bbagent_obj():
    yield BedBaseAgent(config=CONFIG_PATH)


@pytest.fixture()
def example_dict():
    plots = {
        "chrombins": {
            "name": "Regions distribution over chromosomes",
            "path": "plots/bbad85f21962bb8d972444f7f9a3a932_chrombins.pdf",
            "path_thumbnail": "plots/bbad85f21962bb8d972444f7f9a3a932_chrombins.png",
        }
    }
    files = {
        "bedfile": {
            "name": "Bed file",
            "path": os.path.join(
                DATA_PATH, "files/bbad85f21962bb8d972444f7f9a3a932.bed.gz"
            ),
            "description": "Bed file with regions",
        }
    }
    classification = {
        "bed_format": "narrowpeak",
        "bed_type": "bed6+4",
        "genome_alias": "hg38",
        "genome_digest": "2230c535660fb4774114bfa966a62f823fdb6d21acf138d4",
        "name": "bbad85f21962bb8d972444f7f9a3a932",
    }

    return dict(
        identifier="bbad85f21962bb8d972444f7f9a3a932",
        stats={
            "number_of_regions": 1,
            "median_tss_dist": 2,
            "mean_region_width": 3,
            "exon_frequency": 4,
            "exon_percentage": 5,
            "intron_frequency": 6,
            "intron_percentage": 7,
            "intergenic_percentage": 8,
            "intergenic_frequency": 9,
            "promotercore_frequency": 10,
            "promotercore_percentage": 11,
            "fiveutr_frequency": 12,
            "fiveutr_percentage": 13,
            "threeutr_frequency": 14,
            "threeutr_percentage": 15,
            "promoterprox_frequency": 16,
            "promoterprox_percentage": 17,
        },
        metadata={"sample_name": "sample_name_1"},
        plots=plots,
        files=files,
        classification=classification,
        upload_qdrant=False,
        upload_pephub=False,
        upload_s3=True,
        local_path=DATA_PATH,
        overwrite=False,
        nofail=False,
    )


@pytest.fixture
def load_test_data():
    db_engine = get_bbagent().config.db_engine()
