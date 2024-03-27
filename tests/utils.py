from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.db_utils import Bed, Files
from typing import Union

from sqlalchemy.orm import Session


def get_example_dict() -> dict:
    value = {
        "id": "test_id",
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
        "bed_format": "narrowpeak",
        "bed_type": "bed6+4",
        "genome_alias": "hg38",
        "genome_digest": "2230c535660fb4774114bfa966a62f823fdb6d21acf138d4",
        "name": "random_name",
    }
    return value


def get_files() -> dict:
    return {
        "title": "Bed file",
        "name": "bed_file",
        "path": "data/files/bbad85f21962bb8d972444f7f9a3a932.bed.gz",
        "description": "Bed file with regions",
    }


def get_plots() -> dict:
    return {
        "name": "chrombins",
        "description": "Regions distribution over chromosomes",
        "title": "Regions distribution over chromosomes",
        "path": "data/plots/bbad85f21962bb8d972444f7f9a3a932_chrombins.pdf",
        "path_thumbnail": "data/plots/bbad85f21962bb8d972444f7f9a3a932_chrombins.png",
    }


class ContextManagerDBTesting:
    """
    Creates context manager to connect to database at db_url adds data and drop everything from the database upon exit to ensure
    the db is empty for each new test.
    """

    def __init__(self, config: Union[str, BedBaseConfig], add_data: bool = False):
        """
        :param config_path: path to the config file
        :param add_data: add data to the database
        """
        if isinstance(config, BedBaseConfig):
            self.config = config
        else:
            self.config = BedBaseConfig(config)

        self.add_data = add_data

    def __enter__(self):

        self.db_engine = self.config.db_engine

        if self.add_data:
            self._add_data()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.db_engine.delete_schema()

    def _add_data(self):
        with Session(self.db_engine.engine) as session:

            new_bed = Bed(**get_example_dict())
            new_files = Files(**get_files())
            new_plots = Files(**get_plots())

            session.add(new_bed)
            session.add(new_files)
            session.add(new_plots)
            session.commit()
