from typing import Union

from sqlalchemy.orm import Session

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.db_utils import Bed, BedFileBedSetRelation, BedSets, BedStats, Files

BED_TEST_ID = "bbad85f21962bb8d972444f7f9a3a932"
BEDSET_TEST_ID = "test_bedset_id"


stats = {
    "id": BED_TEST_ID,
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
}


def get_example_dict() -> dict:
    value = {
        "id": BED_TEST_ID,
        "bed_format": "narrowpeak",
        "bed_type": "bed6+4",
        "genome_alias": "hg38",
        "genome_digest": "2230c535660fb4774114bfa966a62f823fdb6d21acf138d4",
        "name": "random_name",
    }
    return value


def get_bedset_files() -> dict:
    return {
        "title": "region_commonality",
        "name": "region_commonality",
        "path": "data/files/bbad85f21962bb8d972444f7f9a3a932.bed.gz",
        "description": "Bfffffff",
        "bedset_id": BEDSET_TEST_ID,
    }


def get_files() -> dict:
    return {
        "title": "Bed file",
        "name": "bed_file",
        "path": "data/files/bbad85f21962bb8d972444f7f9a3a932.bed.gz",
        "description": "Bed file with regions",
        "bedfile_id": BED_TEST_ID,
    }


def get_plots() -> dict:
    return {
        "name": "chrombins",
        "description": "Regions distribution over chromosomes",
        "title": "Regions distribution over chromosomes",
        "path": "data/plots/bbad85f21962bb8d972444f7f9a3a932_chrombins.pdf",
        "path_thumbnail": "data/plots/bbad85f21962bb8d972444f7f9a3a932_chrombins.png",
        "bedfile_id": BED_TEST_ID,
    }


class ContextManagerDBTesting:
    """
    Creates context manager to connect to database at db_url adds data and drop everything from the database upon exit to ensure
    the db is empty for each new test.
    """

    def __init__(
        self,
        config: Union[str, BedBaseConfig],
        add_data: bool = False,
        bedset: bool = False,
    ):
        """
        :param config: config object
        :param add_data: add data to the database
        :param bedset: add bedset data to the database
        """
        if isinstance(config, BedBaseConfig):
            self.config = config
        else:
            self.config = BedBaseConfig(config)

        self.add_data = add_data
        self.bedset = bedset

        self.db_engine = self.config.db_engine
        self.db_engine.create_schema()

    def __enter__(self):
        if self.add_data:
            self._add_data()
            if self.bedset:
                self._add_bedset_data()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.db_engine.delete_schema()
        pass

    def _add_data(self):
        with Session(self.db_engine.engine) as session:
            new_bed = Bed(**get_example_dict())
            new_files = Files(**get_files())
            new_plots = Files(**get_plots())
            new_stats = BedStats(**stats)

            session.add(new_bed)
            session.add(new_files)
            session.add(new_plots)
            session.add(new_stats)

            session.commit()

    def _add_bedset_data(self):
        with Session(self.db_engine.engine) as session:
            new_bedset = BedSets(
                id=BEDSET_TEST_ID,
                name=BEDSET_TEST_ID,
                description="random desc",
                bedset_means=stats,
                bedset_standard_deviation=stats,
                md5sum="bbad0000000000000000000000000000",
            )
            new_bed_bedset = BedFileBedSetRelation(
                bedfile_id=BED_TEST_ID,
                bedset_id=BEDSET_TEST_ID,
            )
            new_files = Files(**get_bedset_files())

            session.add(new_bedset)
            session.add(new_bed_bedset)
            session.add(new_files)

            session.commit()

    def _add_universe(self):
        from bbconf.db_utils import Universes

        with Session(self.db_engine.engine) as session:
            new_univ = Universes(
                id=BED_TEST_ID,
            )
            session.add(new_univ)
            session.commit()
