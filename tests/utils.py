from typing import Union

from sqlalchemy.orm import Session

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from bbconf.db_utils import Bed, BedFileBedSetRelation, BedSets, BedStats, Files

BED_TEST_ID = "bbad85f21962bb8d972444f7f9a3a932"
BED_TEST_ID_2 = "cccc00000000000000000000000002"
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
    "distributions": {
        "scalars": {
            "number_of_regions": 1500,
            "mean_region_width": 300.0,
            "median_tss_dist": 5000.0,
            "gc_content": 0.45,
        },
        "partitions": {
            "total": 1500,
            "counts": [
                ["promoterCore", 5],
                ["promoterProx", 60],
                ["threeUTR", 200],
                ["fiveUTR", 15],
                ["exon", 180],
                ["intron", 700],
                ["intergenic", 340],
            ],
        },
        "distributions": {
            "tss_distances": {
                "x_min": -100000.0,
                "x_max": 100000.0,
                "bins": 100,
                "counts": (
                    [3, 3, 4, 4, 5, 5, 6, 7, 8, 9]
                    + [10, 12, 14, 16, 18, 20, 22, 25, 28, 32]
                    + [36, 40, 44, 48, 52, 55, 58, 60, 62, 63]
                    + [64, 65, 66, 66, 65, 64, 63, 62, 60, 58]
                    + [55, 52, 48, 44, 40, 36, 32, 28, 25, 22]
                    + [22, 25, 28, 32, 36, 40, 44, 48, 52, 55]
                    + [58, 60, 62, 63, 64, 65, 66, 66, 65, 64]
                    + [63, 62, 60, 58, 55, 52, 48, 44, 40, 36]
                    + [32, 28, 25, 22, 20, 18, 16, 14, 12, 10]
                    + [9, 8, 7, 6, 5, 5, 4, 4, 3, 3]
                ),
                "total": 1500,
            },
            "widths": {
                "x_min": 150.0,
                "x_max": 1018.0,
                "bins": 50,
                "counts": (
                    [120, 95, 80, 72, 65, 60, 55, 50, 46, 42]
                    + [38, 35, 32, 29, 27, 25, 23, 21, 19, 18]
                    + [16, 15, 14, 13, 12, 11, 10, 10, 9, 8]
                    + [8, 7, 7, 6, 6, 5, 5, 5, 4, 4]
                    + [4, 3, 3, 3, 3, 2, 2, 2, 2, 2]
                ),
                "total": 1500,
                "overflow": 15,
            },
            "neighbor_distances": {
                "x_min": 1.4,
                "x_max": 6.0,
                "n": 512,
                "densities": [
                    round(0.55 * __import__("math").exp(-0.5 * ((i - 220) / 60) ** 2), 4)
                    for i in range(512)
                ],
            },
            "gc_content": {
                "x_min": 0.19,
                "x_max": 0.62,
                "n": 512,
                "densities": [
                    round(4.5 * __import__("math").exp(-0.5 * ((i - 256) / 80) ** 2), 4)
                    for i in range(512)
                ],
                "mean": 0.412,
            },
            "region_distribution": {
                "chr1": [10, 5, 8, 12, 3, 7, 15, 2, 9, 6] + [4] * 240,
                "chr2": [3, 7, 2, 8, 5, 11, 4, 6, 9, 1] + [3] * 240,
            },
            "chromosome_stats": {
                "chr1": {
                    "chromosome": "chr1",
                    "number_of_regions": 800,
                    "start_nucleotide_position": 950036,
                    "end_nucleotide_position": 248917955,
                    "minimum_region_length": 150,
                    "maximum_region_length": 2402,
                    "mean_region_length": 317.58,
                    "median_region_length": 261.5,
                },
                "chr2": {
                    "chromosome": "chr2",
                    "number_of_regions": 700,
                    "start_nucleotide_position": 217434,
                    "end_nucleotide_position": 241746325,
                    "minimum_region_length": 150,
                    "maximum_region_length": 2411,
                    "mean_region_length": 316.0,
                    "median_region_length": 259.0,
                },
            },
        },
        "expected_partitions": {
            "rows": [
                {"partition": "promoterCore", "observed": 5.0, "expected": 12.0, "log10_oe": -0.38, "chi_sq_pval": 0.04},
                {"partition": "promoterProx", "observed": 60.0, "expected": 110.0, "log10_oe": -0.26, "chi_sq_pval": 0.0},
                {"partition": "threeUTR", "observed": 200.0, "expected": 25.0, "log10_oe": 0.90, "chi_sq_pval": 0.0},
                {"partition": "fiveUTR", "observed": 15.0, "expected": 5.0, "log10_oe": 0.48, "chi_sq_pval": 0.01},
                {"partition": "exon", "observed": 180.0, "expected": 24.0, "log10_oe": 0.88, "chi_sq_pval": 0.0},
                {"partition": "intron", "observed": 700.0, "expected": 600.0, "log10_oe": 0.07, "chi_sq_pval": 0.0},
                {"partition": "intergenic", "observed": 340.0, "expected": 824.0, "log10_oe": -0.38, "chi_sq_pval": 0.0},
            ],
        },
    },
}


stats_2 = {
    "id": BED_TEST_ID_2,
    "number_of_regions": 100,
    "median_tss_dist": 200,
    "mean_region_width": 400,
    "exon_frequency": 40,
    "exon_percentage": 50,
    "intron_frequency": 60,
    "intron_percentage": 70,
    "intergenic_percentage": 80,
    "intergenic_frequency": 90,
    "promotercore_frequency": 100,
    "promotercore_percentage": 110,
    "fiveutr_frequency": 120,
    "fiveutr_percentage": 130,
    "threeutr_frequency": 140,
    "threeutr_percentage": 150,
    "promoterprox_frequency": 160,
    "promoterprox_percentage": 170,
    "distributions": {
        "scalars": {
            "number_of_regions": 2500,
            "mean_region_width": 500.0,
            "median_tss_dist": 8000.0,
            "gc_content": 0.55,
        },
        "partitions": {
            "total": 2500,
            "counts": [
                ["promoterCore", 10],
                ["promoterProx", 100],
                ["threeUTR", 300],
                ["fiveUTR", 25],
                ["exon", 250],
                ["intron", 1200],
                ["intergenic", 615],
            ],
        },
        "distributions": {
            "tss_distances": {
                "x_min": -100000.0,
                "x_max": 100000.0,
                "bins": 100,
                "counts": (
                    [6, 6, 8, 8, 10, 10, 12, 14, 16, 18]
                    + [20, 24, 28, 32, 36, 40, 44, 50, 56, 64]
                    + [72, 80, 88, 96, 104, 110, 116, 120, 124, 126]
                    + [128, 130, 132, 132, 130, 128, 126, 124, 120, 116]
                    + [110, 104, 96, 88, 80, 72, 64, 56, 50, 44]
                    + [44, 50, 56, 64, 72, 80, 88, 96, 104, 110]
                    + [116, 120, 124, 126, 128, 130, 132, 132, 130, 128]
                    + [126, 124, 120, 116, 110, 104, 96, 88, 80, 72]
                    + [64, 56, 50, 44, 40, 36, 32, 28, 24, 20]
                    + [18, 16, 14, 12, 10, 10, 8, 8, 6, 6]
                ),
                "total": 2500,
            },
            "widths": {
                "x_min": 200.0,
                "x_max": 1200.0,
                "bins": 50,
                "counts": (
                    [200, 160, 140, 120, 110, 100, 90, 80, 75, 70]
                    + [65, 60, 55, 50, 47, 44, 41, 38, 35, 33]
                    + [30, 28, 26, 24, 22, 20, 19, 18, 17, 16]
                    + [15, 14, 13, 12, 11, 10, 10, 9, 8, 8]
                    + [7, 6, 6, 5, 5, 4, 4, 3, 3, 3]
                ),
                "total": 2500,
                "overflow": 25,
            },
            "neighbor_distances": {
                "x_min": 1.2,
                "x_max": 6.5,
                "n": 512,
                "densities": [
                    round(0.45 * __import__("math").exp(-0.5 * ((i - 200) / 70) ** 2), 4)
                    for i in range(512)
                ],
            },
            "gc_content": {
                "x_min": 0.15,
                "x_max": 0.65,
                "n": 512,
                "densities": [
                    round(3.8 * __import__("math").exp(-0.5 * ((i - 280) / 90) ** 2), 4)
                    for i in range(512)
                ],
                "mean": 0.52,
            },
            "region_distribution": {
                "chr1": [20, 10, 16, 24, 6, 14, 30, 4, 18, 12] + [8] * 240,
                "chr2": [6, 14, 4, 16, 10, 22, 8, 12, 18, 2] + [6] * 240,
            },
            "chromosome_stats": {
                "chr1": {
                    "chromosome": "chr1",
                    "number_of_regions": 1400,
                    "start_nucleotide_position": 850000,
                    "end_nucleotide_position": 248900000,
                    "minimum_region_length": 200,
                    "maximum_region_length": 3000,
                    "mean_region_length": 450.0,
                    "median_region_length": 380.0,
                },
                "chr2": {
                    "chromosome": "chr2",
                    "number_of_regions": 1100,
                    "start_nucleotide_position": 200000,
                    "end_nucleotide_position": 241700000,
                    "minimum_region_length": 200,
                    "maximum_region_length": 2800,
                    "mean_region_length": 420.0,
                    "median_region_length": 350.0,
                },
            },
        },
        "expected_partitions": {
            "rows": [
                {"partition": "promoterCore", "observed": 10.0, "expected": 20.0, "log10_oe": -0.30, "chi_sq_pval": 0.02},
                {"partition": "promoterProx", "observed": 100.0, "expected": 180.0, "log10_oe": -0.26, "chi_sq_pval": 0.0},
                {"partition": "threeUTR", "observed": 300.0, "expected": 40.0, "log10_oe": 0.88, "chi_sq_pval": 0.0},
                {"partition": "fiveUTR", "observed": 25.0, "expected": 8.0, "log10_oe": 0.50, "chi_sq_pval": 0.005},
                {"partition": "exon", "observed": 250.0, "expected": 40.0, "log10_oe": 0.80, "chi_sq_pval": 0.0},
                {"partition": "intron", "observed": 1200.0, "expected": 1000.0, "log10_oe": 0.08, "chi_sq_pval": 0.0},
                {"partition": "intergenic", "observed": 615.0, "expected": 1412.0, "log10_oe": -0.36, "chi_sq_pval": 0.0},
            ],
        },
    },
}


def get_example_dict() -> dict:
    value = {
        "id": BED_TEST_ID,
        "data_format": "encode_narrowpeak",
        "bed_compliance": "bed6+4",
        "genome_alias": "hg38",
        "genome_digest": "2230c535660fb4774114bfa966a62f823fdb6d21acf138d4",
        "name": "random_name",
        "processed": False,
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
        add_data_2: bool = False,
    ):
        """
        :param config: config object
        :param add_data: add data to the database
        :param bedset: add bedset data to the database
        :param add_data_2: add second BED record (for multi-file aggregation tests)
        """
        if isinstance(config, BedBaseConfig):
            self.config = config
        else:
            self.config = BedBaseConfig(config)

        self.add_data = add_data
        self.bedset = bedset
        self.add_data_2 = add_data_2

        self.db_engine = self.config.db_engine
        self.db_engine.create_schema()

    def __enter__(self):
        if self.add_data:
            self._add_data()
            if self.add_data_2:
                self._add_data_2()
            if self.bedset:
                self._add_bedset_data()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        # If we want to keep data, and schema, comment out the following line
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

    def _add_data_2(self):
        """Insert a second BED record with different distributions for multi-file tests."""
        with Session(self.db_engine.engine) as session:
            new_bed = Bed(
                id=BED_TEST_ID_2,
                data_format="encode_narrowpeak",
                bed_compliance="bed6+4",
                genome_alias="hg38",
                genome_digest="2230c535660fb4774114bfa966a62f823fdb6d21acf138d4",
                name="second_test_bed",
                processed=False,
            )
            new_stats = BedStats(**stats_2)
            session.add(new_bed)
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
                processed=False,
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
