from typing import List
import logging

from sqlalchemy.orm import Session

from bbconf.config_parser import BedBaseConfig
from bbconf.db_utils import BedFileBedSetRelation
from bbconf.db_utils import Bed
from bbconf.db_utils import BedSets

from bbconf.models.bed_models import BedStats

from bbconf.const import PKG_NAME


_LOGGER = logging.getLogger(PKG_NAME)


class BedAgentBedSet:
    """
    Class that represents Bedset in Database.

    This class has method to add, delete, get files and metadata from the database.
    """

    def __init__(self, config: BedBaseConfig):
        """
        :param config: config object
        """
        self.config = config
        self._db_engine = self.config.db_engine

    def get(self, identifier: str) -> dict:
        """
        Get file metadata by identifier.

        :param identifier: bed file identifier
        :return: project metadata
        """
        ...

    def create(
        self,
        identifier: str,
        name: str,
        description: str = None,
        bedid_list: List[str] = None,
        statistics: bool = False,
        plots: dict = None,
        upload_pephub: bool = False,
        no_fail: bool = False,
    ) -> None:
        """
        Create bedset in the database.

        :param identifier: bedset identifier
        :param description: bedset description
        :param bedid_list: list of bed file identifiers
        :param statistics: calculate statistics for bedset
        :param plots: dictionary with plots
        :param upload_pephub: upload bedset to pephub (create view in pephub)
        :param no_fail: do not raise an error if bedset already exists
        :return: None
        """
        _LOGGER.info(f"Creating bedset '{identifier}'")

        new_bedset = BedSets(
            id=identifier,
            description=description,
            name=identifier,
            statistics=statistics,
        )

        # TODO: upload plots! We don't have them now

        # TODO: upload to pephub

        with Session(self._db_engine.engine) as session:
            session.add(new_bedset)

            for bedfile in bedid_list:
                session.add(BedFileBedSetRelation(bedset_id=identifier, bed_id=bedfile))

            session.commit()

    def _calculate_statistics(self, bed_ids: List[str]) -> dict:
        """
        Calculate statistics for bedset.

        :param bed_ids: list of bed file identifiers
        :return: statistics
        """
        numeric_columns = BedStats.model_fields

        results_dict = {"mean": {}, "sd": {}}

        for column_name in numeric_columns:
            with bbc.bed.backend.session as s:
                mean_bedset_statement = select(
                    func.round(
                        func.avg(getattr(bbc.BedfileORM, column_name)).cast(Numeric), 4
                    ).cast(Float)
                ).where(bbc.BedfileORM.record_identifier.in_(bedset))
                sd_bedset_statement = select(
                    func.round(
                        func.stddev(getattr(bbc.BedfileORM, column_name)).cast(Numeric),
                        4,
                    ).cast(Float)
                ).where(bbc.BedfileORM.record_identifier.in_(bedset))

                results_dict["mean"][column_name] = s.exec(mean_bedset_statement).one()
                results_dict["sd"][column_name] = s.exec(sd_bedset_statement).one()

        _LOGGER.info("Bedset statistics were calculated successfully")
        return results_dict

    def delete(self) -> None:
        """
        Delete bed file from the database.

        :param identifier: bed file identifier
        :return: None
        """
        ...

    def add_bedfile(self, identifier: str, bedfile: str) -> None: ...

    def delete_bedfile(self, identifier: str, bedfile: str) -> None: ...
