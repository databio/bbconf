import logging
from typing import Optional, Union
from pathlib import Path
import os

from bbconf.config_parser.bedbaseconfig import BedBaseConfig
from sqlalchemy import select, and_, func, or_
from sqlalchemy.orm import Session
from bbconf.const import PKG_NAME
from bbconf.models.base_models import FileModel, FileModelDict
from bbconf.db_utils import Files
from bbconf.models.extras_models import ExtraFilesResults
from bbconf.config_parser.const import DB_EXTRAS_TYPE

_LOGGER = logging.getLogger(PKG_NAME)


class BBExtras:
    """
    Class that holds DRS objects methods.
    """

    def __init__(self, config: BedBaseConfig):
        """
        :param config: config object
        """
        self.config = config

    def add_extra_file(
        self,
        name: str,
        title: Union[str, Path],
        path: str,
        description: Optional[str] = "",
        genome: Optional[str] = None,
    ) -> None:
        """
        Add files that are not related to bed files, but are important for analysis.

        :param name: Type of the file. e.g. open_signal_matrix
        :param title: Name of the file. e.g. open_signal_matrix_hg38 -> should be unique.
        :param path: Local path to the file.
        :param description: Description of the file that will be uploaded
        """

        if not isinstance(path, Path):
            path = Path(path)

        if not path.is_file():
            raise FileNotFoundError(f"Provided file doesn't exist: '{str(path)}'")

        with Session(self.config.db_engine.engine) as session:

            # aws_file_path = self._upload_extra_file(path, name=name)
            files_dict = {
                name: FileModel(
                    name=name,
                    title=title,
                    path=str(path),
                    description=description,
                    genome_alias=genome,
                )
            }
            files_annotation = FileModelDict(
                **files_dict,
            )

            files_annotation: FileModelDict = self.config.upload_files_s3(
                name, files=files_annotation, base_path="", type=DB_EXTRAS_TYPE
            )
            for key, file in files_annotation:
                session.add(
                    Files(
                        name=file.name,
                        type=DB_EXTRAS_TYPE,
                        title=file.title,
                        path=file.path,
                        description=file.description,
                        size=file.size,
                        genome_alias=file.genome_alias,
                    )
                )

            session.commit()

    def search_files(
        self, query: str = None, limit: int = 10, offset: int = 0
    ) -> ExtraFilesResults:
        """
        Get extra files from the database.

        :param query: Query: Name of the file to get. e.g. 'open_signal_matrix_hg38', or just 'open'
        :param limit: Query: page number to get. [Default: 10].
        :param offset: Query: page size to get. [Default: 0].
        :return: ExtraFilesResults object with the results.
        """

        with Session(self.config.db_engine.engine) as session:

            if query:
                where_statement = and_(
                    or_(
                        Files.name.ilike(f"%{query}%"),
                        Files.description.ilike(f"%{query}%"),
                    ),
                    Files.type == DB_EXTRAS_TYPE,
                )
            else:
                where_statement = Files.type == DB_EXTRAS_TYPE

            results = session.scalars(
                select(Files).where(where_statement).limit(limit).offset(offset)
            )

            total_results = session.execute(
                select(func.count(Files.id)).where(where_statement)
            ).one()[0]

            return ExtraFilesResults(
                limit=limit,
                offset=offset,
                total=total_results,
                results=[
                    FileModel(
                        name=file.name,
                        title=file.title,
                        path=file.path,
                        description=file.description,
                        size=file.size,
                        genome_alias=file.genome_alias,
                        access_methods=self.config.construct_access_method_list(
                            file.path
                        ),
                    )
                    for file in results
                ],
            )

    def get(self, name: str) -> FileModel:
        """
        Get extra file from the database.

        :param name: Name of the file to get. e.g. 'open_signal_matrix_hg38'
        :return: FileModel object with the file metadata.
        """

        with Session(self.config.db_engine.engine) as session:

            where_statement = and_(Files.name == name, Files.type == DB_EXTRAS_TYPE)

            result = session.scalars(select(Files).where(where_statement)).one_or_none()

            if not result:
                raise FileNotFoundError(f"File {name} not found")

            return FileModel(
                name=result.name,
                title=result.title,
                path=result.path,
                description=result.description,
                size=result.size,
                genome_alias=result.genome_alias,
                access_methods=self.config.construct_access_method_list(result.path),
            )

    def delete(self, name: str) -> None:
        """
        Delete extra file from the database.

        :param name: Name of the file to delete. e.g. 'open_signal_matrix_hg38'
        """

        with Session(self.config.db_engine.engine) as session:

            where_statement = and_(Files.name == name, Files.type == DB_EXTRAS_TYPE)

            result = session.scalar(select(Files).where(where_statement))

            if not result:
                raise FileNotFoundError(f"File {name} not found")

            self.config.delete_s3(result.path)

            session.delete(result)

            session.commit()
