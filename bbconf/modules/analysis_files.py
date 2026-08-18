import logging
import os
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from bbconf.config_parser import BedBaseConfig
from bbconf.const import PKG_NAME
from bbconf.db_utils import AnalysisFile
from bbconf.exceptions import AnalysisFileNotFoundError
from bbconf.models.base_models import (
    AnalysisFileArtifact,
    AnalysisFileListResult,
    AnalysisFileResult,
)

_LOGGER = logging.getLogger(PKG_NAME)

# All analysis files live under this single S3 prefix. Not configurable.
ANALYSIS_FILES_S3_PREFIX = "analysis_files"


class BedAgentAnalysisFile:
    """
    Class that manages standalone analysis files (the ``analysis_files`` index).

    One row per uploaded file (openSignalMatrix, models, other analysis inputs).
    These files are global: they are not tied to any bed file or bedset. Adding
    a file always uploads it to S3 *and* records it in the database; both writes
    live here in bbconf. This class also exposes read (``list`` / ``get`` /
    ``get_by_name`` / ``get_by_filename``) and ``delete`` helpers.
    """

    def __init__(self, config: BedBaseConfig):
        """
        Initialize BedAgentAnalysisFile.

        Args:
            config: Config object.
        """
        self.config = config
        self._db_engine = self.config.db_engine

    def add(
        self,
        artifacts: AnalysisFileArtifact | list[AnalysisFileArtifact],
        creation_date: datetime | None = None,
    ) -> AnalysisFileListResult:
        """
        Add analysis files: upload each to S3 and record it in the database.

        Every artifact is uploaded under the fixed ``analysis_files/`` prefix and
        then recorded in ``analysis_files``. The index rows are written only
        after all uploads succeed, so a partial upload never leaves dangling
        rows.

        Args:
            artifacts: One artifact or a list of them. Each carries the local
                ``path`` to upload plus its ``name`` and file metadata.
            creation_date: Upload date recorded on every row
                (defaults to now, UTC).

        Returns:
            The created analysis-file rows.
        """
        if isinstance(artifacts, AnalysisFileArtifact):
            artifacts = [artifacts]
        if creation_date is None:
            creation_date = datetime.now(timezone.utc)

        # Upload everything first; only record rows once all uploads succeed.
        uploads: list[tuple[AnalysisFileArtifact, str]] = []
        for artifact in artifacts:
            key = f"{ANALYSIS_FILES_S3_PREFIX}/{os.path.basename(artifact.path)}"
            self.config.upload_s3(artifact.path, s3_path=key)
            uploads.append((artifact, key))

        results: list[AnalysisFileResult] = []
        with Session(self._db_engine.engine) as session:
            for artifact, key in uploads:
                row = AnalysisFile(
                    name=artifact.name,
                    file_path=key,
                    file_type=artifact.file_type,
                    genome=artifact.genome,
                    description=artifact.description,
                    tags=artifact.tags,
                    file_size=artifact.file_size,
                    checksum=artifact.checksum,
                    creation_date=creation_date,
                )
                session.add(row)
                session.flush()
                results.append(self._to_result(row))
            session.commit()

        _LOGGER.info(f"Recorded {len(results)} rows in analysis_files")
        return AnalysisFileListResult(count=len(results), results=results)

    def delete(self, id: int, remove_s3: bool = True) -> None:
        """
        Delete an analysis-file index row.

        Args:
            id: Primary key of the analysis-file row.
            remove_s3: Also delete the underlying S3 object.

        Returns:
            None.

        Raises:
            AnalysisFileNotFoundError: If no row with this id exists.
        """
        with Session(self._db_engine.engine) as session:
            row = session.scalar(select(AnalysisFile).where(AnalysisFile.id == id))
            if row is None:
                raise AnalysisFileNotFoundError(
                    f"Analysis file with id '{id}' not found."
                )
            file_path = row.file_path
            session.delete(row)
            session.commit()

        if remove_s3:
            self.config.delete_s3(file_path)

    def list(
        self,
        file_type: str | None = None,
        genome: str | None = None,
        tag: str | None = None,
        limit: int | None = 100,
        offset: int = 0,
    ) -> AnalysisFileListResult:
        """
        List analysis-file index rows in the database, newest first.

        Args:
            file_type: Optional filter on file type.
            genome: Optional filter on genome/assembly.
            tag: Optional filter; keep only rows whose ``tags`` contain this tag.
            limit: Maximum number of rows to return. ``None`` returns all rows.
            offset: Number of rows to skip.

        Returns:
            List of analysis files and the total matching count.
        """
        statement = select(AnalysisFile)
        count_statement = select(func.count()).select_from(AnalysisFile)
        if file_type is not None:
            statement = statement.where(AnalysisFile.file_type == file_type)
            count_statement = count_statement.where(AnalysisFile.file_type == file_type)
        if genome is not None:
            statement = statement.where(AnalysisFile.genome == genome)
            count_statement = count_statement.where(AnalysisFile.genome == genome)
        if tag is not None:
            statement = statement.where(AnalysisFile.tags.any(tag))
            count_statement = count_statement.where(AnalysisFile.tags.any(tag))
        statement = statement.order_by(
            AnalysisFile.creation_date.desc(), AnalysisFile.id.desc()
        )
        if limit is not None:
            statement = statement.limit(limit).offset(offset)
        elif offset:
            statement = statement.offset(offset)

        with Session(self._db_engine.engine) as session:
            total = session.execute(count_statement).scalar_one()
            rows = session.scalars(statement).all()
            results = [self._to_result(row) for row in rows]

        return AnalysisFileListResult(count=total, results=results)

    def get(self, id: int) -> AnalysisFileResult:
        """
        Get a single analysis-file index row by id.

        Args:
            id: Primary key of the analysis-file row.

        Returns:
            The analysis-file row.

        Raises:
            AnalysisFileNotFoundError: If no row with this id exists.
        """
        with Session(self._db_engine.engine) as session:
            row = session.scalar(select(AnalysisFile).where(AnalysisFile.id == id))
            if row is None:
                raise AnalysisFileNotFoundError(
                    f"Analysis file with id '{id}' not found."
                )
            return self._to_result(row)

    def get_by_name(self, name: str, genome: str | None = None) -> AnalysisFileResult:
        """
        Resolve an analysis file by its logical name (newest matching row).

        Args:
            name: Logical name/key, e.g. ``openSignalMatrix``.
            genome: Optional genome/assembly to disambiguate, e.g. ``hg38``.

        Returns:
            The newest matching analysis-file row.

        Raises:
            AnalysisFileNotFoundError: If no row matches.
        """
        statement = select(AnalysisFile).where(AnalysisFile.name == name)
        if genome is not None:
            statement = statement.where(AnalysisFile.genome == genome)
        statement = statement.order_by(
            AnalysisFile.creation_date.desc(), AnalysisFile.id.desc()
        )
        with Session(self._db_engine.engine) as session:
            row = session.scalars(statement).first()
            if row is None:
                raise AnalysisFileNotFoundError(f"Analysis file '{name}' not found.")
            return self._to_result(row)

    def get_by_filename(self, filename: str) -> AnalysisFileResult:
        """
        Resolve an analysis file by its file name (the basename of its S3 key).

        Returns the newest row whose ``file_path`` basename equals ``filename``.
        Used to round-trip a DRS object-id back to its row.

        Args:
            filename: The bare file name, e.g. ``openSignalMatrix_hg38.txt.gz``.

        Returns:
            The matching analysis-file row.

        Raises:
            AnalysisFileNotFoundError: If no row matches.
        """
        filename = os.path.basename(filename)
        with Session(self._db_engine.engine) as session:
            rows = session.scalars(
                select(AnalysisFile)
                .where(AnalysisFile.file_path.like(f"%{filename}"))
                .order_by(AnalysisFile.creation_date.desc(), AnalysisFile.id.desc())
            ).all()
            for row in rows:
                if os.path.basename(row.file_path) == filename:
                    return self._to_result(row)
        raise AnalysisFileNotFoundError(f"Analysis file '{filename}' not found.")

    def delete_by_checksum(self, checksum: str, remove_s3: bool = True) -> None:
        """
        Delete analysis-file index rows by their checksum.

        Deletes every ``analysis_files`` row whose ``checksum`` matches (a
        checksum identifies one file's content) and optionally removes the
        underlying S3 objects.

        Args:
            checksum: SHA256 checksum of the analysis file.
            remove_s3: Also delete the underlying S3 object(s).

        Returns:
            None.

        Raises:
            AnalysisFileNotFoundError: If no row matches the checksum.
        """
        with Session(self._db_engine.engine) as session:
            rows = session.scalars(
                select(AnalysisFile).where(AnalysisFile.checksum == checksum)
            ).all()
            if not rows:
                raise AnalysisFileNotFoundError(
                    f"Analysis file with checksum '{checksum}' not found."
                )
            file_paths = {row.file_path for row in rows}
            for row in rows:
                session.delete(row)
            session.commit()

        if remove_s3:
            for file_path in file_paths:
                self.config.delete_s3(file_path)

    @staticmethod
    def _to_result(row: AnalysisFile) -> AnalysisFileResult:
        return AnalysisFileResult(
            id=row.id,
            name=row.name,
            file_path=row.file_path,
            file_type=row.file_type,
            genome=row.genome,
            description=row.description,
            tags=list(row.tags) if row.tags is not None else None,
            file_size=row.file_size,
            checksum=row.checksum,
            creation_date=row.creation_date,
        )
