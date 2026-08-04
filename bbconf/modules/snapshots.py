import logging
import os
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from bbconf.config_parser import BedBaseConfig
from bbconf.const import PKG_NAME
from bbconf.db_utils import BedSnapshot
from bbconf.exceptions import SnapshotNotFoundError
from bbconf.models.base_models import (
    BedSnapshotArtifact,
    BedSnapshotListResult,
    BedSnapshotResult,
)

_LOGGER = logging.getLogger(PKG_NAME)

# All snapshots live under this single S3 prefix. Not configurable.
SNAPSHOT_S3_PREFIX = "snapshot"


class BedAgentSnapshot:
    """
    Class that manages bulk-export snapshots (the ``bed_snapshots`` index).

    One row per published artifact (metadata / bedsets / bedset_membership /
    manifest). Adding a snapshot always uploads the file to S3 *and* records it
    in the database; both writes live here in bbconf. This class also exposes
    read (``list`` / ``get``) and ``delete`` helpers.
    """

    def __init__(self, config: BedBaseConfig):
        """
        Initialize BedAgentSnapshot.

        Args:
            config: Config object.
        """
        self.config = config
        self._db_engine = self.config.db_engine

    def add(
        self,
        artifacts: BedSnapshotArtifact | list[BedSnapshotArtifact],
        creation_date: datetime | None = None,
    ) -> BedSnapshotListResult:
        """
        Add snapshot artifacts: upload each to S3 and record it in the database.

        Every artifact is uploaded under the fixed ``snapshot/`` prefix and then
        recorded in ``bed_snapshots``. The index rows are written only after all
        uploads succeed, so a partial upload never leaves dangling rows.

        Args:
            artifacts: One artifact or a list of them. Each carries the local
                ``path`` to upload plus its ``file_type`` and file metadata.
            creation_date: Build date recorded on every row
                (defaults to now, UTC).

        Returns:
            The created snapshot rows.
        """
        if isinstance(artifacts, BedSnapshotArtifact):
            artifacts = [artifacts]
        if creation_date is None:
            creation_date = datetime.now(timezone.utc)

        # Upload everything first; only record rows once all uploads succeed.
        results: list[BedSnapshotResult] = []
        for artifact in artifacts:
            key = f"{SNAPSHOT_S3_PREFIX}/{os.path.basename(artifact.path)}"
            self.config.upload_s3(artifact.path, s3_path=key)
            results.append(
                BedSnapshotResult(
                    file_path=key,
                    file_type=artifact.file_type,
                    creation_date=creation_date,
                    record_count=artifact.record_count,
                    file_size=artifact.file_size,
                    checksum=artifact.checksum,
                    schema_version=artifact.schema_version,
                )
            )

        with Session(self._db_engine.engine) as session:
            for result in results:
                session.add(
                    BedSnapshot(
                        file_path=result.file_path,
                        file_type=result.file_type,
                        creation_date=result.creation_date,
                        record_count=result.record_count,
                        file_size=result.file_size,
                        checksum=result.checksum,
                        schema_version=result.schema_version,
                    )
                )
            session.commit()

        _LOGGER.info(f"Recorded {len(results)} rows in bed_snapshots")
        return BedSnapshotListResult(count=len(results), results=results)

    def delete(self, id: int, remove_s3: bool = True) -> None:
        """
        Delete a snapshot index row.

        Args:
            id: Primary key of the snapshot row.
            remove_s3: Also delete the underlying S3 object.

        Returns:
            None.

        Raises:
            SnapshotNotFoundError: If no row with this id exists.
        """
        with Session(self._db_engine.engine) as session:
            row = session.scalar(select(BedSnapshot).where(BedSnapshot.id == id))
            if row is None:
                raise SnapshotNotFoundError(f"Snapshot with id '{id}' not found.")
            file_path = row.file_path
            session.delete(row)
            session.commit()

        if remove_s3:
            self.config.delete_s3(file_path)

    def list(
        self,
        file_type: str | None = None,
        limit: int | None = 100,
        offset: int = 0,
    ) -> BedSnapshotListResult:
        """
        List all snapshot index rows in the database, newest first.

        Args:
            file_type: Optional filter on file type.
            limit: Maximum number of rows to return. ``None`` returns all rows.
            offset: Number of rows to skip.

        Returns:
            List of snapshots and the total matching count.
        """
        statement = select(BedSnapshot)
        count_statement = select(func.count()).select_from(BedSnapshot)
        if file_type is not None:
            statement = statement.where(BedSnapshot.file_type == file_type)
            count_statement = count_statement.where(
                BedSnapshot.file_type == file_type
            )
        statement = statement.order_by(
            BedSnapshot.creation_date.desc(), BedSnapshot.id.desc()
        )
        if limit is not None:
            statement = statement.limit(limit).offset(offset)
        elif offset:
            statement = statement.offset(offset)

        with Session(self._db_engine.engine) as session:
            total = session.execute(count_statement).scalar_one()
            rows = session.scalars(statement).all()
            results = [self._to_result(row) for row in rows]

        return BedSnapshotListResult(count=total, results=results)

    def get_by_filename(self, filename: str) -> BedSnapshotResult:
        """
        Resolve a snapshot by its file name (the basename of its S3 key).

        Returns the newest row whose ``file_path`` basename equals ``filename``.
        Used to round-trip an export's DRS object-id back to its row.

        Args:
            filename: The bare file name, e.g.
                ``bedbase_metadata_2026_08_03.parquet``.

        Returns:
            The matching snapshot row.

        Raises:
            SnapshotNotFoundError: If no row matches.
        """
        filename = os.path.basename(filename)
        with Session(self._db_engine.engine) as session:
            rows = session.scalars(
                select(BedSnapshot)
                .where(BedSnapshot.file_path.like(f"%{filename}"))
                .order_by(
                    BedSnapshot.creation_date.desc(), BedSnapshot.id.desc()
                )
            ).all()
            for row in rows:
                if os.path.basename(row.file_path) == filename:
                    return self._to_result(row)
        raise SnapshotNotFoundError(f"Snapshot '{filename}' not found.")

    def delete_by_checksum(self, checksum: str, remove_s3: bool = True) -> None:
        """
        Delete snapshot index rows by their checksum.

        Deletes every ``bed_snapshots`` row whose ``checksum`` matches (a checksum
        identifies one file's content) and optionally removes the underlying S3
        objects.

        Args:
            checksum: SHA256 checksum of the snapshot file.
            remove_s3: Also delete the underlying S3 object(s).

        Returns:
            None.

        Raises:
            SnapshotNotFoundError: If no row matches the checksum.
        """
        with Session(self._db_engine.engine) as session:
            rows = session.scalars(
                select(BedSnapshot).where(BedSnapshot.checksum == checksum)
            ).all()
            if not rows:
                raise SnapshotNotFoundError(
                    f"Snapshot with checksum '{checksum}' not found."
                )
            file_paths = {row.file_path for row in rows}
            for row in rows:
                session.delete(row)
            session.commit()

        if remove_s3:
            for file_path in file_paths:
                self.config.delete_s3(file_path)

    def get(self, id: int) -> BedSnapshotResult:
        """
        Get a single snapshot index row by id.

        Args:
            id: Primary key of the snapshot row.

        Returns:
            The snapshot row.

        Raises:
            SnapshotNotFoundError: If no row with this id exists.
        """
        with Session(self._db_engine.engine) as session:
            row = session.scalar(select(BedSnapshot).where(BedSnapshot.id == id))
            if row is None:
                raise SnapshotNotFoundError(f"Snapshot with id '{id}' not found.")
            return self._to_result(row)

    @staticmethod
    def _to_result(row: BedSnapshot) -> BedSnapshotResult:
        return BedSnapshotResult(
            file_path=row.file_path,
            file_type=row.file_type,
            creation_date=row.creation_date,
            record_count=row.record_count,
            file_size=row.file_size,
            checksum=row.checksum,
            schema_version=row.schema_version,
        )
