from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy.engine.url import URL
import logging

from bbconf.const import PKG_NAME

_LOGGER = logging.getLogger(PKG_NAME)


def run_sql_migrations(db_url: URL, revision: str = "head") -> None:
    """
    Execute sql schema migration

    :param db_url: sql url. e.g. postgresql+psycopg://username:password@host:port/database
    :param revision: revision to upgrade to

    :return: None
    """

    ini_dir = Path(__file__).parent.parent
    config_file = ini_dir / "alembic.ini"

    _LOGGER.info(f"Attempting to run migrations with config file: {config_file}")
    config = Config(file_=config_file)

    # set database connection string
    config.set_main_option(
        "sqlalchemy.url", db_url.render_as_string(hide_password=False)
    )

    # set the location of the migrations
    migrations_dir = ini_dir / "db_migrations"
    _LOGGER.info(f"Setting migrations directory to: {migrations_dir}")
    config.set_main_option("script_location", str(migrations_dir))

    # upgrade the database to the latest revision
    command.upgrade(config=config, revision=revision)
    _LOGGER.info("Migrations complete.")
