import warnings
from bbconf.bbagent import BedBaseAgent

from sqlalchemy.exc import OperationalError

# from .conftest import DNS

DNS = "postgresql+psycopg://postgres:docker@localhost:5432/bedbase"

config = "/home/bnt4me/virginia/repos/bbuploader/config_db_local.yaml"


def db_setup():
    # Check if the database is setup
    try:
        BedBaseAgent(config=config)
    except OperationalError:
        warnings.warn(
            UserWarning(
                f"Skipping tests, because DB is not setup. {DNS}. To setup DB go to README.md"
            )
        )
        return False
    return True


def test_bb_database():
    assert db_setup()

def test_get():
    agent = BedBaseAgent(config=config)
    ff = agent.bed.get("91b2754c8ff01769bacfc80e6923c46e")
    print(ff)
    assert ff != None