"""
Connection-pool hardening checks.

These do not need a live database: the engine is built lazily, and the one
test that connects short-circuits libpq with a ``do_connect`` handler so we
can read back the parameters psycopg would have been handed.
"""

import pytest
from sqlalchemy import create_engine, event

from bbconf.db_utils import CONNECT_ARGS, POOL_RECYCLE_SECONDS, BaseEngine

KEEPALIVE_KEYS = {
    "connect_timeout",
    "keepalives",
    "keepalives_idle",
    "keepalives_interval",
    "keepalives_count",
    "tcp_user_timeout",
}


def test_connect_args_cover_dead_peer_detection():
    assert KEEPALIVE_KEYS <= set(CONNECT_ARGS)
    assert CONNECT_ARGS["keepalives"] == 1
    # keepalives_idle + keepalives_interval * keepalives_count: a dead idle
    # peer must be noticed in about a minute, not the ~2 h Linux default.
    detection = CONNECT_ARGS["keepalives_idle"] + (
        CONNECT_ARGS["keepalives_interval"] * CONNECT_ARGS["keepalives_count"]
    )
    assert detection <= 120
    assert CONNECT_ARGS["connect_timeout"] <= 10
    assert CONNECT_ARGS["tcp_user_timeout"] <= 60_000


def test_base_engine_hardens_the_pool(monkeypatch):
    """BaseEngine must build its engine with pre-ping, recycle, and keepalives."""
    captured = {}

    def fake_create_engine(dsn, **kwargs):
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        return create_engine("postgresql+psycopg://u:p@localhost:5432/db", **kwargs)

    monkeypatch.setattr("bbconf.db_utils.create_engine", fake_create_engine)
    monkeypatch.setattr(BaseEngine, "create_schema", lambda self, engine=None: None)
    monkeypatch.setattr(BaseEngine, "check_db_connection", lambda self: None)

    base = BaseEngine(host="localhost", database="bedbase", user="u", password="p")

    assert captured["kwargs"]["pool_pre_ping"] is True
    assert captured["kwargs"]["pool_recycle"] == POOL_RECYCLE_SECONDS
    assert KEEPALIVE_KEYS <= set(captured["kwargs"]["connect_args"])

    # And the settings survive onto the live pool object.
    assert base.engine.pool._pre_ping is True
    assert base.engine.pool._recycle == POOL_RECYCLE_SECONDS


def test_connect_args_reach_the_driver():
    """The libpq keywords are actually handed to psycopg, not silently dropped."""
    engine = create_engine(
        "postgresql+psycopg://u:p@localhost:5432/db",
        connect_args=dict(CONNECT_ARGS),
    )

    seen = {}

    class _Stop(Exception):
        pass

    @event.listens_for(engine, "do_connect")
    def _capture(dialect, conn_rec, cargs, cparams):
        seen.update(cparams)
        raise _Stop()

    with pytest.raises(Exception):
        engine.connect()

    for key, value in CONNECT_ARGS.items():
        assert seen[key] == value
