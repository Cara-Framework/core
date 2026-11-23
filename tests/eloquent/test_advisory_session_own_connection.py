"""``DatabaseManager.advisory_session`` holds its lock on a session of its own.

Live defect: the session came from ``create_connection_instance``, which,
inside an open transaction, hands back the transaction's PINNED wrapper. The
lock was taken on the transaction's session and the block's ``finally``
closed that wrapper, so the caller's transaction lost its connection
mid-flight and its commit ran on nothing. And when the unlock never ran,
the session went back to the pool still holding the lock, locking every
later ``schema:apply`` out until the idle connection happened to close.

Driven through a fake psycopg2: no PostgreSQL needed.
"""

from __future__ import annotations

import importlib
import sys
import types

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections import PostgresPool

from ._fixtures import install_fake_psycopg2, pool_of

ResolverModule = importlib.import_module("cara.eloquent.connections.ConnectionResolver")


class _Info:
    transaction_status = 0


class _Cursor:
    def __init__(self, session):
        self._session = session

    def execute(self, query, bindings=None):
        self._session.log.append(query)
        if self._session.refuse is not None and self._session.refuse in query:
            raise RuntimeError(f"{self._session.refuse} refused")

    def fetchone(self):
        return {"acquired": self._session.lock_granted}

    def close(self):
        return None


class _Session:
    def __init__(self, lock_granted: bool, refuse: str | None):
        self.closed = False
        self.autocommit = True
        self.info = _Info()
        self.log: list[str] = []
        self.lock_granted = lock_granted
        self.refuse = refuse

    def cursor(self, cursor_factory=None):
        return _Cursor(self)

    def commit(self):
        return None

    def rollback(self):
        return None

    def close(self):
        self.closed = True


@pytest.fixture
def driver(monkeypatch):
    """``driver.sessions``: every session opened; ``driver.lock_granted`` and
    ``driver.refuse`` (a statement fragment that raises) shape new ones."""
    state = types.SimpleNamespace(sessions=[], lock_granted=True, refuse=None)

    def connect(**_dsn):
        state.sessions.append(_Session(state.lock_granted, state.refuse))
        return state.sessions[-1]

    install_fake_psycopg2(monkeypatch, connect)
    monkeypatch.setitem(
        sys.modules, "psycopg2.extras", types.SimpleNamespace(RealDictCursor=object)
    )
    PostgresPool.reset_all(close=False)
    ResolverModule.reset_registry()
    yield state
    ResolverModule.reset_registry()
    PostgresPool.reset_all(close=False)


def _manager() -> DatabaseManager:
    return DatabaseManager(
        "app",
        {
            "app": {
                "driver": "postgres",
                "host": "db",
                "database": "main",
                "user": "u",
                "port": 5432,
                "password": "p",
                "connection_pooling_enabled": True,
                "connection_pooling_max_size": 3,
            }
        },
    )


def _pool(manager):
    return pool_of(manager.create_connection_instance("app", connect=False))


def _ran_lock(session) -> bool:
    return any("pg_try_advisory_lock" in query for query in session.log)


def test_inside_a_transaction_the_lock_takes_a_session_of_its_own(driver):
    manager = _manager()

    with manager.transaction("app"):
        pinned = ResolverModule._get_registry()["app"]
        transaction_session = pinned._connection

        with manager.advisory_session(42) as acquired:
            assert acquired is True

        assert not _ran_lock(transaction_session), (
            "the lock was taken on the transaction's own session"
        )
        assert pinned._connection is transaction_session, (
            "advisory_session closed the transaction's pinned connection"
        )
        assert transaction_session.closed is False
        assert pinned.transaction_level == 1

    lock_sessions = [session for session in driver.sessions if _ran_lock(session)]
    assert len(lock_sessions) == 1
    assert lock_sessions[0] is not transaction_session


def test_an_unlocked_session_goes_back_to_its_pool(driver):
    manager = _manager()

    with manager.advisory_session(42) as acquired:
        assert acquired is True

    (session,) = driver.sessions
    assert any("pg_advisory_unlock" in query for query in session.log)
    assert session.closed is False
    assert _pool(manager).stats()["in_use"] == 0
    assert _pool(manager).stats()["idle"] == 1


def test_a_lock_not_granted_is_not_unlocked(driver):
    driver.lock_granted = False
    manager = _manager()

    with manager.advisory_session(42) as acquired:
        assert acquired is False

    (session,) = driver.sessions
    assert not any("pg_advisory_unlock" in query for query in session.log)
    assert _pool(manager).stats()["idle"] == 1


def test_a_session_whose_unlock_did_not_run_is_closed_not_pooled(driver):
    driver.refuse = "pg_advisory_unlock"
    manager = _manager()

    with (
        pytest.raises(RuntimeError, match="unlock refused"),
        manager.advisory_session(42) as acquired,
    ):
        assert acquired is True

    (session,) = driver.sessions
    assert session.closed is True, "a session still holding the lock stayed open"
    assert _pool(manager).stats()["idle"] == 0, "the lock holder was pooled"
    assert _pool(manager).stats()["in_use"] == 0, "its permit was kept"
