"""Each configured connection checks sessions out of its OWN pool.

Live defect: every PostgreSQL connection shared one module-level list of idle
sessions and one semaphore. A second configured connection — another
database, another role, another ``search_path`` — was handed a session the
first one had parked, so a statement meant for the reporting database ran on
a session of the primary, under the primary's role. One connection's traffic
also spent the other's permits, and a session that never came out of a pool
(pooling off, or set on the wrapper by hand) was put into the shared list on
close. ``PostgresPool`` now keys a pool on the connection name plus the
resolved DSN/session options; a permit goes back to the pool that issued it,
also when an interrupt (``BaseException``) cuts the checkout short.

Driven through a fake psycopg2 whose sessions remember what they were opened
with.
"""

from __future__ import annotations

import pytest

from cara.eloquent import DatabaseManager
from cara.eloquent.connections import PostgresConnection, PostgresPool

from ._fixtures import install_fake_psycopg2, pool_of


class _Info:
    transaction_status = 0


class _Cursor:
    def __init__(self, session):
        self._session = session

    def execute(self, query, bindings=None):
        self._session.log.append(query)
        if self._session.on_execute is not None:
            self._session.on_execute(query)

    def close(self):
        return None


class _Session:
    """One fake server session, tagged with the DSN it was opened with."""

    def __init__(self, **dsn):
        self.dsn = dsn
        self.closed = False
        self.autocommit = True
        self.info = _Info()
        self.log: list[str] = []
        self.on_execute = None

    def cursor(self, cursor_factory=None):
        return _Cursor(self)

    def rollback(self):
        return None

    def close(self):
        self.closed = True


@pytest.fixture
def sessions(monkeypatch):
    opened: list[_Session] = []

    def connect(**dsn):
        opened.append(_Session(**dsn))
        return opened[-1]

    install_fake_psycopg2(monkeypatch, connect)
    PostgresPool.reset_all(close=False)
    yield opened
    PostgresPool.reset_all(close=False)


def _manager(**databases) -> DatabaseManager:
    """One pooled PostgreSQL connection per ``name=database`` pair."""
    return DatabaseManager(
        next(iter(databases)),
        {
            name: {
                "driver": "postgres",
                "host": "db",
                "database": database,
                "user": "u",
                "port": 5432,
                "password": "p",
                "connection_pooling_enabled": True,
                "connection_pooling_max_size": 1,
            }
            for name, database in databases.items()
        },
    )


def _wrapper(**details) -> PostgresConnection:
    return PostgresConnection(
        host="db",
        database="main",
        user="u",
        port=5432,
        password="p",
        full_details={
            "connection_pooling_enabled": True,
            "connection_pooling_max_size": 2,
            **details,
        },
    )


def test_a_session_parked_by_one_connection_is_never_handed_to_another(sessions):
    manager = _manager(app="main", reporting="replica")

    app = manager.create_connection_instance("app")
    primary = app._connection
    app.close_connection()  # parked, idle, in app's pool

    reporting = manager.create_connection_instance("reporting")
    assert reporting._connection is not primary, (
        "the reporting connection was handed a session of the primary"
    )
    assert reporting._connection.dsn["database"] == "replica"
    reporting.close_connection()

    again = manager.create_connection_instance("app")
    assert again._connection is primary, "app's own parked session is reused"
    again.close_connection()
    assert len(sessions) == 2


def test_two_names_on_one_database_are_two_budgets(sessions):
    manager = _manager(app="main", jobs="main")

    app = manager.create_connection_instance("app")  # app's only permit
    jobs = manager.create_connection_instance("jobs", connect=False)
    jobs._POOL_ACQUIRE_TIMEOUT = 5  # a shared pool would wait it out, then 503
    jobs.make_connection()

    assert jobs._connection is not app._connection
    assert pool_of(app).stats()["in_use"] == 1
    assert pool_of(jobs).stats()["in_use"] == 1
    app.close_connection()
    jobs.close_connection()


def test_a_session_opened_under_one_search_path_never_serves_another(sessions):
    plain = _wrapper()
    plain.make_connection()
    plain.close_connection()  # parked under the default search_path

    tenant = _wrapper().set_schema("tenant_a")
    tenant.make_connection()

    assert tenant._connection is not sessions[0]
    assert tenant._connection.dsn["options"] == "-c search_path=tenant_a"
    tenant.close_connection()


def test_a_permit_goes_back_to_the_pool_that_issued_it(sessions):
    pc = _wrapper()
    pc.make_connection()
    issuing = pool_of(pc)

    pc.set_schema("tenant_a")  # the wrapper now resolves another pool
    other = pool_of(pc)
    assert other is not issuing
    pc.close_connection()

    assert issuing.stats()["in_use"] == 0
    assert issuing.stats()["idle"] == 1, "the session is parked where it came from"
    assert other.stats() | {"connection": None} == {
        "connection": None,
        "in_use": 0,
        "idle": 0,
        "max": 2,
    }


def test_an_interrupt_while_probing_a_parked_session_returns_the_permit(sessions):
    pc = _wrapper()
    stale = _Session()

    def interrupted(_query):
        raise KeyboardInterrupt

    stale.on_execute = interrupted
    pool_of(pc).warm(lambda: stale, 1)  # pre-warmed: probed on checkout

    with pytest.raises(KeyboardInterrupt):
        pc.make_connection()

    assert pool_of(pc).stats()["in_use"] == 0, "the interrupt kept a permit"
    assert pc._permit_pool is None


def test_an_interrupt_during_session_setup_returns_the_permit(sessions, monkeypatch):
    pc = _wrapper(foreign_keys=True)  # setup runs a statement on the session

    def interrupted(_query):
        raise KeyboardInterrupt

    def connect(**dsn):
        session = _Session(**dsn)
        session.on_execute = interrupted
        return session

    install_fake_psycopg2(monkeypatch, connect)

    with pytest.raises(KeyboardInterrupt):
        pc.make_connection()

    assert pool_of(pc).stats()["in_use"] == 0, "the interrupt kept a permit"
    assert pc._connection is None


def test_a_session_not_taken_from_a_pool_is_closed_not_pooled(sessions):
    pc = _wrapper()
    by_hand = _Session()
    pc._connection = by_hand  # never checked out: no permit, no pool

    pc.close_connection()

    assert by_hand.closed is True
    assert pool_of(pc).stats()["idle"] == 0, "a foreign session was pooled"
