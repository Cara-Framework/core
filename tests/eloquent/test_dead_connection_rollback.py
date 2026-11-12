"""A transaction whose server session died gives its pool permit back.

Live (db-chaos stress, 2026-11-12): ``pg_terminate_backend`` on a session in
the middle of ``with DB.transaction()`` — what a PostgreSQL restart or
failover, a network drop or ``idle_in_transaction_session_timeout`` does —
made the next statement raise ``OperationalError``. The context manager then
rolled back, ``PostgresConnection.rollback`` called ``rollback()`` on the
closed psycopg2 connection, and that raised ``InterfaceError: connection
already closed``. The ``InterfaceError`` replaced the real error, and it left
``ConnectionResolver.rollback`` before the resolver unpinned the connection
and handed its pool permit back. A queue job, or a dispatch on a short-lived
thread, gets a fresh registry, so the permit was gone for the life of the
process. With ``DB_POOL_MAX=3``, three killed sessions left the process
waiting 30 s for a connection and then failing with "Connection pool
exhausted" on every query, until it restarted.

The server ends every open transaction level with the session, so rolling
back a dead session has nothing to undo. The rollback now drops the dead
handle and releases its permit. The resolver also unpins and closes
the connection when a rollback raises for some other reason.

The unit tests use a fake driver. The PostgreSQL test (``CARA_RUN_INTEGRATION=1``,
``DB_DATABASE=synk_scratch_*``) repeats the live failure with real sessions.
"""

from __future__ import annotations

import importlib
import os
import threading

import pytest

from ._fixtures import install_fake_psycopg2

PGModule = importlib.import_module("cara.eloquent.connections.PostgresConnection")
ResolverModule = importlib.import_module("cara.eloquent.connections.ConnectionResolver")
PostgresConnection = PGModule.PostgresConnection
ConnectionResolver = ResolverModule.ConnectionResolver


def _fresh_pool(monkeypatch, *, size: int) -> threading.Semaphore:
    sem = threading.Semaphore(size)
    monkeypatch.setattr(PGModule, "_pool_initialized", True)
    monkeypatch.setattr(PGModule, "_pool_semaphore", sem)
    monkeypatch.setattr(PGModule, "CONNECTION_POOL", [])
    return sem


def _make_pc(size: int) -> PostgresConnection:
    return PostgresConnection(
        host="x",
        database="x",
        user="x",
        port=5432,
        password="x",
        full_details={
            "connection_pooling_enabled": True,
            "connection_pooling_max_size": size,
        },
    )


class _Closed(Exception):
    """psycopg2.InterfaceError: connection already closed."""


class _DyingConnection:
    """A psycopg2-shaped session the server terminates on the next statement."""

    def __init__(self, fake_driver):
        self.closed = 0
        self.autocommit = True
        self.info = type("Info", (), {"transaction_status": 0})()
        self._driver = fake_driver

    def cursor(self, *args, **kwargs):
        session = self

        class _Cursor:
            description = None
            rowcount = 0

            def execute(self, *_a, **_k):
                if session.closed:
                    raise _Closed("connection already closed")

            def close(self):
                return None

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return _Cursor()

    def kill(self):
        self.closed = 2

    def rollback(self):
        if self.closed:
            raise _Closed("connection already closed")

    def commit(self):
        if self.closed:
            raise _Closed("connection already closed")

    def close(self):
        self.closed = 1


@pytest.fixture
def driver(monkeypatch):
    sessions: list[_DyingConnection] = []

    def _connect(**_kw):
        session = _DyingConnection(fake)
        sessions.append(session)
        return session

    fake = install_fake_psycopg2(monkeypatch, connect_factory=_connect)
    fake.InterfaceError = _Closed
    fake.sessions = sessions
    return fake


def test_rolling_back_a_dead_session_releases_its_permit(monkeypatch, driver):
    sem = _fresh_pool(monkeypatch, size=3)
    pc = _make_pc(3)
    pc.begin()
    assert sem._value == 2
    driver.sessions[-1].kill()

    pc.rollback()  # the server already ended the transaction: nothing to raise

    assert pc.transaction_level == 0
    assert sem._value == 3, "the dead session kept its pool permit"
    assert pc._connection is None


def test_nested_rollback_on_a_dead_session_unwinds_one_level_at_a_time(
    monkeypatch, driver
):
    sem = _fresh_pool(monkeypatch, size=3)
    pc = _make_pc(3)
    pc.begin()
    pc.begin()  # SAVEPOINT sp_1
    driver.sessions[-1].kill()

    pc.rollback()  # inner: the enclosing transaction still owns the handle
    assert pc.transaction_level == 1
    assert sem._value == 2

    pc.rollback()  # outer: now the dead handle and its permit go
    assert pc.transaction_level == 0
    assert sem._value == 3


def test_a_live_rollback_failure_still_raises(monkeypatch, driver):
    """Only a DEAD session is excused: a live one that refuses ROLLBACK raises."""
    _fresh_pool(monkeypatch, size=3)
    pc = _make_pc(3)
    pc.begin()

    def _refuse():
        raise RuntimeError("rollback refused on a live session")

    driver.sessions[-1].rollback = _refuse
    with pytest.raises(RuntimeError):
        pc.rollback()


class _Manager:
    def __init__(self, pc):
        self.pc = pc

    def get_connection_class(self, _name):
        return None


def _resolver_with(pc) -> ConnectionResolver:
    resolver = ConnectionResolver.__new__(ConnectionResolver)
    resolver._create_connection_instance = lambda _name: pc
    return resolver


def test_resolver_unpins_and_frees_the_permit_when_rollback_raises(monkeypatch, driver):
    """Belt: a rollback that raises (any reason) still unpins at level 0."""
    sem = _fresh_pool(monkeypatch, size=2)
    ResolverModule.reset_registry()
    pc = _make_pc(2)
    resolver = _resolver_with(pc)
    resolver.begin_transaction("app")
    assert sem._value == 1

    def _refuse():
        raise RuntimeError("rollback refused")

    driver.sessions[-1].rollback = _refuse
    with pytest.raises(RuntimeError):
        resolver.rollback("app")

    assert "app" not in ResolverModule._get_registry()
    assert sem._value == 2, "a failed rollback kept the transaction's pool permit"


def test_dead_sessions_in_fresh_contexts_never_drain_the_pool(monkeypatch, driver):
    """The live failure: one killed session per job context, more kills than permits."""
    sem = _fresh_pool(monkeypatch, size=3)

    def _one_job():
        ResolverModule.reset_registry()  # what every queue job gets
        pc = _make_pc(3)
        resolver = _resolver_with(pc)
        with pytest.raises(_Closed), resolver.transaction("app"):
            driver.sessions[-1].kill()
            pc.set_cursor = lambda: None
            raise _Closed("server closed the connection unexpectedly")

    for _ in range(5):
        worker = threading.Thread(target=_one_job)
        worker.start()
        worker.join()

    assert sem._value == 3, f"{3 - sem._value} pool permit(s) leaked to dead sessions"


_integration = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and DB_DATABASE=synk_scratch_*",
)


@_integration
def test_postgres_terminated_sessions_mid_transaction_keep_the_pool_whole(monkeypatch):
    import psycopg2

    sem = _fresh_pool(monkeypatch, size=2)
    side = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    side.autocommit = True
    errors: list[str] = []

    def _one_job():
        ResolverModule.reset_registry()  # what every queue job gets
        pc = PostgresConnection(
            host=os.getenv("DB_HOST", "127.0.0.1"),
            database=os.environ["DB_DATABASE"],
            user=os.getenv("DB_USERNAME", "postgres"),
            port=int(os.getenv("DB_PORT", "5432")),
            password=os.getenv("DB_PASSWORD", ""),
            full_details={
                "connection_pooling_enabled": True,
                "connection_pooling_max_size": 2,
            },
        )
        resolver = _resolver_with(pc)
        try:
            with resolver.transaction("app"):
                pc.set_cursor()
                pc.statement("SELECT pg_backend_pid() AS pid")
                pid = pc._cursor.fetchone()["pid"]
                side.cursor().execute("SELECT pg_terminate_backend(%s)", [pid])
                side.cursor().execute("SELECT pg_sleep(0.05)")
                pc.set_cursor()
                pc.statement("SELECT 1 AS one")
        except Exception as exc:  # noqa: BLE001 — what the caller is told
            errors.append(type(exc).__name__)

    try:
        for _ in range(4):  # twice the pool
            worker = threading.Thread(target=_one_job)
            worker.start()
            worker.join()
    finally:
        side.close()

    assert errors == ["OperationalError"] * 4
    assert sem._value == 2
