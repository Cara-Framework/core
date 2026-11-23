"""``PostgresConnection`` — pool slot lifecycle pins.

Each configured connection has its own pool (``PostgresPool``): the idle
psycopg2 sessions sitting ready to be popped, and the permits that count
how many sessions are checked out. A caller MUST take a permit before
opening / reusing a session and MUST hand exactly one back when done.

The wrapper remembers the pool that issued its permit
(``self._permit_pool``) so ``close_connection`` knows whether it owns a
release, and where it goes. Every acquire-without-release path leaks a
permit; under burst load the pool drains to zero and the whole API hangs on
``acquire()`` (or 503s after the 30s timeout).

These tests pin three lifecycle invariants:

  1. **Pool exhaustion** surfaces as ``DatabaseUnavailableException``
     with ``retry_after`` set — distinct from a query bug. The
     exception handler then returns 503 (capacity) instead of 500
     (application fault) so the load balancer / client retries.

  2. **Slot leak on connection re-create.** When the query path
     hits ``if self._connection.closed: self.make_connection()``
     (psycopg2 noticed the server-side close between queries), the
     wrapper RE-acquires a slot. Pre-fix the old slot was never
     released — every flaky network event silently drained one
     slot from the pool. Under sustained instability the pool
     exhausts within minutes.

  3. **Slot release on close even when the connection failed.**
     ``close_connection`` released the slot only inside the
     pooling-enabled branch's success arm; the early-return arm
     (``self._connection is None``) already releases, but the
     ``connection.closed=True`` case (push-to-pool skipped) needs
     to release too. Pinned to prevent a future tightening of the
     control flow from re-introducing a leak.
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock

import pytest

from cara.eloquent.connections import PostgresPool

from ._fixtures import install_fake_psycopg2, pool_of

PGModule = importlib.import_module("cara.eloquent.connections.PostgresConnection")
PostgresConnection = PGModule.PostgresConnection
DatabaseUnavailableException = PGModule.DatabaseUnavailableException


@pytest.fixture(autouse=True)
def _no_pools():
    """Every test starts and ends with no pool registered."""
    PostgresPool.reset_all(close=False)
    yield
    PostgresPool.reset_all(close=False)


def _make_pc(size: int = 4, **overrides):
    """Build a PostgresConnection wired for pool-enabled mode with a
    small pool by default. Tests override individual full_details."""
    full_details = {
        "connection_pooling_enabled": True,
        "connection_pooling_max_size": size,
        **overrides.pop("full_details", {}),
    }
    return PostgresConnection(
        host="x",
        database="x",
        user="x",
        port=5432,
        password="x",
        full_details=full_details,
    )


def _in_use(pc) -> int:
    """Permits of ``pc``'s pool currently checked out."""
    return pool_of(pc).stats()["in_use"]


def _mock_pg_connection() -> MagicMock:
    """A psycopg2-shaped connection that survives the SELECT 1 probe."""
    conn = MagicMock(name="psycopg2_conn")
    conn.closed = False
    conn.info.transaction_status = 0
    cur = MagicMock(name="cursor")
    cur.execute.return_value = None
    conn.cursor.return_value = cur
    return conn


# ── Pool exhaustion → DatabaseUnavailableException ─────────────


class TestPoolExhaustionRaises503Shape:
    """When every slot is checked out and the 30s acquire timeout
    expires, the API must surface a 503-flavoured exception (with
    ``retry_after``) — NOT a generic 500. The exception handler
    routes the two outcomes differently and the load balancer /
    HTTP client only retries on the 503."""

    def test_exhausted_pool_raises_database_unavailable(self, monkeypatch):
        pc = _make_pc(size=2)
        pool = pool_of(pc)
        # Drain both permits so the next acquire times out.
        assert pool.acquire(0) and pool.acquire(0)
        # Shorten the acquire timeout so the test finishes fast —
        # production default is 30s and we don't need to prove the
        # exact value here, just that the timeout fires.
        monkeypatch.setattr(pc, "_POOL_ACQUIRE_TIMEOUT", 0.05)

        with pytest.raises(DatabaseUnavailableException) as excinfo:
            pc.create_connection()
        # The exception must carry ``retry_after`` so the framework
        # adds a Retry-After header — without it the client treats
        # the failure as permanent and never re-attempts.
        assert getattr(excinfo.value, "retry_after", None) == 1, (
            "DatabaseUnavailableException must set retry_after=1 so "
            "the exception handler produces a 503 with a retry hint"
        )
        # Permit state must not have been mutated by a failed acquire —
        # the wrapper had nothing to release.
        assert pc._permit_pool is None
        assert pool.stats()["in_use"] == 2


# ── Slot leak on close_connection with _connection=None ─────────


class TestCloseConnectionReleasesOrphanSlot:
    """If a caller acquires a slot, fails to assign a connection
    (rare — happens when ``_connect_with_retry`` fails AFTER the
    slot was claimed but BEFORE assignment to ``self._connection``),
    then calls ``close_connection``, the slot MUST still be
    released. Pre-fix the early-return arm released only when the
    flag was set AND the connection was assigned; an orphaned
    slot held until process exit."""

    def test_release_when_connection_never_assigned(self, monkeypatch):
        pc = _make_pc(size=3)
        pool = pool_of(pc)
        # Simulate the post-acquire / pre-assign hole: permit taken,
        # _connection still None.
        assert pool.acquire(0)
        pc._connection = None
        pc._permit_pool = pool

        pc.close_connection()
        assert pool.stats()["in_use"] == 0, (
            "close_connection must release the permit the wrapper held "
            "even when no session was ever assigned"
        )
        # The wrapper forgets its pool so a second close_connection on
        # the same wrapper doesn't double-release.
        assert pc._permit_pool is None
        pc.close_connection()
        assert pool.stats()["in_use"] == 0


class TestConnectFailureReleasesSlot:
    """DBAPI connection failures must not consume pool permits.

    psycopg2's ``OperationalError`` is not an ``OSError``. A narrow cleanup
    handler therefore leaked one permit for every refused/failed connection
    until the pool exhausted and unrelated callers blocked for 30 seconds.
    """

    def test_operational_error_on_fresh_connect_releases_slot(self, monkeypatch):
        fake = install_fake_psycopg2(monkeypatch, connect_factory=lambda **_kw: None)

        def _refused(**_kw):
            raise fake.OperationalError("connection refused")

        fake.connect = _refused
        pc = _make_pc(size=3)

        with pytest.raises(fake.OperationalError, match="connection refused"):
            pc.create_connection()

        assert _in_use(pc) == 0, (
            "a failed psycopg2.connect drained a pool permit; repeated DB "
            "outages would exhaust the pool"
        )
        assert pc._permit_pool is None

    def test_probe_and_replacement_errors_release_slot(self, monkeypatch):
        stale = _mock_pg_connection()
        fake = install_fake_psycopg2(monkeypatch, connect_factory=lambda **_kw: None)
        pc = _make_pc(size=3)
        pool_of(pc).warm(lambda: stale, 1)  # pre-warmed: probed on checkout
        stale.cursor.return_value.execute.side_effect = fake.OperationalError(
            "idle connection dropped"
        )

        def _replacement_refused(**_kw):
            raise fake.OperationalError("replacement refused")

        fake.connect = _replacement_refused

        with pytest.raises(fake.OperationalError, match="replacement refused"):
            pc.create_connection()

        stale.cursor.return_value.close.assert_called_once_with()
        stale.close.assert_called_once_with()
        assert _in_use(pc) == 0
        assert pc._permit_pool is None

    def test_operational_error_while_returning_connection_releases_slot(
        self, monkeypatch
    ):
        fake = install_fake_psycopg2(monkeypatch, _mock_pg_connection)
        conn = _mock_pg_connection()
        conn.info.transaction_status = 1
        conn.rollback.side_effect = fake.OperationalError("connection reset")

        pc = _make_pc(size=2)
        pool = pool_of(pc)
        assert pool.acquire(0)
        pc._connection = conn
        pc._permit_pool = pool
        pc.close_connection()

        assert pool.stats() | {"connection": None} == {
            "connection": None,
            "in_use": 0,
            "idle": 0,  # a session that cannot be reset is not parked
            "max": 2,
        }
        assert pc._permit_pool is None
        assert pc._connection is None
        conn.close.assert_called_once()


# ── Slot leak on mid-life reconnect ─────────────────────────────


class TestSlotLeakOnMidLifeReconnect:
    """``query()`` checks ``if not self._connection or self._connection.closed:
    self.make_connection()``. The ``.closed=True`` branch fires when
    psycopg2 noticed the server-side close between two queries on
    the same wrapper. Pre-fix this re-acquired a fresh pool slot
    WITHOUT releasing the old one — every flaky network event
    silently drained one slot.

    The fix: ``make_connection`` / ``create_connection`` MUST
    release any previously-held slot before acquiring a new one.
    Net invariant: at most ONE slot per wrapper at any time.
    """

    def test_reconnect_after_closed_releases_old_slot(self, monkeypatch):
        # Stage 1: first make_connection mints a connection and
        # acquires slot 1. Stage 2: connection is marked closed.
        # Stage 3: caller invokes make_connection again (mimicking
        # the query() branch). Net slots held MUST be 1, not 2.
        first_conn = _mock_pg_connection()
        second_conn = _mock_pg_connection()
        connects: list = []

        def _factory(**_kw):
            conn = first_conn if not connects else second_conn
            connects.append(conn)
            return conn

        install_fake_psycopg2(monkeypatch, connect_factory=_factory)

        pc = _make_pc()
        pc.make_connection()
        slots_before = _in_use(pc)
        assert slots_before == 1, (
            f"first make_connection should hold one slot; held {slots_before}"
        )

        # Simulate the network drop psycopg2 caught.
        first_conn.closed = True

        # The bug: this acquires a second slot without releasing the
        # first. Post-fix the wrapper releases the orphaned slot
        # before re-acquiring, so slot count stays at 1.
        pc.make_connection()
        slots_after = _in_use(pc)
        assert slots_after == 1, (
            f"mid-life reconnect leaked a slot: held {slots_before} "
            f"before, {slots_after} after — invariant says ≤1 slot "
            f"per wrapper at any time"
        )


class TestMakeConnectionSetupFailureReleasesSlot:
    """``make_connection`` runs two post-acquire operations before it
    can be considered fully constructed:

      * ``self._connection.autocommit = True`` — psycopg2 property
        assign; on a TCP-RST'd connection raises ``OperationalError``.
      * ``self.enable_disable_foreign_keys()`` — issues an actual
        SQL roundtrip; the network/server can drop the connection
        between ``create_connection`` returning and this statement
        executing.

    Pre-fix either failure mode bubbled straight out of
    ``make_connection``, but ``create_connection`` had already
    acquired a pool slot and assigned ``self._connection``. The
    caller saw the exception, abandoned the wrapper, and the slot
    stayed checked out until process exit. Under sustained
    instability (Postgres restart, network flap) every fire
    drained one slot from the global semaphore; once exhausted,
    every subsequent caller hung for the 30 s acquire timeout
    and then 503'd.

    These tests pin that ``make_connection`` releases the slot via
    ``close_connection`` before re-raising. Net invariant: a
    failed setup leaves the semaphore unchanged from its
    pre-call value.
    """

    def test_enable_foreign_keys_failure_releases_slot(self, monkeypatch):
        install_fake_psycopg2(monkeypatch, _mock_pg_connection)

        # ``foreign_keys=True`` forces ``enable_disable_foreign_keys`` to issue
        # ``self._connection.cursor().execute(...)`` — a DBAPI connection has no
        # ``.execute()``, so the setup goes through a CURSOR. Pin the cursor to
        # raise on that call.
        pc = _make_pc(full_details={"foreign_keys": True})

        # The fake psycopg2 calls _mock_pg_connection() each time; patch it so
        # the returned connection's cursor raises on execute.
        original = _mock_pg_connection

        def _broken_mock_pg_connection(**_kw):
            conn = original()
            conn.cursor.return_value.execute.side_effect = RuntimeError(
                "server-side socket closed mid-setup"
            )
            return conn

        install_fake_psycopg2(monkeypatch, _broken_mock_pg_connection)

        with pytest.raises(RuntimeError, match="socket closed"):
            pc.make_connection()

        assert _in_use(pc) == 0, (
            "make_connection setup failure leaked a permit. The fix routes "
            "through close_connection on the exception path so the permit "
            "is released before the exception bubbles."
        )
        # The permit's pool must be forgotten so a subsequent
        # close_connection on the (abandoned) wrapper doesn't
        # double-release.
        assert pc._permit_pool is None, (
            "the cleanup path must forget the permit's pool so a stray "
            "close_connection can't double-release"
        )

    def test_autocommit_assign_failure_releases_slot(self, monkeypatch):
        """Same shape, different surface — psycopg2's
        ``connection.autocommit = True`` setter can raise
        ``OperationalError`` if the underlying socket died between
        ``create_connection`` and this assignment."""
        # Use a connection whose autocommit setter raises. MagicMock's
        # default behaviour accepts any property assign; override via
        # PropertyMock so the setter side-effect fires.
        from unittest.mock import PropertyMock

        def _exploding_mock_pg_connection(**_kw):
            conn = _mock_pg_connection()
            type(conn).autocommit = PropertyMock(
                side_effect=RuntimeError("TCP RST before autocommit could land")
            )
            return conn

        install_fake_psycopg2(monkeypatch, _exploding_mock_pg_connection)
        pc = _make_pc()

        with pytest.raises(RuntimeError, match="TCP RST"):
            pc.make_connection()

        assert _in_use(pc) == 0, "autocommit-assign failure leaked a permit"

    def test_operational_error_during_setup_releases_slot(self, monkeypatch):
        fake = install_fake_psycopg2(monkeypatch, connect_factory=lambda **_kw: None)

        def _connection_with_failed_setup(**_kw):
            conn = _mock_pg_connection()
            conn.cursor.return_value.execute.side_effect = fake.OperationalError(
                "database restarted during setup"
            )
            return conn

        fake.connect = _connection_with_failed_setup
        pc = _make_pc(full_details={"foreign_keys": True})

        with pytest.raises(fake.OperationalError, match="restarted during setup"):
            pc.make_connection()

        assert _in_use(pc) == 0
        assert pc._permit_pool is None

    def test_repeated_setup_failures_do_not_drain_pool(self, monkeypatch):
        """Belt-and-braces: 10 consecutive setup failures must leave
        the pool with EVERY slot still available. Pre-fix this would
        drain a 4-slot pool in 4 iterations and 503 every caller for
        the rest of the process lifetime."""

        def _broken_mock_pg_connection(**_kw):
            conn = _mock_pg_connection()
            conn.cursor.return_value.execute.side_effect = RuntimeError("repeat failure")
            return conn

        install_fake_psycopg2(monkeypatch, _broken_mock_pg_connection)

        for _ in range(10):
            pc = _make_pc(full_details={"foreign_keys": True})
            with pytest.raises(RuntimeError):
                pc.make_connection()

        assert _in_use(pc) == 0, (
            f"10 setup failures drained the pool: {_in_use(pc)}/4 permits "
            f"out. The fix must release on every failed setup."
        )


class TestRepeatReconnectsDoNotAccumulate:
    """Belt-and-braces: 10 consecutive reconnects on the same
    wrapper must hold EXACTLY 1 slot at the end. Pre-fix each
    reconnect leaked one slot; 10 reconnects on a small pool
    would 503 every subsequent caller."""

    def test_ten_reconnects(self, monkeypatch):
        def _factory(**_kw):
            return _mock_pg_connection()

        install_fake_psycopg2(monkeypatch, connect_factory=_factory)

        pc = _make_pc(size=20)
        for _ in range(10):
            pc.make_connection()
            # Mark this connection as closed so the next iteration
            # hits the reconnect path.
            pc._connection.closed = True

        held = _in_use(pc)
        assert held == 1, (
            f"10 reconnects held {held} slots; expected 1 — slots are "
            f"leaking on the closed-connection re-acquire path"
        )


# ── A builder's connection opens where it is returned ───────────


def _pooled_manager(monkeypatch, *, size: int):
    """A real DatabaseManager on a fake driver and a fresh ``size``-permit pool."""
    from cara.eloquent import DatabaseManager

    fake = install_fake_psycopg2(
        monkeypatch, connect_factory=lambda **kw: _mock_pg_connection()
    )
    extras = type(sys)("psycopg2.extras")
    extras.RealDictCursor = object
    fake.extras = extras
    monkeypatch.setitem(sys.modules, "psycopg2.extras", extras)
    manager = DatabaseManager(
        "app",
        {
            "app": {
                "driver": "postgres",
                "host": "x",
                "database": "x",
                "user": "x",
                "port": 5432,
                "password": "x",
                "connection_pooling_enabled": True,
                "connection_pooling_max_size": size,
            }
        },
    )
    probe = manager.create_connection_instance("app", connect=False)
    return manager, pool_of(probe)


class TestBuilderConnectionsHoldNoPermitWhileCompiling:
    """The builder used to OPEN its connection before compiling the SQL.

    ``self.new_connection().query(self.to_qmark(), ...)`` evaluates
    ``new_connection()`` first, and that took a pool permit; ``to_qmark()``
    then ran the scopes and the grammar. Anything raising there skipped
    ``query()``, whose ``finally`` is the only release, so the permit leaked
    for the life of the process. Once every permit was gone each request
    waited out the 30s acquire timeout and answered 503 — the API's
    integration lane stalled exactly that way (2026-09-15).
    """

    def test_a_compile_failure_leaves_every_permit_in_the_pool(self, monkeypatch):
        from cara.eloquent.query import QueryBuilder

        manager, pool = _pooled_manager(monkeypatch, size=2)
        builder = QueryBuilder(connection="app", database_manager=manager).table("users")

        def refuse():
            raise RuntimeError("scope refused to compile")

        monkeypatch.setattr(builder, "to_qmark", refuse)
        for _ in range(3):
            with pytest.raises(RuntimeError, match="scope refused"):
                builder.get()

        assert pool.stats()["in_use"] == 0, "a failed compile kept a pool permit"

    def test_an_unopened_connection_takes_its_permit_inside_query(self, monkeypatch):
        manager, pool = _pooled_manager(monkeypatch, size=2)

        connection = manager.create_connection_instance("app", connect=False)
        assert connection._connection is None
        assert pool.stats()["in_use"] == 0

        connection.query("SELECT 1")
        assert pool.stats()["in_use"] == 0, "query() must return the permit it took"
        assert pool.stats()["idle"] == 1, "the session was parked in its own pool"

    def test_begin_opens_an_unopened_connection_and_close_returns_it(self, monkeypatch):
        manager, pool = _pooled_manager(monkeypatch, size=2)

        connection = manager.create_connection_instance("app", connect=False)
        connection.begin()
        assert connection.transaction_level == 1
        assert pool.stats()["in_use"] == 1

        connection.rollback()
        connection.close_connection()
        assert pool.stats()["in_use"] == 0
