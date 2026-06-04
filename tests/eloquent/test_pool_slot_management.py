"""``PostgresConnection`` — pool slot lifecycle pins.

The connection pool has two concurrency primitives that have to
stay in sync:

  * ``CONNECTION_POOL`` (module-level list) — warm psycopg2
    connections sitting idle, ready to be popped.
  * ``_pool_semaphore`` (module-level semaphore) — counts how many
    "active" connection slots are checked out. A caller MUST
    acquire one before opening / reusing a connection, and MUST
    release exactly one when done.

Tracked on the wrapper via ``self._pool_slot_acquired`` so
``close_connection`` knows whether it owns a release. Every
acquire-without-release path leaks a slot; under burst load the
semaphore drains to zero and the whole API hangs on
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
import threading
import types
from unittest.mock import MagicMock

import pytest


PGModule = importlib.import_module("cara.eloquent.connections.PostgresConnection")
PostgresConnection = PGModule.PostgresConnection
DatabaseUnavailableException = PGModule.DatabaseUnavailableException


def _install_fake_psycopg2(monkeypatch, connect_factory):
    """Insert a minimal fake psycopg2 module so ``create_connection``
    can run without the real driver attached to a live Postgres."""
    fake = types.ModuleType("psycopg2")
    fake.connect = connect_factory
    fake.OperationalError = type("OperationalError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "psycopg2", fake)
    return fake


def _fresh_pool(
    monkeypatch, *, size: int, warm: list | None = None
) -> threading.Semaphore:
    """Reset the module-level pool state so each test is hermetic.

    Returns the semaphore so tests can assert against the slot count.
    """
    sem = threading.Semaphore(size)
    monkeypatch.setattr(PGModule, "_pool_initialized", True)
    monkeypatch.setattr(PGModule, "_pool_semaphore", sem)
    monkeypatch.setattr(PGModule, "CONNECTION_POOL", list(warm or []))
    return sem


def _make_pc(**overrides):
    """Build a PostgresConnection wired for pool-enabled mode with a
    small pool by default. Tests override individual full_details."""
    full_details = {
        "connection_pooling_enabled": True,
        "connection_pooling_max_size": 4,
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
        sem = _fresh_pool(monkeypatch, size=2)
        # Drain both slots so the next acquire times out.
        sem.acquire()
        sem.acquire()

        pc = _make_pc()
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
        # Slot state must not have been mutated by a failed acquire —
        # the wrapper had nothing to release.
        assert (
            pc._pool_slot_acquired is False
            if hasattr(pc, "_pool_slot_acquired")
            else True
        )


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
        sem = _fresh_pool(monkeypatch, size=3)

        pc = _make_pc()
        # Simulate the post-acquire / pre-assign hole: flag set,
        # _connection still None.
        pc._connection = None
        pc._pool_slot_acquired = True
        sem.acquire()  # mimic the acquire that set the flag

        # Two slots should be in use right now (the manual acquire
        # above + the one we held implicitly when setting the flag).
        # Pre-fix close_connection here would release ONE; the wrapper
        # then gets garbage-collected with one slot still held.
        before = sem._value
        pc.close_connection()
        after = sem._value
        assert after == before + 1, (
            f"close_connection must release exactly one slot when the "
            f"wrapper held one; semaphore went {before} → {after}"
        )
        # Flag clears so a future close_connection on the same
        # wrapper doesn't double-release.
        assert pc._pool_slot_acquired is False


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
        sem = _fresh_pool(monkeypatch, size=4)

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

        _install_fake_psycopg2(monkeypatch, connect_factory=_factory)

        pc = _make_pc()
        pc.make_connection()
        slots_before = 4 - sem._value
        assert slots_before == 1, (
            f"first make_connection should hold one slot; held {slots_before}"
        )

        # Simulate the network drop psycopg2 caught.
        first_conn.closed = True

        # The bug: this acquires a second slot without releasing the
        # first. Post-fix the wrapper releases the orphaned slot
        # before re-acquiring, so slot count stays at 1.
        pc.make_connection()
        slots_after = 4 - sem._value
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
        sem = _fresh_pool(monkeypatch, size=3)
        before = sem._value

        _install_fake_psycopg2(monkeypatch, _mock_pg_connection)

        # ``foreign_keys=True`` forces ``enable_disable_foreign_keys``
        # to issue ``self._connection.execute(...)``; pin the connection
        # to raise on that call.
        pc = _make_pc(full_details={"foreign_keys": True})

        def _raising_execute(_sql):
            raise RuntimeError("server-side socket closed mid-setup")

        # Monkeypatch the connection that the fake psycopg2 will return.
        # The fake_psycopg2 calls _mock_pg_connection() each time; patch
        # that to inject a raising execute on the returned mock.
        original = _mock_pg_connection

        def _broken_mock_pg_connection(**_kw):
            conn = original()
            conn.execute = _raising_execute  # type: ignore[method-assign]
            return conn

        _install_fake_psycopg2(monkeypatch, _broken_mock_pg_connection)

        with pytest.raises(RuntimeError, match="socket closed"):
            pc.make_connection()

        after = sem._value
        assert after == before, (
            f"make_connection setup failure leaked a slot: "
            f"semaphore {before} → {after}. The fix routes through "
            f"close_connection on the exception path so the slot is "
            f"released before the exception bubbles."
        )
        # ``_pool_slot_acquired`` must be cleared so a subsequent
        # close_connection on the (abandoned) wrapper doesn't
        # double-release.
        assert getattr(pc, "_pool_slot_acquired", False) is False, (
            "_pool_slot_acquired flag must be cleared by the cleanup "
            "path so a stray close_connection can't double-release"
        )

    def test_autocommit_assign_failure_releases_slot(self, monkeypatch):
        """Same shape, different surface — psycopg2's
        ``connection.autocommit = True`` setter can raise
        ``OperationalError`` if the underlying socket died between
        ``create_connection`` and this assignment."""
        sem = _fresh_pool(monkeypatch, size=3)
        before = sem._value

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

        _install_fake_psycopg2(monkeypatch, _exploding_mock_pg_connection)
        pc = _make_pc()

        with pytest.raises(RuntimeError, match="TCP RST"):
            pc.make_connection()

        after = sem._value
        assert after == before, (
            f"autocommit-assign failure leaked a slot: {before} → {after}"
        )

    def test_repeated_setup_failures_do_not_drain_pool(self, monkeypatch):
        """Belt-and-braces: 10 consecutive setup failures must leave
        the pool with EVERY slot still available. Pre-fix this would
        drain a 4-slot pool in 4 iterations and 503 every caller for
        the rest of the process lifetime."""
        sem = _fresh_pool(monkeypatch, size=4)

        def _broken_mock_pg_connection(**_kw):
            conn = _mock_pg_connection()

            def _raising_execute(_sql):
                raise RuntimeError("repeat failure")

            conn.execute = _raising_execute  # type: ignore[method-assign]
            return conn

        _install_fake_psycopg2(monkeypatch, _broken_mock_pg_connection)

        for _ in range(10):
            pc = _make_pc(full_details={"foreign_keys": True})
            with pytest.raises(RuntimeError):
                pc.make_connection()

        assert sem._value == 4, (
            f"10 setup failures drained the pool: {sem._value}/4 slots "
            f"available. The fix must release on every failed setup."
        )


class TestRepeatReconnectsDoNotAccumulate:
    """Belt-and-braces: 10 consecutive reconnects on the same
    wrapper must hold EXACTLY 1 slot at the end. Pre-fix each
    reconnect leaked one slot; 10 reconnects on a small pool
    would 503 every subsequent caller."""

    def test_ten_reconnects(self, monkeypatch):
        sem = _fresh_pool(monkeypatch, size=20)

        def _factory(**_kw):
            return _mock_pg_connection()

        _install_fake_psycopg2(monkeypatch, connect_factory=_factory)

        pc = _make_pc()
        for _ in range(10):
            pc.make_connection()
            # Mark this connection as closed so the next iteration
            # hits the reconnect path.
            pc._connection.closed = True

        held = 20 - sem._value
        assert held == 1, (
            f"10 reconnects held {held} slots; expected 1 — slots are "
            f"leaking on the closed-connection re-acquire path"
        )
