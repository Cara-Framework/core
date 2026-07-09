from __future__ import annotations

try:
    from typing import Self
except ImportError:  # Python <3.11
    from typing import Self  # noqa: F401

"""Connection / transaction resolution for the Cara ORM.

**Thread-safety fix (2026-04-23):** ``_active_connections`` used to be a
class-level plain ``dict``, shared across every thread in the process.
When two threads both started transactions on the same connection name
(a hot path in the queue worker once we enabled ``--concurrency=N``),
the second thread's ``_active_connections[name] = conn_B`` would overwrite
the first thread's entry, so Thread A's commit/rollback ran against the
wrong connection and surfaced as ``ValueError: No active transaction
found for connection: 'app'``.

The registry is now keyed per execution-context via ``ContextVar``, which
matches how the rest of the codebase (``ExecutionContext``, ``JobContext``,
``TenantScope``) already handles thread-local state. A ``ContextVar`` is
both thread-safe and async-task-safe: each thread and each ``asyncio``
task gets its own view of the dict automatically, without leaking state
across boundaries.

Tests for concurrent begin/commit/rollback live alongside the rest of
the ORM suite; see ``tests/cara/eloquent/test_concurrent_transactions.py``.
"""

from contextlib import contextmanager, suppress
from contextvars import ContextVar

from cara.exceptions import (
    ConfigurationException,
    DriverNotFoundException,
    InvalidArgumentException,
)

# Per-context registry of "currently inside-transaction" connections.
# Keyed by connection name (``"app"``, ``"mysql"``, …) so a single
# context can hold transactions on multiple logical connections
# simultaneously without them clobbering each other. The default is
# ``None``; we lazily build the per-context dict on first write so
# contexts that never touch a transaction don't pay the allocation.
_ACTIVE_CONNECTIONS: ContextVar[dict[str, object] | None] = ContextVar(
    "cara.eloquent.connection_resolver.active_connections", default=None
)

# Per-context registry of after-commit callbacks (Laravel's
# ``DB::afterCommit``). Keyed by connection name and tied to the
# OUTERMOST open transaction on that connection: callbacks registered
# at any nesting level accumulate in one list and fire exactly once,
# right after the real driver-level commit of the outermost transaction
# succeeds. A rollback of the outermost transaction discards them so a
# deferred job/event never fires for work that was undone. Like
# ``_ACTIVE_CONNECTIONS`` this is lazily built so non-transactional
# contexts pay no allocation.
_AFTER_COMMIT_CALLBACKS: ContextVar[dict[str, list] | None] = ContextVar(
    "cara.eloquent.connection_resolver.after_commit_callbacks", default=None
)


def _get_registry() -> dict[str, object]:
    """Return the current context's active-transaction dict, creating it on demand.

    ContextVar initializes lazily: child threads / asyncio tasks inherit
    the parent's snapshot at spawn time, so a thread that starts a
    transaction won't see peer threads' transactions and vice-versa.
    """
    registry = _ACTIVE_CONNECTIONS.get()
    if registry is None:
        registry = {}
        _ACTIVE_CONNECTIONS.set(registry)
    return registry


def _get_after_commit_registry() -> dict[str, list]:
    """Return this context's after-commit callback map, creating it on demand.

    Mirrors :func:`_get_registry`'s lazy ContextVar pattern so the
    after-commit list is isolated per thread / asyncio task and never
    shared across concurrent jobs.
    """
    registry = _AFTER_COMMIT_CALLBACKS.get()
    if registry is None:
        registry = {}
        _AFTER_COMMIT_CALLBACKS.set(registry)
    return registry


def reset_registry() -> None:
    """Bind a FRESH, empty active-transaction registry to the current context.

    A new asyncio task (or thread) created via ``copy_context()`` inherits the
    parent's ContextVar bindings — and ``_ACTIVE_CONNECTIONS`` holds a *mutable
    dict by reference*, so the child task ends up sharing the SAME registry
    object as its parent and peer tasks. Under concurrent job execution
    (workers run up to ``WORKER_SCRAPE_CONCURRENCY`` jobs as peer tasks) two
    jobs would then share one registry: job A's transaction ``commit`` pops the
    connection out of the shared dict while job B is still mid-transaction on
    it, so B's own ``commit``/``rollback`` raises "No active transaction found
    for connection: app" and dead-letters.

    Calling this at a job-execution boundary rebinds a brand-new dict in the
    *current* context only (``ContextVar.set`` is context-local), giving each
    job a truly isolated transaction registry. Direct in-process ``execute()``
    sub-calls (which share the caller's context and must see its open
    transaction) are unaffected — they never cross this boundary.
    """
    _ACTIVE_CONNECTIONS.set({})
    # Rebind a fresh after-commit map too: a job inheriting a parent's
    # reference would otherwise fire (or leak) the parent's deferred
    # callbacks. Each job gets its own isolated after-commit list.
    _AFTER_COMMIT_CALLBACKS.set({})


class ConnectionResolver:
    """
    Single Responsibility: Manages connections and transactions ONLY.
    Open/Closed: Can be extended with new connection types.
    No configuration management — gets config from DatabaseManager.

    Transaction state is tracked per execution context (thread + asyncio
    task) via :data:`_ACTIVE_CONNECTIONS`. Prior to 2026-04-23 this used
    a class-level dict that leaked across threads; see module docstring.
    """

    def __init__(self, database_manager=None):
        """Initialize ConnectionResolver with DatabaseManager dependency"""
        from ..connections import ConnectionFactory

        self.connection_factory = ConnectionFactory()
        self.database_manager = database_manager
        self._register_default_connections()

    def set_database_manager(self, database_manager) -> Self:
        """Set database manager dependency"""
        self.database_manager = database_manager
        return self

    def _register_default_connections(self):
        """Register default connection types - Open/Closed principle"""
        from ..connections import (
            MSSQLConnection,
            MySQLConnection,
            PostgresConnection,
            SQLiteConnection,
        )

        connection_types = [
            SQLiteConnection,
            PostgresConnection,
            MySQLConnection,
            MSSQLConnection,
        ]
        for connection_type in connection_types:
            self.connection_factory.register(connection_type.name, connection_type)

    def _get_connection_info(self, connection_name):
        """Get connection info from DatabaseManager"""
        if not self.database_manager:
            raise ConfigurationException("DatabaseManager not set on ConnectionResolver")

        return self.database_manager.get_connection_info(connection_name)

    def _create_connection_instance(self, connection_name):
        """Return a connection instance for ``connection_name``.

        **Transaction-aware:** if this execution context has an active
        transaction on ``connection_name``, we reuse that same instance so
        queries run under the transaction's psycopg2 session instead of
        spawning a fresh pool connection that silently bypasses it.

        Before 2026-04-23 this always minted a new connection, which
        meant every query inside ``with db.transaction(): ...`` ran on
        autocommit against a sibling psycopg2 handle — rollbacks couldn't
        undo writes because the writes were never part of the transaction
        on the pinned connection.
        """
        # Short-circuit to the active transaction's connection if the
        # caller is currently inside ``with resolver.transaction(...)``.
        registry = _get_registry()
        active = registry.get(connection_name)
        if active is not None:
            return active

        connection_info = self._get_connection_info(connection_name)
        driver = connection_info.get("driver")

        if not driver:
            raise DriverNotFoundException(f"Driver not found for connection: {connection_name}")

        connection_class = self.connection_factory.make(driver)

        # Create connection instance with clean parameters
        clean_info = {
            "host": connection_info.get("host"),
            "database": connection_info.get("database"),
            "user": connection_info.get("user"),
            "port": connection_info.get("port"),
            "password": connection_info.get("password"),
            "prefix": connection_info.get("prefix", ""),
            "options": connection_info.get("options", {}),
            "full_details": connection_info.get("full_details", {}),
        }

        return connection_class(**clean_info).make_connection()

    # === Transaction Management - Single Responsibility ===

    def begin_transaction(self, connection_name):
        """Start (or NEST) a transaction — pinned to the current execution context.

        If a transaction is ALREADY active on this connection in the current
        context, reuse that SAME connection so ``.begin()`` opens a SAVEPOINT
        (incrementing ``transaction_level``) instead of opening a SECOND
        connection. Opening a fresh connection per nested ``db.transaction()``
        overwrote the registry entry, so the inner commit popped the
        connection and the OUTER commit raised "No active transaction found
        for connection: {name}". This bit every nested transaction — e.g.
        a job's refresh path on a retry, which wraps a
        persister that opens its own ``db.transaction()`` — and made
        a sync command fail with ``match_failed`` for any record
        that already existed.

        The connection instance is stored in the per-context registry so
        sibling threads / async tasks can't observe or commit it.
        """
        registry = _get_registry()
        existing = registry.get(connection_name)
        if existing is not None:
            # Nested transaction in the same context → SAVEPOINT on the
            # connection that ``transaction_level`` (and commit/rollback)
            # already track. Do NOT open a second connection.
            existing.begin()
            return existing
        connection = self._create_connection_instance(connection_name).begin()
        registry[connection_name] = connection
        return connection

    def commit(self, connection_name):
        """Commit the transaction opened in this context on ``connection_name``.

        After the commit, return the connection to the pool so it can be
        reused. Without this explicit close, every ``with db.transaction()``
        block would leak one psycopg2 connection — ``PostgresConnection.query``
        only auto-closes at ``transaction_level <= 0`` inside the query
        path, so a transaction block with no trailing SELECT leaves its
        connection dangling.
        """
        connection = self._get_active_connection(connection_name)
        connection.commit()
        # Only unpin + return the connection to the pool once the OUTERMOST
        # transaction has committed. ``db.transaction()`` nests (a job's
        # transaction calls a repo/sibling that opens its own) and the
        # connection handles that with SAVEPOINTs via ``transaction_level``.
        # Popping the registry + closing on every (incl. nested) commit left
        # the ENCLOSING transaction with no registry entry — which surfaced as
        # "No active transaction found for connection: app" at the outer commit.
        if getattr(connection, "transaction_level", 0) <= 0:
            self._remove_active_connection(connection_name)
            self._safe_close(connection)
            # The OUTERMOST transaction is now durably committed at the
            # driver level — fire deferred after-commit callbacks. Run
            # them AFTER unpin/close so a callback that opens its own
            # ``db.transaction()`` (or dispatches a job) starts from a
            # clean, transaction-free state (Laravel parity).
            self._run_after_commit_callbacks(connection_name)

    def after_commit(self, connection_name, callback):
        """Defer ``callback`` until the outermost transaction commits.

        Laravel's ``DB::afterCommit`` semantics:

        * Inside a transaction → append the callback to the outermost
          transaction's list; it fires once, right after the real commit
          succeeds, and never if the transaction rolls back.
        * No transaction open → run the callback IMMEDIATELY (there is
          nothing to wait for).
        """
        if _get_registry().get(connection_name) is None:
            # No open transaction on this connection in this context.
            callback()
            return
        _get_after_commit_registry().setdefault(connection_name, []).append(callback)

    def _run_after_commit_callbacks(self, connection_name):
        """Drain and invoke the after-commit callbacks for ``connection_name``.

        Pops the list before running so re-entrant registration (a
        callback that itself calls ``after_commit`` with no transaction
        open, which runs immediately) can't double-fire the drained set.
        """
        callbacks = _get_after_commit_registry().pop(connection_name, None)
        if not callbacks:
            return
        for callback in callbacks:
            callback()

    def rollback(self, connection_name):
        """Rollback the transaction opened in this context on ``connection_name``."""
        connection = self._get_active_connection(connection_name)
        connection.rollback()
        # Mirror ``commit``: a nested rollback only unwinds its SAVEPOINT, so
        # keep the connection pinned for the still-open enclosing transaction.
        if getattr(connection, "transaction_level", 0) <= 0:
            self._remove_active_connection(connection_name)
            self._safe_close(connection)
            # Outermost transaction rolled back → DISCARD every deferred
            # after-commit callback so nothing fires for undone work.
            _get_after_commit_registry().pop(connection_name, None)

    @staticmethod
    def _safe_close(connection) -> None:
        """Return the connection to the pool, swallowing any close errors.

        Transactional connections must be returned explicitly — the query
        path's ``close_connection`` guard only fires at
        ``transaction_level <= 0`` *inside* ``query()``, so a committed
        transaction with no trailing query would otherwise leak.
        """
        try:
            connection.open = 0
            connection.close_connection()
        except Exception as exc:
            # Log close failures so pool exhaustion leaks are detectable.
            # Silent pass here previously hid connection leaks that
            # eventually starved the pool.
            import logging

            logging.getLogger("cara.database.pool").warning(
                "Connection close failed (potential pool leak): %s", exc
            )

    @contextmanager
    def transaction(self, connection_name):
        """Context manager for transaction handling - Single responsibility.

        Catches ``BaseException`` (not just ``Exception``) so that
        ``KeyboardInterrupt``, ``SystemExit``, and
        ``asyncio.CancelledError`` still trigger rollback + connection
        return. The previous ``except Exception`` left the transaction
        pinned in the registry on Ctrl-C / cancelled coroutine, leaking
        a psycopg2 connection per interrupted call and silently
        committing on the next code path that touched the pool.
        """
        self.begin_transaction(connection_name)
        try:
            yield self
        except BaseException:
            try:
                self.rollback(connection_name)
            except (OSError, RuntimeError, AttributeError):
                # Best-effort: surface the original exception, not the
                # rollback failure (likely "no active transaction").
                pass
            raise

        try:
            self.commit(connection_name)
        except BaseException:
            with suppress(OSError, RuntimeError, AttributeError):
                self.rollback(connection_name)
            raise

    def _get_active_connection(self, connection_name):
        """Helper method - DRY principle"""
        registry = _get_registry()
        if connection_name not in registry:
            raise InvalidArgumentException(
                f"No active transaction found for connection: {connection_name}"
            )
        return registry[connection_name]

    def _remove_active_connection(self, connection_name):
        """Helper method - DRY principle"""
        _get_registry().pop(connection_name, None)

    # === Builder Factory Methods - Interface Segregation ===

    def get_schema_builder(self, connection_name, schema=None):
        """Factory method for schema builder - Interface segregation"""
        from ..schema import Schema

        # Create connection instance for schema operations
        connection = self._create_connection_instance(connection_name)
        return Schema(connection=connection, schema=schema)

    def get_query_builder(self, connection_name):
        """Factory method for query builder - Interface segregation

        Pass the connection *name* (string) instead of a live connection
        instance. QueryBuilder.new_connection() will open the real
        psycopg2 connection lazily — only when a query is actually
        executed — and PostgresConnection.query() returns it to the pool
        in its ``finally`` block.

        Previously this called ``_create_connection_instance()`` eagerly,
        but QueryBuilder.on() detected the instance (has ``name`` and
        ``make_connection`` attrs), discarded it, and stored the default
        connection name instead. The opened psycopg2 handle was then
        abandoned — never returned to the pool, never closed — leaking
        one server-side connection per get_query_builder() call until GC
        reclaimed the orphan. Under burst load this exhausted
        ``max_connections`` within seconds.
        """
        from ..query import QueryBuilder

        return QueryBuilder(connection=connection_name)

    def statement(self, query, bindings=(), connection_name=None):
        """Execute raw SQL statement - Delegation to appropriate builder"""
        return self.get_query_builder(connection_name).statement(query, bindings)
