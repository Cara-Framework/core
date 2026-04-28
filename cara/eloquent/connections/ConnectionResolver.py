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

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Dict


# Per-context registry of "currently inside-transaction" connections.
# Keyed by connection name (``"app"``, ``"mysql"``, …) so a single
# context can hold transactions on multiple logical connections
# simultaneously without them clobbering each other. The default is
# ``None``; we lazily build the per-context dict on first write so
# contexts that never touch a transaction don't pay the allocation.
_ACTIVE_CONNECTIONS: "ContextVar[Dict[str, object] | None]" = ContextVar(
    "cara.eloquent.connection_resolver.active_connections", default=None
)


def _get_registry() -> Dict[str, object]:
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

    def set_database_manager(self, database_manager):
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
            raise RuntimeError("DatabaseManager not set on ConnectionResolver")

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
            raise ValueError(f"Driver not found for connection: {connection_name}")

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
        """Start transaction — pinned to the current execution context.

        The resulting connection instance is stored in the per-context
        registry so sibling threads can't observe or commit it.
        """
        connection = self._create_connection_instance(connection_name).begin()
        _get_registry()[connection_name] = connection
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
        self._remove_active_connection(connection_name)
        try:
            connection.commit()
        finally:
            self._safe_close(connection)

    def rollback(self, connection_name):
        """Rollback the transaction opened in this context on ``connection_name``."""
        connection = self._get_active_connection(connection_name)
        self._remove_active_connection(connection_name)
        try:
            connection.rollback()
        finally:
            self._safe_close(connection)

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
            except Exception:
                # Best-effort: surface the original exception, not the
                # rollback failure (likely "no active transaction").
                pass
            raise

        try:
            self.commit(connection_name)
        except BaseException:
            try:
                self.rollback(connection_name)
            except Exception:
                pass
            raise

    # ── Backwards-compat alias for callers that still read the old class dict ──
    # Prior code (and a few tests) reach into ``ConnectionResolver._active_connections``
    # directly for diagnostics. Expose a read-only snapshot of the current
    # context's registry so those callers keep working without being able to
    # mutate cross-context state.
    @property
    def _active_connections(self) -> Dict[str, object]:
        return dict(_get_registry())

    def _get_active_connection(self, connection_name):
        """Helper method - DRY principle"""
        registry = _get_registry()
        if connection_name not in registry:
            raise ValueError(
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
        """Factory method for query builder - Interface segregation"""
        from ..query import QueryBuilder

        # Create connection for query operations
        connection = self._create_connection_instance(connection_name)
        return QueryBuilder(connection=connection)

    def statement(self, query, bindings=(), connection_name=None):
        """Execute raw SQL statement - Delegation to appropriate builder"""
        return self.get_query_builder(connection_name).statement(query, bindings)
