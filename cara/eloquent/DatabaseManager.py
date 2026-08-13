from __future__ import annotations

import logging
from collections.abc import Mapping
from contextlib import contextmanager
from copy import deepcopy
from typing import Self

from cara.exceptions import ConfigurationException, ConnectionNotRegisteredException

from .connections import ConnectionResolver
from .connections.ConnectionResolver import _get_registry

_logger = logging.getLogger("cara.database")


def _normalize_database_config(
    default_connection,
    connection_details,
) -> tuple[str, dict[str, dict]]:
    """Validate and snapshot the complete application database config."""
    if hasattr(connection_details, "to_dict"):
        connection_details = connection_details.to_dict()
    if not isinstance(connection_details, Mapping):
        raise ConfigurationException("database.drivers must be a mapping")

    normalized_connections: dict[str, dict] = {}
    for connection_name, details in connection_details.items():
        if not isinstance(connection_name, str) or not connection_name.strip():
            raise ConfigurationException(
                "database.drivers keys must be non-empty connection names"
            )
        if not isinstance(details, Mapping):
            raise ConfigurationException(
                f"database.drivers.{connection_name} must be a mapping"
            )
        normalized_details = deepcopy(dict(details))
        driver = normalized_details.get("driver")
        if not isinstance(driver, str) or not driver.strip():
            raise ConfigurationException(
                f"database.drivers.{connection_name}.driver must be configured"
            )
        normalized_details["driver"] = driver.strip()
        normalized_connections[connection_name] = normalized_details

    if not isinstance(default_connection, str) or not default_connection.strip():
        raise ConfigurationException("database.default must name a configured connection")
    default_connection = default_connection.strip()
    if default_connection not in normalized_connections:
        raise ConfigurationException(
            f"database.default '{default_connection}' is not present in database.drivers"
        )
    return default_connection, normalized_connections


class DatabaseManager:
    """
    Database Manager - Central database management with integrated configuration
    Single Responsibility: Database operations, connection management and configuration
    Open/Closed: Extensible through ConnectionResolver
    """

    def __init__(self, default_connection, connection_details):
        """Build a fully configured, application-owned manager."""
        default_connection, connections = _normalize_database_config(
            default_connection,
            connection_details,
        )
        self._resolver = None
        self._default_connection = default_connection
        self._connections = connections
        self._morph_map = {}
        self._ensure_resolver()

    def _ensure_resolver(self):
        """Lazy initialization of resolver to avoid circular dependency"""
        if self._resolver is None:
            self._resolver = ConnectionResolver(database_manager=self)
        return self._resolver

    def _resolve_connection_name(self, name=None):
        """Resolves connection name - simple logic"""
        if name is None:
            return self._default_connection
        if name == "default":
            return self._default_connection
        return name

    def _get_connection_config(self, connection):
        """Get connection config"""
        connection_name = self._resolve_connection_name(connection)
        if connection_name not in self._connections:
            raise ConnectionNotRegisteredException(
                f"Connection '{connection_name}' not found"
            )
        return self._connections[connection_name]

    def connection(self, connection=None):
        """Returns QueryBuilder for specific connection"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.get_query_builder(connection_name)

    def begin_transaction(self, connection=None):
        """Starts transaction"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.begin_transaction(connection_name)

    def commit(self, connection=None):
        """Commits transaction"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.commit(connection_name)

    def rollback(self, connection=None):
        """Rollbacks transaction"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.rollback(connection_name)

    def after_commit(self, callback, connection=None) -> None:
        """Register a callback to run after the current transaction commits.

        Laravel parity (``DB::afterCommit``): if a transaction is open on
        the connection, ``callback`` is deferred until that transaction's
        outermost level actually commits — and is discarded if it rolls
        back. If NO transaction is open, ``callback`` runs immediately.

        This gives jobs/events a real after-commit seam instead of the
        per-call-site hand-rolling that used to wrap dispatches in manual
        ``if not in_transaction`` checks.
        """
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.after_commit(connection_name, callback)

    def after_rollback(self, callback, connection=None) -> bool:
        """Register a callback for rollback of the current transaction level.

        Returns ``False`` when no transaction is active; unlike after-commit,
        rollback callbacks must never run eagerly.
        """
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.after_rollback(connection_name, callback)

    def transaction_level(self, connection=None) -> int:
        """Return the open transaction depth on the context-pinned connection.

        ``0`` means this execution context holds no transaction. Framework
        execution boundaries read the depth BEFORE they run a unit of work so
        they can afterwards unwind exactly the levels they opened themselves
        — see :meth:`commit_transactions_above`.
        """

        connection_name = self._resolve_connection_name(connection)
        conn = _get_registry().get(connection_name)
        if conn is None:
            return 0
        return int(getattr(conn, "transaction_level", 0) or 0)

    def commit_transactions_above(self, baseline: int, connection=None) -> None:
        """Commit only the transaction levels opened above ``baseline``.

        :meth:`commit_open_transactions` finalizes EVERYTHING pinned in the
        context registry. That is right for a pipeline stage that owns the
        connection and wrong for a boundary that runs inside a transaction it
        did not open: sync ``Bus`` dispatch executes inline in the caller's
        asyncio task and therefore shares the caller's ContextVar-pinned
        registry, so unwinding every level committed the caller's ambient
        business transaction early — a later failure in the same use case
        could no longer undo the write — and left the caller's own
        ``with DB.transaction():`` exit raising ``No active transaction found
        for connection: app``. DOCTRINE §8 keeps the business transaction with
        the use-case service; a framework boundary owns only what it opened.

        Levels at or below ``baseline`` are left untouched, including the
        after-commit / after-rollback callbacks registered there: the resolver
        keys those by level and only drains them at the outermost commit.
        """
        self._unwind_transactions(baseline, connection, commit=True)

    def rollback_transactions_above(self, baseline: int, connection=None) -> None:
        """Roll back only the transaction levels opened above ``baseline``.

        Failure-side counterpart of :meth:`commit_transactions_above`, used
        before a framework boundary propagates a failed or cancelled unit of
        work so its own levels cannot leak into the next one.
        """
        self._unwind_transactions(baseline, connection, commit=False)

    def _unwind_transactions(self, baseline: int, connection, *, commit: bool) -> None:
        """Drive the pinned connection down to ``baseline`` through the resolver.

        The resolver already unpins the registry entry and returns the
        connection to the pool when the OUTERMOST level closes, so this loop
        must never pop or close on its own — doing that unconditionally is
        precisely what let a boundary release a connection it did not open.
        """

        connection_name = self._resolve_connection_name(connection)
        registry = _get_registry()
        resolver = self._ensure_resolver()
        floor = max(int(baseline), 0)
        finalize = resolver.commit if commit else resolver.rollback

        # Bounded loop: a connection whose ``transaction_level`` never
        # decrements must not spin the boundary forever.
        for _ in range(64):
            conn = registry.get(connection_name)
            if conn is None:
                break
            if int(getattr(conn, "transaction_level", 0) or 0) <= floor:
                break
            finalize(connection_name)

    def _release_pinned_connection(self, connection=None) -> None:
        """Unpin a fully-unwound connection and return it to the pool.

        The resolver already does this when the outermost level closes; this
        covers the residue case where a registry entry survives with no open
        level (a boundary running after a driver-level failure), which would
        otherwise pin a dead handle for the rest of the context.
        """

        registry = _get_registry()
        connection_name = self._resolve_connection_name(connection)
        conn = registry.get(connection_name)
        if conn is None or int(getattr(conn, "transaction_level", 0) or 0) > 0:
            return

        registry.pop(connection_name, None)
        try:
            conn.open = 0
            conn.close_connection()
        except Exception:
            _logger.debug(
                "transaction boundary: connection close failed",
                exc_info=True,
            )

    def commit_open_transactions(self, connection=None) -> None:
        """Commit every open transaction level on the context-pinned connection.

        Sync pipeline jobs run inline in one asyncio task. When an outer
        ``with db.transaction()`` stays pinned in the ContextVar registry,
        later ``with db.transaction()`` blocks become SAVEPOINTs — releasing
        them does not persist rows. A rollback when the root job finishes
        then drops rows that downstream stages already read, and a
        post-stage verification sees an empty table.

        Call at pipeline stage boundaries (match → validate → consolidate)
        so each stage's writes are durably committed before the next stage
        runs. Only a caller that OWNS the connection may use this; a boundary
        that can run inside somebody else's transaction must take a baseline
        and use :meth:`commit_transactions_above` instead.
        """
        self._unwind_transactions(0, connection, commit=True)
        self._release_pinned_connection(connection)

    def rollback_open_transactions(self, connection=None) -> None:
        """Roll back every open level on the context-pinned connection.

        This is the failure-side counterpart of
        :meth:`commit_open_transactions`. Framework execution boundaries use
        it before propagating a failed or cancelled unit of work so a caught
        exception cannot leak a live transaction into the next sync job.
        """
        self._unwind_transactions(0, connection, commit=False)
        self._release_pinned_connection(connection)

    @contextmanager
    def transaction(self, connection=None):
        """Context manager for transaction handling"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        with resolver.transaction(connection_name):
            yield self

    def select(self, query, bindings=(), connection=None):
        """Execute a raw SELECT query and return results as list of dicts.

        Always returns the connection to the pool — the previous code
        path minted a fresh psycopg2 connection per call and never
        closed it, so every ``DB.select(...)`` outside a transaction
        leaked one connection. After ``max_overflow`` calls the pool
        was exhausted and every request hung in ``checkout()``.

        If the current context already has an open transaction on this
        connection, ``_create_connection_instance`` short-circuits to
        the pinned handle — we must NOT close that one (the
        transaction's commit/rollback path owns its lifecycle).
        """

        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        in_active_txn = _get_registry().get(connection_name) is not None
        conn = resolver._create_connection_instance(connection_name)
        try:
            conn.set_cursor()
            conn.statement(query, bindings)
            rows = conn._cursor.fetchall() if conn._cursor else []
            return [dict(row) for row in rows]
        finally:
            if not in_active_txn:
                try:
                    conn.open = 0
                    conn.close_connection()
                except Exception:
                    _logger.debug("connection close failed in select()", exc_info=True)

    def select_one(self, query, bindings=(), connection=None):
        """Execute a raw SELECT and return the first row as a dict, or None.

        Convenience wrapper around :meth:`select` for queries that are
        expected to return at most one row (aggregates, lookups by PK,
        ``LIMIT 1``, etc.).  Eliminates the pervasive
        ``rows = DB.select(...); row = rows[0] if rows else None``
        boilerplate scattered across repositories.
        """
        rows = self.select(query, bindings, connection)
        return rows[0] if rows else None

    def statement(self, query, bindings=(), connection=None):
        """Executes raw SQL statement"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.statement(query, bindings, connection_name)

    def table(self, table_name, connection=None):
        """Returns query builder scoped to a table (Laravel DB::table equivalent)."""
        return self.query(connection).table(table_name)

    def query(self, connection=None):
        """Returns query builder instance"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.get_query_builder(connection_name)

    def schema(self, connection=None, schema=None):
        """Returns schema builder instance"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.get_schema_builder(connection_name, schema)

    # === Config and Logic Provider Methods ===

    def get_resolver(self):
        """Access to the ConnectionResolver instance."""
        return self._ensure_resolver()

    def get_connection_details(self):
        """Returns connection details - for Schema/Migration compatibility"""
        return {"default": self._default_connection, **self._connections}

    def get_default_connection(self):
        """Returns default connection name"""
        return self._default_connection

    def get_connection_info(self, connection=None):
        """Get connection information"""
        connection_name = self._resolve_connection_name(connection)
        config = self._get_connection_config(connection_name)

        return {
            "name": connection_name,
            "driver": config.get("driver"),
            "host": config.get("host"),
            "database": config.get("database"),
            "user": config.get("user"),
            "port": config.get("port"),
            "password": config.get("password"),
            "prefix": config.get("prefix", ""),
            "options": config.get("options", {}),
            "full_details": config,
        }

    def get_connection_class(self, connection=None):
        """Get connection class for specific connection"""
        connection_name = self._resolve_connection_name(connection)
        config = self._get_connection_config(connection_name)
        driver = config.get("driver")

        if not driver:
            raise ConfigurationException(
                f"No driver specified for connection '{connection_name}'"
            )

        resolver = self._ensure_resolver()
        return resolver.connection_factory.make(driver)

    def create_connection_instance(self, connection=None, schema=None):
        """Return a connection instance — transaction-aware.

        If the current execution context has an open transaction on this
        connection name (tracked in ``ConnectionResolver``'s per-context
        ``_active_connections`` ``ContextVar``), return that same instance
        so ``QueryBuilder`` and callers that bypass the resolver still
        run inside the transaction's psycopg2 session.

        Prior behaviour always minted a fresh instance, which meant every
        ``QueryBuilder.new_connection()`` call inside a
        ``with db.transaction(): ...`` block ran against a pool-checked-out
        autocommit connection — the transaction's rollback couldn't undo
        writes because the writes were never part of the transaction.
        """
        connection_name = self._resolve_connection_name(connection)

        # Transaction-aware short-circuit: reuse the active connection if
        # this context is inside ``with db.transaction()``.
        try:
            active = _get_registry().get(connection_name)
            if active is not None:
                return active
        except Exception:
            # Defensive — if the registry lookup ever fails we still want
            # to fall through to a fresh connection rather than crash.
            _logger.warning("transaction registry lookup failed", exc_info=True)

        connection_info = self.get_connection_info(connection_name)
        connection_class = self.get_connection_class(connection_name)

        # Remove fields that connection class doesn't expect but keep full_details
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

        return connection_class(**clean_info).set_schema(schema).make_connection()

    def get_platform(self, connection=None):
        """Get platform for specific connection"""
        connection_class = self.get_connection_class(connection)
        platform_class = connection_class.get_default_platform()
        # Ensure we return an instance, not a class
        if isinstance(platform_class, type):
            return platform_class()
        return platform_class

    def get_grammar(self, connection=None):
        """Get grammar for specific connection"""
        connection_class = self.get_connection_class(connection)
        return connection_class.get_default_query_grammar()

    def resolve_connection_for_schema(self, connection_key):
        """Resolve connection for Schema class usage"""
        if connection_key == "default" or connection_key is None:
            return self._default_connection
        return connection_key

    def morph_map(self, morph_map_dict) -> Self:
        """Register morph type mappings for polymorphic relationships.

        Args:
            morph_map_dict: Dict mapping type names to model class paths
                           e.g. {"post": "app.models.Post.Post", "user": "app.models.User.User"}

        Returns:
            self for method chaining
        """
        self._morph_map = morph_map_dict
        return self

    def get_morph_map(self):
        """Get the morph map for polymorphic relationships.

        Returns:
            Dict mapping type names to model class paths
        """
        return self._morph_map

    def validate_connection(self, connection_name):
        """Validate that connection exists and has required config"""
        if connection_name not in self._connections:
            raise ConnectionNotRegisteredException(
                f"Could not find the '{connection_name}' connection details"
            )

        config = self._connections[connection_name]
        if not config.get("driver"):
            raise ConnectionNotRegisteredException(
                f"No driver specified for connection '{connection_name}'"
            )

        return True
