"""Atomic transaction support for Cara ORM.

Provides @atomic decorator and context manager for clean transaction
handling with automatic savepoint support for nested transactions.

Two distinct ``__exit__`` paths
-------------------------------
1. **Top-level** (we opened the outer transaction): commit / rollback
   must go through the resolver so the registry entry is cleared and
   the psycopg2 connection is returned to the pool. Calling
   ``self._conn.commit()`` directly bypasses the resolver — the
   connection stays pinned in the per-context registry forever, every
   subsequent query inside that context picks up the same committed
   handle thinking it's still in a transaction, and the pool slowly
   bleeds connections.

2. **Nested savepoint** (the outer transaction already existed when
   ``__enter__`` ran): we own a savepoint, not the connection. The
   underlying ``connection.commit()`` releases the savepoint via the
   driver's transaction-level math and the outer transaction stays
   active. Resolver-level commit must NOT run here — it would unwind
   the outer caller's transaction.
"""

from functools import wraps


class Atomic:
    """Transaction context manager and decorator with savepoint support."""

    def __init__(self, connection=None):
        self.connection_name = connection
        self._db = None
        self._conn = None
        # Track whether THIS Atomic opened the outer transaction.
        # Only the opener invokes resolver-level commit/rollback.
        self._is_outer_transaction = False
        self._connection_name: str | None = None

    def _get_db(self):
        if self._db is None:
            from cara.eloquent.DatabaseManager import DatabaseManager

            self._db = DatabaseManager.get_instance()
        return self._db

    def __enter__(self):
        db = self._get_db()
        resolver = db._ensure_resolver()
        conn_name = self.connection_name or db.get_default_connection()

        # Try to attach to an existing active connection (savepoint
        # path). If that raises, we're the outer caller and must open
        # a real transaction via the resolver — which registers the
        # connection in the per-context registry.
        try:
            self._conn = resolver._get_active_connection(conn_name)
            # Already in a transaction — push a savepoint via the
            # connection's nested-transaction machinery.
            self._conn.begin()
            self._is_outer_transaction = False
        except Exception:
            self._conn = resolver.begin_transaction(conn_name)
            self._is_outer_transaction = True

        self._connection_name = conn_name
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Always re-resolve the resolver so we don't keep a stale
        # reference if the manager swapped instances mid-flight (rare,
        # but happens during test re-init).
        db = self._get_db()
        resolver = db._ensure_resolver()
        name = self._connection_name

        try:
            if exc_type is not None:
                if self._is_outer_transaction:
                    resolver.rollback(name)
                else:
                    self._conn.rollback()
                return False  # re-raise
            else:
                if self._is_outer_transaction:
                    resolver.commit(name)
                else:
                    self._conn.commit()
                return False
        finally:
            self._conn = None

    def __call__(self, func):
        """Use as decorator: @atomic() or @atomic(connection='other')

        Each call materializes a *fresh* ``Atomic`` instance — the
        decorator builds at import time, but ``self._conn`` /
        ``self._db`` are per-transaction state. Reusing the import-time
        instance lets two concurrent callers stomp on each other's
        ``_conn``: caller B's commit unwinds caller A's transaction,
        caller A's rollback closes B's connection. Any sync-ORM call
        site decorated with ``@atomic()`` and reached from threads or
        ``asyncio.to_thread`` was vulnerable.
        """
        connection_name = self.connection_name

        @wraps(func)
        def wrapper(*args, **kwargs):
            with Atomic(connection=connection_name):
                return func(*args, **kwargs)

        return wrapper


def atomic(connection=None):
    """Create an atomic transaction context manager/decorator.

    Usage as context manager::

        with atomic():
            Product.create({...})

    Usage as decorator::

        @atomic()
        def my_func():
            Product.create({...})

    Nested usage (inner becomes savepoint)::

        with atomic():
            Product.create({...})
            with atomic():
                ProductImage.create({...})
    """
    return Atomic(connection=connection)
