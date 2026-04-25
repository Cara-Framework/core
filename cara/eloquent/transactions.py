"""Atomic transaction support for Cara ORM.

Provides @atomic decorator and context manager for clean transaction handling
with automatic savepoint support for nested transactions.
"""

from contextlib import contextmanager
from functools import wraps


class Atomic:
    """Transaction context manager and decorator with savepoint support."""

    def __init__(self, connection=None):
        self.connection_name = connection
        self._db = None
        self._conn = None

    def _get_db(self):
        if self._db is None:
            from cara.eloquent.DatabaseManager import DatabaseManager
            self._db = DatabaseManager.get_instance()
        return self._db

    def __enter__(self):
        db = self._get_db()
        resolver = db._ensure_resolver()
        conn_name = self.connection_name or db.get_default_connection()

        # Get or create the active connection
        try:
            self._conn = resolver._get_active_connection(conn_name)
            # Already in a transaction — use savepoint
            self._conn.begin()  # This now auto-creates savepoint when nested
        except Exception:
            # No active transaction — start one
            self._conn = resolver.begin_transaction(conn_name)

        self._connection_name = conn_name
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self._conn.rollback()
            return False
        else:
            self._conn.commit()
            return False

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

    Usage as context manager:
        with atomic():
            Product.create({...})

    Usage as decorator:
        @atomic()
        def my_func():
            Product.create({...})

    Nested usage (inner becomes savepoint):
        with atomic():
            Product.create({...})
            with atomic():
                ProductImage.create({...})
    """
    return Atomic(connection=connection)
