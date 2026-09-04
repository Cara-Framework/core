from __future__ import annotations

import logging
from typing import Self

from cara.exceptions import ConnectionNotRegisteredException

_logger = logging.getLogger("cara.schema")


class SchemaConnectionManager:
    """Single Responsibility: Manages schema connection logic"""

    def __init__(self, db_manager):
        self._db_manager = db_manager
        self._connection = None
        self.connection = None
        self.connection_class = None
        self.platform = None

    def resolve_connection(self, connection_key) -> Self:
        """Resolve connection using DatabaseManager"""
        self.connection = self._db_manager.resolve_connection_for_schema(connection_key)

        if not self.connection:
            raise ConnectionNotRegisteredException(
                "No connection specified and no default connection found"
            )

        # Validate and get connection components
        self._db_manager.validate_connection(self.connection)
        self.connection_class = self._db_manager.get_connection_class(self.connection)
        self.platform = self._db_manager.get_platform(self.connection)

        return self

    def create_connection_instance(self, schema=None):
        """Create actual connection instance"""
        if not self.connection:
            raise ConnectionNotRegisteredException("No connection resolved")

        self._connection = self._db_manager.create_connection_instance(
            self.connection, schema
        )
        return self._connection

    def get_connection_info(self):
        """Get connection information"""
        return self._db_manager.get_connection_info(self.connection)

    @staticmethod
    def release(connection) -> None:
        """Return a connection this manager minted, never an active transaction.

        The rule lives here because this class is what hands the connection
        out. ``Schema``, ``SchemaQueryExecutor`` and ``MigrationTracker`` all
        borrow DDL connections and each used to carry its own copy of the
        teardown; this is the single home for all three.
        """
        if connection is None:
            return
        transaction_level = getattr(connection, "transaction_level", 0)
        if isinstance(transaction_level, (int, float)) and transaction_level > 0:
            return
        try:
            close = getattr(connection, "close_connection", None)
            if callable(close):
                close()
        except Exception:
            # Cleanup must never mask the real result, whatever driver-specific
            # exception the close raises — but it must not vanish either.
            _logger.debug("schema connection close failed", exc_info=True)
