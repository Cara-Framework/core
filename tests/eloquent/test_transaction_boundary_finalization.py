"""Framework transaction-boundary finalization."""

from __future__ import annotations

from cara.eloquent import DatabaseManager
from cara.eloquent.connections.ConnectionResolver import _get_registry


class _Connection:
    def __init__(self, level: int) -> None:
        self.transaction_level = level
        self.open = 1
        self.closed = False

    def rollback(self) -> None:
        self.transaction_level -= 1

    def close_connection(self) -> None:
        self.closed = True


def test_rollback_open_transactions_unwinds_and_releases_pinned_connection() -> None:
    database = DatabaseManager()
    connection = _Connection(level=3)
    database._ensure_resolver()
    registry = _get_registry()
    connection_name = database._resolve_connection_name(None)
    registry[connection_name] = connection

    database.rollback_open_transactions()

    assert connection.transaction_level == 0
    assert connection.closed is True
    assert connection_name not in registry
