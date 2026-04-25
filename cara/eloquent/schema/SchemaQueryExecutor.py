class SchemaQueryExecutor:
    """Single Responsibility: Executes schema queries.

    Each schema statement borrows a fresh pool connection; if we don't
    return it the pool drains within ~50 statements (e.g. a migration
    sweep that touches partitioned tables and indexes). The connection
    is always released back via ``close_connection()`` in a finally.
    """

    def __init__(self, connection_manager, dry=False):
        self.connection_manager = connection_manager
        self.dry = dry
        self._sql = None

    def execute_query(self, sql, bindings=()):
        """Execute SQL query or store for dry run."""
        if self.dry:
            self._sql = sql
            return sql

        connection = self.connection_manager.create_connection_instance()
        try:
            return bool(connection.query(sql, bindings))
        finally:
            self._release(connection)

    def get_query_result(self, sql, bindings=()):
        """Get query result (not boolean)."""
        if self.dry:
            self._sql = sql
            return sql

        connection = self.connection_manager.create_connection_instance()
        try:
            return connection.query(sql, bindings)
        finally:
            self._release(connection)

    @staticmethod
    def _release(connection) -> None:
        """Return the connection to the pool, swallow any cleanup error."""
        if connection is None:
            return
        try:
            close = getattr(connection, "close_connection", None)
            if callable(close):
                close()
        except Exception:
            # A failed release should never mask the real query result.
            pass
