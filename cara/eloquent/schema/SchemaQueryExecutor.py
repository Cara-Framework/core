class SchemaQueryExecutor:
    """Single Responsibility: Executes schema queries"""

    def __init__(self, connection_manager, dry=False):
        self.connection_manager = connection_manager
        self.dry = dry
        self._sql = None

    def execute_query(self, sql, bindings=()):
        """Execute SQL query or store for dry run"""
        if self.dry:
            self._sql = sql
            return sql

        connection = self.connection_manager.create_connection_instance()
        return bool(connection.query(sql, bindings))

    def get_query_result(self, sql, bindings=()):
        """Get query result (not boolean)"""
        if self.dry:
            self._sql = sql
            return sql

        connection = self.connection_manager.create_connection_instance()
        return connection.query(sql, bindings)
