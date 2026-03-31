from contextlib import contextmanager


class ConnectionResolver:
    """
    Single Responsibility: Manages connections and transactions ONLY
    Open/Closed: Can be extended with new connection types
    No configuration management - gets config from DatabaseManager
    """

    _active_connections = {}

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
        """Create connection instance"""
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
        """Start transaction - Single responsibility for transaction management"""
        connection = self._create_connection_instance(connection_name).begin()
        self.__class__._active_connections.update({connection_name: connection})
        return connection

    def commit(self, connection_name):
        """Commit transaction - Single responsibility"""
        connection = self._get_active_connection(connection_name)
        self._remove_active_connection(connection_name)
        connection.commit()

    def rollback(self, connection_name):
        """Rollback transaction - Single responsibility"""
        connection = self._get_active_connection(connection_name)
        self._remove_active_connection(connection_name)
        connection.rollback()

    @contextmanager
    def transaction(self, connection_name):
        """Context manager for transaction handling - Single responsibility"""
        self.begin_transaction(connection_name)
        try:
            yield self
        except Exception:
            self.rollback(connection_name)
            raise

        try:
            self.commit(connection_name)
        except Exception:
            self.rollback(connection_name)
            raise

    def _get_active_connection(self, connection_name):
        """Helper method - DRY principle"""
        if connection_name not in self._active_connections:
            raise ValueError(
                f"No active transaction found for connection: {connection_name}"
            )
        return self._active_connections[connection_name]

    def _remove_active_connection(self, connection_name):
        """Helper method - DRY principle"""
        self._active_connections.pop(connection_name, None)

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
