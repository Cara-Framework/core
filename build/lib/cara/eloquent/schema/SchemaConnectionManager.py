from cara.exceptions import ConnectionNotRegisteredException


class SchemaConnectionManager:
    """Single Responsibility: Manages schema connection logic"""

    def __init__(self, db_manager):
        self._db_manager = db_manager
        self._connection = None
        self.connection = None
        self.connection_class = None
        self.platform = None

    def resolve_connection(self, connection_key):
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
