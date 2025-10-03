from contextlib import contextmanager


class DatabaseManager:
    """
    Database Manager - Central database management with integrated configuration
    Single Responsibility: Database operations, connection management and configuration
    Open/Closed: Extensible through ConnectionResolver
    """

    _instance = None

    def __init__(self):
        # Initialize database configuration directly
        self._database_config = {"default": "app", "drivers": {}}
        self._queue_config = {
            "default": "database",
            "drivers": {
                "database": {
                    "connection": "default",
                    "table": "job",
                    "failed_table": "failed_job",
                }
            },
        }

        # Initialize resolver with self to avoid circular dependency
        self._resolver = None
        self._default_connection = "app"
        self._connections = {}

    def _ensure_resolver(self):
        """Lazy initialization of resolver to avoid circular dependency"""
        if self._resolver is None:
            from .connections import ConnectionResolver

            self._resolver = ConnectionResolver(database_manager=self)
        return self._resolver

    def set_database_config(self, default_connection, connection_details):
        """Set database configuration"""
        self._database_config = {
            "default": default_connection,
            "drivers": connection_details,
        }
        self._default_connection = default_connection
        self._connections = connection_details

        # Ensure resolver is initialized
        self._ensure_resolver()
        return self

    def get_database_config(self):
        """Get database configuration"""
        return self._database_config

    def set_queue_config(self, config):
        """Set queue configuration"""
        self._queue_config = config
        return self

    def get_queue_config(self):
        """Get queue configuration"""
        return self._queue_config

    @classmethod
    def get_instance(cls):
        """
        Get the global DatabaseManager singleton instance.

        Configuration strategy (hybrid approach):
        1. Primary: EloquentProvider explicitly injects config (clear, testable)
        2. Fallback: Auto-configure from config module if not yet configured

        This hybrid approach provides both clarity and convenience:
        - Provider injection = explicit, professional
        - Auto-configure fallback = works everywhere, zero boilerplate
        """
        if cls._instance is None:
            cls._instance = cls()
            cls._instance._auto_configure()
        return cls._instance

    def _auto_configure(self):
        """
        Auto-configure from config module as fallback.

        Only runs if instance not yet configured (empty connections).
        Allows DatabaseManager to work even if called before Provider runs.
        """
        # Skip if already configured by Provider
        if self._connections:
            return

        try:
            # Try to load config module
            from cara.configuration import config

            default_connection = config("database.default", "app")
            connection_details = config("database.drivers", {})

            if connection_details:
                self.set_database_config(default_connection, connection_details)
        except Exception:
            # Config not available yet (early bootstrap)
            # Provider will configure later - this is OK
            pass

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
            raise ValueError(f"Connection '{connection_name}' not found")
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

    @contextmanager
    def transaction(self, connection=None):
        """Context manager for transaction handling"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        with resolver.transaction(connection_name):
            yield self

    def statement(self, query, bindings=(), connection=None):
        """Executes raw SQL statement"""
        connection_name = self._resolve_connection_name(connection)
        resolver = self._ensure_resolver()
        return resolver.statement(query, bindings, connection_name)

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
        """Access to ConnectionResolver instance (backward compatibility)"""
        return self._ensure_resolver()

    def get_connection_details(self):
        """Returns connection details - for Schema/Migration compatibility"""
        return {"default": self._default_connection, **self._connections}

    def get_default_connection(self):
        """Returns default connection name"""
        return self._default_connection

    def get_available_connections(self):
        """List available connections"""
        return list(self._connections.keys())

    def has_connection(self, connection):
        """Check if connection exists"""
        connection_name = (
            connection if connection != "default" else self._default_connection
        )
        return connection_name in self._connections

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
            raise ValueError(f"No driver specified for connection '{connection_name}'")

        resolver = self._ensure_resolver()
        return resolver.connection_factory.make(driver)

    def create_connection_instance(self, connection=None, schema=None):
        """Create actual connection instance"""
        connection_name = self._resolve_connection_name(connection)
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

    def validate_connection(self, connection_name):
        """Validate that connection exists and has required config"""
        if connection_name not in self._connections:
            from cara.exceptions import ConnectionNotRegisteredException

            raise ConnectionNotRegisteredException(
                f"Could not find the '{connection_name}' connection details"
            )

        config = self._connections[connection_name]
        if not config.get("driver"):
            raise ConnectionNotRegisteredException(
                f"No driver specified for connection '{connection_name}'"
            )

        return True


# Convenience function for accessing DatabaseManager without tight coupling
def get_database_manager():
    """Get DatabaseManager instance without dependency coupling"""
    return DatabaseManager.get_instance()
