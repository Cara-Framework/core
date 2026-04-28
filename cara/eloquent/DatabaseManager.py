import threading
from contextlib import contextmanager


class DatabaseManager:
    """
    Database Manager - Central database management with integrated configuration
    Single Responsibility: Database operations, connection management and configuration
    Open/Closed: Extensible through ConnectionResolver
    """

    _instance = None
    # Guards ``get_instance`` against the classic double-checked-locking
    # race: two threads both observe ``_instance is None``, both call
    # ``cls()``, both auto-configure — one wins, the other's instance
    # is silently discarded along with anything that captured a
    # reference to it. With a lock, exactly one bootstrap runs.
    _instance_lock = threading.Lock()

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

        # Initialize morph map for polymorphic relationships
        self._morph_map = {}

    def _ensure_resolver(self):
        """Lazy initialization of resolver to avoid circular dependency"""
        if self._resolver is None:
            from .connections import ConnectionResolver

            self._resolver = ConnectionResolver(database_manager=self)
        return self._resolver

    def set_database_config(self, default_connection, connection_details):
        """Set database configuration"""
        # Normalize connection_details to a plain dict. Sometimes config()
        # returns a dotty_dict whose __hash__/__str__ recurses infinitely
        # when used as a dict key or in `in` checks — flatten it here.
        if hasattr(connection_details, "to_dict"):
            connection_details = connection_details.to_dict()
        elif not isinstance(connection_details, dict):
            connection_details = dict(connection_details)
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
            with cls._instance_lock:
                if cls._instance is None:
                    instance = cls()
                    instance._auto_configure()
                    cls._instance = instance
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
        except Exception as e:
            # Config not available yet (early bootstrap).
            # Provider will configure later — this is OK. Log at debug
            # so it's visible if someone is troubleshooting boot order.
            import logging
            logging.getLogger("cara.database").debug(
                "DatabaseManager._auto_configure skipped (early bootstrap): %s", e
            )

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
        from .connections.ConnectionResolver import _get_registry

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
                    pass

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
        """Access to the ConnectionResolver instance."""
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
            from .connections.ConnectionResolver import _get_registry
            active = _get_registry().get(connection_name)
            if active is not None:
                return active
        except Exception:
            # Defensive — if the registry lookup ever fails we still want
            # to fall through to a fresh connection rather than crash.
            pass

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

    def morph_map(self, morph_map_dict):
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
