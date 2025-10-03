from cara.configuration import config
from cara.eloquent.DatabaseManager import DatabaseManager
from cara.foundation import Provider


class EloquentProvider(Provider):
    """
    Eloquent ORM Provider - Configures and binds DatabaseManager.

    PATTERN: Explicit Dependency Injection
    - Provider reads config (single source of truth)
    - Injects config into DatabaseManager
    - Binds configured instance to container

    Benefits: Clear, testable, no magic, follows Laravel pattern
    """

    def __init__(self, application):
        self.application = application

    def register(self):
        """Configure and register DatabaseManager as singleton"""
        # Read database config explicitly (single responsibility)
        default_connection = config("database.default", "app")
        connection_details = config("database.drivers", {})

        # Get singleton instance
        database_manager = DatabaseManager.get_instance()

        # Inject config explicitly (testable, clear)
        if connection_details:
            database_manager.set_database_config(default_connection, connection_details)

        # Bind configured instance to container
        self.application.bind("DB", database_manager)

    def boot(self):
        """Boot provider (optional hook for post-registration setup)"""
        pass
