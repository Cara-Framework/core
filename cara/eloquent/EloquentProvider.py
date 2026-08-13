from __future__ import annotations

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
        """Build and bind this application's configured database manager."""

        # Read database config explicitly (single responsibility)
        default_connection = config("database.default")
        connection_details = config("database.drivers")

        database_manager = DatabaseManager(
            default_connection,
            connection_details,
        )

        # Bind configured instance to container
        self.application.bind("DB", database_manager)
        self.application.bind(DatabaseManager, database_manager)

    def boot(self):
        """Boot provider (optional hook for post-registration setup)"""
        pass
