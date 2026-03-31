"""
Path Manager for filesystem paths in the Cara framework.

This module provides utilities for handling filesystem paths throughout the project structure.
"""

import os


class PathManager:
    """
    Filesystem Path Manager for the Cara framework.

    Handles all filesystem path operations for the project structure.
    If `set_base_path(...)` is never called, `base_path()` returns cwd.
    """

    _base_path: str = None

    @staticmethod
    def set_base_path(path: str) -> None:
        """
        Manually set the project base path (absolute).
        Call this early to override cwd.
        """
        PathManager._base_path = path

    @staticmethod
    def base_path(relative: str = "") -> str:
        """
        Return the project root path.
        If `relative` is provided, append it under the root.
        """
        base = PathManager._base_path or os.getcwd()
        return os.path.join(base, relative) if relative else base

    # App structure paths
    @staticmethod
    def app_path(relative: str = "") -> str:
        """Return <base>/app[/relative]."""
        base = PathManager.base_path()
        return (
            os.path.join(base, "app", relative) if relative else os.path.join(base, "app")
        )

    @staticmethod
    def controllers_path(relative: str = "") -> str:
        """Return <base>/app/controllers[/relative]."""
        return PathManager.app_path(
            os.path.join("controllers", relative) if relative else "controllers"
        )

    @staticmethod
    def middlewares_path(relative: str = "") -> str:
        """Return <base>/app/middlewares[/relative]."""
        return PathManager.app_path(
            os.path.join("middlewares", relative) if relative else "middlewares"
        )

    @staticmethod
    def models_path(relative: str = "") -> str:
        """Return <base>/app/models[/relative]."""
        return PathManager.app_path(
            os.path.join("models", relative) if relative else "models"
        )

    @staticmethod
    def commands_path(relative: str = "") -> str:
        """Return <base>/app/commands[/relative]."""
        return PathManager.app_path(
            os.path.join("commands", relative) if relative else "commands"
        )

    @staticmethod
    def providers_path(relative: str = "") -> str:
        """Return <base>/app/providers[/relative]."""
        return PathManager.app_path(
            os.path.join("providers", relative) if relative else "providers"
        )

    @staticmethod
    def mailables_path(relative: str = "") -> str:
        """Return <base>/app/mail[/relative]."""
        return PathManager.app_path(
            os.path.join("mail", relative) if relative else "mail"
        )

    @staticmethod
    def jobs_path(relative: str = "") -> str:
        """Return <base>/app/jobs[/relative]."""
        return PathManager.app_path(
            os.path.join("jobs", relative) if relative else "jobs"
        )

    @staticmethod
    def events_path(relative: str = "") -> str:
        """Return <base>/app/events[/relative]."""
        return PathManager.app_path(
            os.path.join("events", relative) if relative else "events"
        )

    @staticmethod
    def listeners_path(relative: str = "") -> str:
        """Return <base>/app/listeners[/relative]."""
        return PathManager.app_path(
            os.path.join("listeners", relative) if relative else "listeners"
        )

    @staticmethod
    def notifications_path(relative: str = "") -> str:
        """Return <base>/app/notifications[/relative]."""
        return PathManager.app_path(
            os.path.join("notifications", relative) if relative else "notifications"
        )

    @staticmethod
    def policies_path(relative: str = "") -> str:
        """Return <base>/app/policies[/relative]."""
        return PathManager.app_path(
            os.path.join("policies", relative) if relative else "policies"
        )

    # Config and database paths
    @staticmethod
    def config_path(relative: str = "") -> str:
        """Return <base>/config[/relative]."""
        base = PathManager.base_path()
        return (
            os.path.join(base, "config", relative)
            if relative
            else os.path.join(base, "config")
        )

    @staticmethod
    def routes_path(relative: str = "") -> str:
        """Return <base>/routes[/relative]."""
        base = PathManager.base_path()
        return (
            os.path.join(base, "routes", relative)
            if relative
            else os.path.join(base, "routes")
        )

    @staticmethod
    def database_path(relative: str = "") -> str:
        """Return <base>/database[/relative]."""
        base = PathManager.base_path()
        return (
            os.path.join(base, "database", relative)
            if relative
            else os.path.join(base, "database")
        )

    @staticmethod
    def migrations_path(relative: str = "") -> str:
        """Return <base>/database/migrations[/relative]."""
        return PathManager.database_path(
            os.path.join("migrations", relative) if relative else "migrations"
        )

    @staticmethod
    def seeds_path(relative: str = "") -> str:
        """Return <base>/database/seeds[/relative]."""
        return PathManager.database_path(
            os.path.join("seeds", relative) if relative else "seeds"
        )

    @staticmethod
    def db_path(relative: str = "") -> str:
        """Return <base>/database/db[/relative]."""
        return PathManager.database_path(
            os.path.join("db", relative) if relative else "db"
        )

    # Storage and public paths
    @staticmethod
    def storage_path(relative: str = "") -> str:
        """Return <base>/storage[/relative]."""
        base = PathManager.base_path()
        return (
            os.path.join(base, "storage", relative)
            if relative
            else os.path.join(base, "storage")
        )

    @staticmethod
    def public_path(relative: str = "") -> str:
        """Return <base>/public[/relative]."""
        base = PathManager.base_path()
        return (
            os.path.join(base, "public", relative)
            if relative
            else os.path.join(base, "public")
        )

    @staticmethod
    def resources_path(relative: str = "") -> str:
        """Return <base>/resources[/relative]."""
        base = PathManager.base_path()
        return (
            os.path.join(base, "resources", relative)
            if relative
            else os.path.join(base, "resources")
        )

    @staticmethod
    def views_path(relative: str = "") -> str:
        """Return <base>/resources/views[/relative]."""
        return PathManager.resources_path(
            os.path.join("views", relative) if relative else "views"
        )
