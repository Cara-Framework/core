"""
Path Manager for the Cara runtime environment.

This module provides utilities for handling filesystem paths throughout the project structure.
"""

from __future__ import annotations

import os


class PathManager:
    """
    Filesystem Path Manager for the Cara framework.

    Handles all filesystem path operations for the project structure.
    If `set_base_path(...)` is never called, `base_path()` anchors to the
    nearest ancestor of cwd that owns `bootstrap.py` — the deployable — and
    refuses outright when cwd is the workspace root that HOLDS deployables.
    See :meth:`_anchor_from_cwd`.

    Individual paths can be overridden via `set_path_override(key, path)`.
    When set, the corresponding `*_path()` method returns the override
    instead of deriving from base_path.  This lets multi-app monorepos
    share a single migrations (or seeds, etc.) directory without Cara
    needing to know about the repo layout.
    """

    _base_path: str = None
    _overrides: dict = {}
    # cwd -> resolved anchor. Resolution walks the filesystem, and base_path()
    # is called on nearly every path derivation.
    _anchors: dict = {}

    @staticmethod
    def set_base_path(path: str) -> None:
        """
        Manually set the project base path (absolute).
        Call this early to override cwd.
        """
        PathManager._base_path = path

    @staticmethod
    def set_path_override(key: str, path: str) -> None:
        """
        Override a specific path key with an absolute path.

        Supported keys match method names: 'migrations', 'seeds', 'database',
        'storage', 'config', 'routes', 'public', 'views', 'app', etc.

        Example::

            PathManager.set_path_override(
                "migrations", "/project/commons/database/migrations"
            )
        """
        PathManager._overrides[key] = path

    @staticmethod
    def get_path_override(key: str):
        """Return the override for *key*, or ``None`` if not set."""
        return PathManager._overrides.get(key)

    @staticmethod
    def _holds_deployables(directory: str) -> bool:
        """True when *directory* CONTAINS deployables instead of being one."""
        try:
            children = os.scandir(directory)
        except OSError:
            return False
        with children:
            return any(
                entry.is_dir()
                and os.path.isfile(os.path.join(entry.path, "bootstrap.py"))
                for entry in children
            )

    @staticmethod
    def _anchor_from_cwd() -> str:
        """Anchor an unconfigured process to its DEPLOYABLE, never to bare cwd.

        This used to return ``os.getcwd()`` outright, which makes every derived
        path — ``database/migrations``, ``storage/logs``, ``config`` — a
        function of where the operator happened to be standing. A run from the
        WORKSPACE ROOT therefore created ``database/`` and ``storage/`` there:
        outside every deployable, outside every ``.gitignore`` (the workspace
        root is not a git repository, so nothing ignores them) and outside the
        retention the deployables' own ``storage/`` gets. It really happened —
        a stray migration-generation lock and a month of API stack traces.
        DOCTRINE §1 enumerates the workspace-root children exhaustively and
        neither directory is among them.

        A deployable is the nearest ancestor owning ``bootstrap.py`` — the
        entry point that calls :meth:`set_base_path` when it is the one
        running. Standing in the workspace root is not ambiguity but misuse: it
        HOLDS deployables rather than being one, so it fails loudly instead of
        being written into. Everywhere else keeps cwd, so a fixture tree or an
        installed package behaves exactly as before.
        """
        try:
            cwd = os.getcwd()
        except OSError:  # the working directory was removed under us
            return os.sep
        cached = PathManager._anchors.get(cwd)
        if cached is not None:
            return cached

        anchor = None
        candidate = cwd
        while anchor is None:
            if os.path.isfile(os.path.join(candidate, "bootstrap.py")):
                anchor = candidate
                break
            parent = os.path.dirname(candidate)
            if parent == candidate:
                break
            candidate = parent

        if anchor is None:
            if PathManager._holds_deployables(cwd):
                raise RuntimeError(
                    f"{cwd} is a workspace root, not a deployable: it holds the "
                    "deployables rather than being one, and anchoring here "
                    "writes database/ and storage/ outside every repository. "
                    "Run the command from api/ or services/, or call "
                    "PathManager.set_base_path() with the deployable root."
                )
            anchor = cwd

        PathManager._anchors[cwd] = anchor
        return anchor

    @staticmethod
    def base_path(relative: str = "") -> str:
        """
        Return the project root path.
        If `relative` is provided, append it under the root.
        """
        base = PathManager._base_path or PathManager._anchor_from_cwd()
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
        """Return the models directory, defaulting to <base>/commons/models.

        Honors ``set_path_override("models", ...)`` first — monorepo Kernels
        point this at the shared ``commons/models`` directory, which lives
        outside the per-deployable ``app`` subtree (mirrors how ``migrations``
        is wired). Absent an override it defaults to ``<base>/commons/models``.
        (Pre-W2 this returned ``<base>/app/models``.)
        """
        override = PathManager.get_path_override("models")
        if override:
            return os.path.join(override, relative) if relative else override
        base = PathManager.base_path()
        return (
            os.path.join(base, "commons", "models", relative)
            if relative
            else os.path.join(base, "commons", "models")
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
        """Return <base>/database/migrations[/relative], or the override if set."""
        override = PathManager.get_path_override("migrations")
        if override:
            return os.path.join(override, relative) if relative else override
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
