"""Lazy optional-dependency loading for CLI command groups.

cara's command groups for DB migrations (``cara.eloquent`` → ``psycopg2`` /
``faker``) and queues (``cara.queues`` → ``pika`` / ``redis``) depend on heavy,
OPTIONAL third-party packages. Importing those at command-MODULE load time
forced every service to install them just to have a working CLI — and because
``cara.commands.core.__init__`` imports every command eagerly, ONE missing dep
made the whole package import abort partway and SILENTLY drop every command
registered after it (this is what made ``serve`` / ``routes:*`` vanish on the
DB-less ``studio`` service).

Root fix: command groups defer their heavy imports to CALL time and, when the
optional group isn't installed, fail LOUD with an actionable message naming the
``cara[<extra>]`` to install — affecting only that one command, never its
siblings. Module import stays dependency-free, so the command package always
imports cleanly and every unrelated command (serve, routes, make:*) registers.
"""

from __future__ import annotations


class OptionalDependencyError(RuntimeError):
    """A command needs an optional cara dependency group that isn't installed.

    Carries the extra name so the message tells the operator exactly what to
    install. Raised at command run time, never at import time.
    """


def missing_optional(extra: str, exc: ImportError) -> OptionalDependencyError:
    """Wrap a lazy-import ``ImportError`` into a loud, actionable error.

    Usage inside a command's ``handle`` (or a helper it calls)::

        try:
            from cara.eloquent.migrations import Migration
        except ImportError as exc:
            raise missing_optional("db", exc) from exc
    """
    return OptionalDependencyError(
        f"This command requires cara's optional '{extra}' dependency group, which "
        f"is not installed (import failed: {exc}). Install it with:\n\n"
        f"    pip install 'cara[{extra}]'\n\n"
        f"or add the '{extra}' dependencies to your service's requirements. "
        f"All other commands are unaffected."
    )
