"""May a destructive developer command touch this database?

``dev:reset --db``, a local seed, a fixture loader: every one of them is
irreversible on the database it reaches, and every one used to ask only
"is this production?". That is a blacklist — ``APP_ENV=staging``,
``preprod``, a typo or an unset value all read as "not production", and a
developer shell with ``APP_ENV=local`` pointed at a remote database passed
too. The answer here is a whitelist on BOTH axes:

* the environment is a developer or test environment
  (``is_local_environment``), or one the caller names explicitly, and
* the database host is this machine.

The predicate is pure: it reads no configuration. ``configured_database_host``
is the one place that turns ``database.default`` + ``database.drivers`` into a
host, so every caller asks about the connection the process will actually use.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

from .Environments import is_local_environment, normalize_environment

#: The only database hosts a destructive developer command may reach. An
#: unset host is NOT here: it is a misconfiguration, not proof of locality.
LOCAL_DATABASE_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def is_local_database_host(raw: object) -> bool:
    """Whether *raw* names a database on this machine."""
    return str(raw or "").strip().lower() in LOCAL_DATABASE_HOSTS


def local_environment_refusal(
    *, app_env: object, extra_environments: Iterable[str] = ()
) -> str | None:
    """The environment half of ``local_database_refusal``: why *app_env* is not
    a developer/test environment (nor one of ``extra_environments``)."""
    allowed_extra = {normalize_environment(name) for name in extra_environments}
    if is_local_environment(app_env) or normalize_environment(app_env) in allowed_extra:
        return None
    shown = str(app_env or "").strip() or "(unset)"
    return f"APP_ENV={shown} is not a local environment"


def local_database_refusal(
    *,
    app_env: object,
    db_host: object,
    extra_environments: Iterable[str] = (),
) -> str | None:
    """Why a destructive developer command may not run here, or ``None``.

    ``extra_environments`` widens the ENVIRONMENT whitelist only (e.g. a CI
    lane whose database is a throwaway container); the host check still
    applies to it.
    """
    refusal = local_environment_refusal(
        app_env=app_env, extra_environments=extra_environments
    )
    if refusal is not None:
        return refusal
    if not is_local_database_host(db_host):
        shown = str(db_host or "").strip().lower() or "(unset)"
        return f"DB_HOST={shown} is not a local database"
    return None


def configured_database_host(read: Callable[[str, Any], Any]) -> object:
    """The host of the default connection, read through *read* (``config``)."""
    drivers = read("database.drivers", {}) or {}
    connection = drivers.get(read("database.default", "app") or "app") or {}
    return connection.get("host") if isinstance(connection, dict) else None


def configured_local_database_refusal(
    read: Callable[[str, Any], Any],
    *,
    extra_environments: Iterable[str] = (),
) -> str | None:
    """``local_database_refusal`` for the running process's own configuration.

    An unreadable configuration is a refusal, never a pass.
    """
    try:
        return local_database_refusal(
            app_env=read("app.env", ""),
            db_host=configured_database_host(read),
            extra_environments=extra_environments,
        )
    except Exception as exc:  # noqa: BLE001 — unreadable config refuses
        return f"configuration could not be verified ({type(exc).__name__})"


__all__ = [
    "LOCAL_DATABASE_HOSTS",
    "configured_database_host",
    "configured_local_database_refusal",
    "is_local_database_host",
    "local_database_refusal",
    "local_environment_refusal",
]
