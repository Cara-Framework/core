"""Database-infrastructure exceptions and the root of the ORM taxonomy.

``ORMException`` is the single base for EVERY ORM/database error the
framework raises — connections, migrations, and (via ``ModelException``
in ``types.ModelExceptions``) model and query failures.

It did not used to be. Two disjoint roots shipped side by side:
``ORMException`` here and ``ModelException`` there, each with its own
``ModelNotFoundException`` / ``QueryException`` / ``InvalidArgumentException``
/ ``MultipleRecordsFoundException`` / ``DriverNotFoundException``. The two
public barrels one level apart bound those short names to DIFFERENT
classes, so ``except ORMException`` — the base whose own docstring
advertised it as "Base for ORM-related errors" — caught NONE of the
errors the ORM actually raised, and ``from cara.exceptions.types import
ModelNotFoundException`` produced an ``except`` clause that could never
fire. The duplicates are gone and ``ModelException`` now descends from
this class, so one ``except ORMException`` finally covers "anything the
database layer raised".
"""

from __future__ import annotations

from .Base import CaraException


class ORMException(CaraException):
    """Base for every ORM/database error, including migrations and connections."""

    pass


class ConnectionNotRegisteredException(ORMException):
    """Exception raised when a database connection is not registered."""

    pass


class MigrationException(ORMException):
    """The migration history layer refused, or could not describe, a request.

    Exists because ``ORMException`` stopped being a usable sentinel the
    moment it became the root of the WHOLE database taxonomy.
    ``MigrationTracker.ensure_migrations_table`` wraps any bootstrap
    failure with the table name and re-raises its OWN already-contextual
    errors untouched — ``except ORMException: raise`` was how it told the
    two apart. Once ``QueryException`` joined the taxonomy under
    ``ORMException``, that clause also matched the driver error raised by
    ``CREATE TABLE`` itself, so a permission failure on the migrations
    table propagated as a bare ``relation "migrations" permission denied``
    with no hint of which table or which phase produced it. The
    "mine, already explained" signal is now a class, not a base everyone
    else joined.
    """

    pass


class SchemaPlanRefused(MigrationException):
    """A model↔database difference evolve-mode planning will not derive SQL for.

    Raised per difference and collected by the planner, so one unmappable
    column reports itself without abandoning the rest of the plan. The message
    IS the reason a human needs — "NOT NULL with no default", "type change
    whose USING clause depends on the data" — because the whole point of
    refusing is to hand the decision back with its context intact.
    """


class DatabaseUnavailableException(ORMException):
    """Postgres is unreachable, connection was refused, or the pool was
    exhausted before a slot could be acquired.

    Distinct from ``QueryException`` (a bad query) — this is the
    "the database isn't answering" path. Maps to HTTP 503 so callers
    (and load balancers) can distinguish it from a 500 application
    fault and retry without alarming oncall.
    """

    is_http_exception = True
    status_code = 503

    def __init__(
        self,
        message: str = "Database temporarily unavailable",
        retry_after: int | None = None,
    ):
        super().__init__(message)
        if retry_after is not None:
            self.retry_after = retry_after


__all__ = [
    "ConnectionNotRegisteredException",
    "DatabaseUnavailableException",
    "MigrationException",
    "ORMException",
]
