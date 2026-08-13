"""MigrationException."""

from __future__ import annotations

from .ORMException import ORMException


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
