"""PartitionDeclarationError."""

from __future__ import annotations

from .MigrationException import MigrationException


class PartitionDeclarationError(MigrationException, ValueError):
    """A model's ``__partition__`` the framework cannot build.

    Raised by ``cara.eloquent.schema.HashPartitioning`` when the declaration is
    malformed (a method other than ``hash``, a partition count out of range)
    or names a table Postgres would refuse to partition (a nullable key, a
    PRIMARY KEY or UNIQUE that omits the key). It fails ``make:migration``
    and the Blueprint compile, so it never reaches a database. ``ValueError``
    stays a second base: the declaration is a bad value.
    """
