"""ScratchDatabaseException."""

from __future__ import annotations

from .MigrationException import MigrationException


class ScratchDatabaseException(MigrationException):
    """A disposable database could not be named, created or filled.

    Sibling of ``SchemaPlanRefused`` rather than a plain ``RuntimeError``
    because the two are the same kind of answer: the schema tooling declining
    to proceed, with the reason a human needs already in the message. Both
    ``schema:verify`` and ``schema:plan --rehearse`` catch it to report and
    exit rather than to recover — nothing here is retryable, and a scratch
    that could not be prepared must never be silently skipped, or a rehearsal
    reports success having rehearsed nothing.
    """
