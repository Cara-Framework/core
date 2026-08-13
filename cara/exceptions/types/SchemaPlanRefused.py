"""SchemaPlanRefused."""

from __future__ import annotations

from .MigrationException import MigrationException


class SchemaPlanRefused(MigrationException):
    """A model↔database difference evolve-mode planning will not derive SQL for.

    Raised per difference and collected by the planner, so one unmappable
    column reports itself without abandoning the rest of the plan. The message
    IS the reason a human needs — "NOT NULL with no default", "type change
    whose USING clause depends on the data" — because the whole point of
    refusing is to hand the decision back with its context intact.
    """
