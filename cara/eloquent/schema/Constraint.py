from __future__ import annotations


class Constraint:
    def __init__(
        self,
        name,
        constraint_type,
        columns=None,
        expression=None,
        where=None,
    ):
        self.name = name
        self.constraint_type = constraint_type
        self.columns = columns or []
        # Raw boolean SQL for a CHECK constraint, e.g.
        # ``current_price IS NULL OR current_price >= 0``. Only set for
        # ``constraint_type == "check"``; the platform renders it verbatim
        # inside ``CONSTRAINT <name> CHECK (<expression>)``.
        self.expression = expression
        # Optional partial-index predicate for a conditional UNIQUE
        # (Postgres partial unique index). When set on a ``unique``
        # constraint the platform emits a standalone
        # ``CREATE UNIQUE INDEX <name> ON <table> (...) WHERE <where>``
        # instead of an inline table-level UNIQUE constraint (which cannot
        # carry a predicate). ``None`` keeps the plain UNIQUE behaviour.
        self.where = where
