"""The ``failed_job`` dead-letter table, declared as a model.

Written by ``cara.queues.drivers`` when a job exhausts its attempts and the
dead-letter rail is the database. Reads happen through operator commands
(``queue:monitor`` / ``queue:retry``), never through application code, so the
class carries no relationships and no scopes — it exists so the migration
generator can own this table's schema like any other.
"""

from __future__ import annotations

from cara.eloquent.models import Model
from cara.eloquent.schema import Schema


class FailedJob(Model):
    """Dead-lettered queue jobs: payload, exception, and when they failed."""

    __table__ = "failed_job"
    __primary_key__ = "id"

    __fillable__ = [
        "driver",
        "queue",
        "name",
        "connection",
        "payload",
        "exception",
        "failed_at",
    ]

    __casts__ = {
        "created_at": "datetime",
        "failed_at": "datetime",
    }

    @property
    def fields(self):
        """Define table fields for migration auto-generation."""
        return Schema.build(
            lambda field: (
                field.big_increments("id"),
                field.string("driver", 50),
                field.string("queue", 255),
                field.string("name", 255),
                field.string("connection", 50).nullable(),
                field.text("payload"),
                field.text("exception").nullable(),
                # Nullable by design: the row is written at failure time and
                # ``failed_at`` is the meaningful instant; ``created_at`` is
                # populated when the writer passes it, not enforced.
                field.datetime("created_at").nullable(),
                field.datetime("failed_at").nullable(),
            )
        )
