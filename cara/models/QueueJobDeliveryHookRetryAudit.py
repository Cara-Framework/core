"""Operator audit rows for post-hook retries on the delivery ledger.

When an operator replays a quarantined post-hook run (``queue:retry``), the
store records who asked, why, and what the failure state was at that moment.
The parent ledger row keeps only the LATEST hook state; this table is the
history that survives the retry overwriting it.

Schema declaration only — writes happen in
``cara.queues.delivery.QueueJobDeliveryStore`` alongside the CAS update that
re-opens the hook lease, in the same transaction.
"""

from __future__ import annotations

from cara.eloquent.models import Model
from cara.eloquent.schema import Schema


class QueueJobDeliveryHookRetryAudit(Model):
    """Who replayed a quarantined post-hook run, when, and from what state."""

    __table__ = "queue_job_delivery_hook_retry_audit"
    __primary_key__ = "id"

    __fillable__ = []

    __casts__ = {
        "prior_attempts": "integer",
        "requested_at": "datetime",
    }

    @property
    def fields(self):
        """Define table fields for migration auto-generation."""
        return Schema.build(
            lambda field: (
                field.big_increments("id"),
                field.string("job_id", 64),
                field.string("requested_by", 200),
                field.string("reason", 1000),
                field.integer("prior_attempts"),
                field.text("prior_error"),
                field.datetime("requested_at"),
            )
        )

    __indexes__ = [
        {
            "name": "queue_job_delivery_hook_retry_attempts_check",
            "up": (
                "ALTER TABLE queue_job_delivery_hook_retry_audit "
                "ADD CONSTRAINT queue_job_delivery_hook_retry_attempts_check "
                "CHECK (prior_attempts >= 0)"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery_hook_retry_audit "
                "DROP CONSTRAINT IF EXISTS "
                "queue_job_delivery_hook_retry_attempts_check"
            ),
        },
        {
            "name": "queue_job_delivery_hook_retry_job_foreign",
            "up": (
                "ALTER TABLE queue_job_delivery_hook_retry_audit "
                "ADD CONSTRAINT queue_job_delivery_hook_retry_job_foreign "
                "FOREIGN KEY (job_id) REFERENCES queue_job_delivery(job_id) "
                "ON DELETE CASCADE"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery_hook_retry_audit "
                "DROP CONSTRAINT IF EXISTS "
                "queue_job_delivery_hook_retry_job_foreign"
            ),
        },
        {
            "name": "queue_job_delivery_hook_retry_job_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_hook_retry_job_idx "
                "ON queue_job_delivery_hook_retry_audit (job_id, requested_at)"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_hook_retry_job_idx",
        },
    ]
