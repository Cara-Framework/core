"""The queue delivery ledger, declared as a model.

``queue_job_delivery`` is the durable outbox and execution ledger behind
``cara.queues.delivery.QueueJobDeliveryStore``: one row per dispatched job,
carrying both the publish half (outbox → AMQP, leased by the relay) and the
execution half (consumer lease → terminal state → post hooks). The relay, the
workers and the hooks runner all synchronise exclusively through this table's
compare-and-set lease columns.

The class is a SCHEMA DECLARATION. Every write goes through the store's raw
SQL — the lease protocol is expressed in single-statement CAS updates that
model saves cannot represent — and nothing may route writes through the ORM.
It exists so ``make:migration`` generates this table like any other, instead
of every product hand-maintaining a private copy of ~230 lines of DDL behind
an escape marker.

State mached here, enforced below as CHECK constraints:

* ``status``: pending → processing → completed / retry_scheduled /
  dead_lettered / expired. A processing row holds a lease pair; a terminal
  row holds ``completed_at`` and no lease.
* ``publish_status``: pending → processing → published / failed, with the
  same lease-pair discipline on the publish columns.
* ``tenant_mode``: ``central`` rows carry no tenant, ``tenant`` rows a
  positive one — the queue-tenancy envelope contract.
* Replays: a replay row names its source, requester and reason together, and
  at most one replay may exist per source row.
"""

from __future__ import annotations

from cara.eloquent.models import Model
from cara.eloquent.schema import Schema


class QueueJobDelivery(Model):
    """Durable publish + execution ledger for every dispatched queue job."""

    __table__ = "queue_job_delivery"
    __primary_key__ = "job_id"

    __fillable__ = []

    __casts__ = {
        "db_job_id": "integer",
        "signed_envelope": "json",
        "tenant_id": "integer",
        "attempts": "integer",
        "post_hooks_attempts": "integer",
        "publish_attempts": "integer",
        "lease_expires_at": "datetime",
        "completed_at": "datetime",
        "post_hooks_completed_at": "datetime",
        "post_hooks_lease_expires_at": "datetime",
        "post_hooks_quarantined_at": "datetime",
        "expires_at": "datetime",
        "available_at": "datetime",
        "publish_retry_at": "datetime",
        "publish_lease_expires_at": "datetime",
        "published_at": "datetime",
        "created_at": "datetime",
        "updated_at": "datetime",
    }

    @property
    def fields(self):
        """Define table fields for migration auto-generation."""
        return Schema.build(
            lambda field: (
                field.string("job_id", 64),
                field.big_integer("db_job_id"),
                field.string("replay_of", 64).nullable(),
                field.string("replay_requested_by", 200).nullable(),
                field.string("replay_reason", 1000).nullable(),
                field.char("payload_sha256", 64),
                field.jsonb("signed_envelope"),
                field.string("tenant_mode", 16),
                field.big_integer("tenant_id").nullable(),
                field.string("queue", 100),
                field.string("priority", 16),
                field.string("status", 24).default("pending"),
                field.integer("attempts").default(0),
                field.string("lease_token", 64).nullable(),
                field.datetime("lease_expires_at").nullable(),
                field.datetime("completed_at").nullable(),
                field.text("terminal_reason").nullable(),
                field.datetime("post_hooks_completed_at").nullable(),
                field.string("post_hooks_lease_token", 64).nullable(),
                field.datetime("post_hooks_lease_expires_at").nullable(),
                field.integer("post_hooks_attempts").default(0),
                field.datetime("post_hooks_quarantined_at").nullable(),
                field.text("post_hooks_last_error").nullable(),
                field.datetime("expires_at"),
                field.datetime("available_at"),
                field.string("publish_status", 16).default("pending"),
                field.integer("publish_attempts").default(0),
                field.datetime("publish_retry_at"),
                field.string("publish_lease_token", 64).nullable(),
                field.datetime("publish_lease_expires_at").nullable(),
                field.datetime("published_at").nullable(),
                field.text("last_publish_error").nullable(),
                # Durable UniqueJob key: at most one OPEN row per
                # (tenant scope, key) — refereed by the partial unique
                # index below, not by a plain UNIQUE, because terminal
                # rows must not block a re-dispatch.
                field.string("unique_key", 512).nullable(),
                field.datetime("created_at"),
                field.datetime("updated_at"),
            )
        )

    # Constraint and index DDL the field DSL cannot state inline: named CHECK
    # state machines, the two RESTRICT foreign keys, and the partial/INCLUDE
    # indexes the store's hot paths scan. Names are load-bearing — the store's
    # error handling and the operators' runbooks reference them.
    __indexes__ = [
        {
            "name": "queue_job_delivery_status_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_status_check CHECK "
                "(status IN ('pending','processing','completed',"
                "'retry_scheduled','dead_lettered','expired'))"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_status_check"
            ),
        },
        {
            "name": "queue_job_delivery_publish_status_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_publish_status_check CHECK "
                "(publish_status IN ('pending','processing','published','failed'))"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_publish_status_check"
            ),
        },
        {
            "name": "queue_job_delivery_attempts_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_attempts_check "
                "CHECK (attempts >= 0)"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_attempts_check"
            ),
        },
        {
            "name": "queue_job_delivery_priority_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_priority_check CHECK "
                "(priority IN ('critical','high','default','low'))"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_priority_check"
            ),
        },
        {
            "name": "queue_job_delivery_publish_attempts_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_publish_attempts_check "
                "CHECK (publish_attempts >= 0)"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_publish_attempts_check"
            ),
        },
        {
            "name": "queue_job_delivery_post_hook_attempts_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_post_hook_attempts_check "
                "CHECK (post_hooks_attempts >= 0)"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_post_hook_attempts_check"
            ),
        },
        {
            "name": "queue_job_delivery_execution_state_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_execution_state_check CHECK ("
                "(status = 'processing' AND completed_at IS NULL "
                "AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL) OR "
                "(status = 'pending' AND completed_at IS NULL "
                "AND lease_token IS NULL AND lease_expires_at IS NULL) OR "
                "(status IN ('completed','retry_scheduled','dead_lettered','expired') "
                "AND completed_at IS NOT NULL "
                "AND lease_token IS NULL AND lease_expires_at IS NULL)"
                ")"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_execution_state_check"
            ),
        },
        {
            "name": "queue_job_delivery_publish_state_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_publish_state_check CHECK ("
                "(publish_status = 'processing' AND published_at IS NULL "
                "AND publish_lease_token IS NOT NULL "
                "AND publish_lease_expires_at IS NOT NULL) OR "
                "(publish_status = 'published' AND published_at IS NOT NULL "
                "AND publish_lease_token IS NULL "
                "AND publish_lease_expires_at IS NULL) OR "
                "(publish_status IN ('pending','failed') "
                "AND published_at IS NULL "
                "AND publish_lease_token IS NULL "
                "AND publish_lease_expires_at IS NULL)"
                ")"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_publish_state_check"
            ),
        },
        {
            "name": "queue_job_delivery_tenant_scope_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_tenant_scope_check CHECK ("
                "(tenant_mode = 'central' AND tenant_id IS NULL) OR "
                "(tenant_mode = 'tenant' AND tenant_id > 0)"
                ")"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_tenant_scope_check"
            ),
        },
        {
            "name": "queue_job_delivery_expiry_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_expiry_check "
                "CHECK (expires_at > available_at)"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_expiry_check"
            ),
        },
        {
            "name": "queue_job_delivery_db_job_id_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_db_job_id_check "
                "CHECK (db_job_id > 0)"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_db_job_id_check"
            ),
        },
        {
            "name": "queue_job_delivery_replay_not_self_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_replay_not_self_check "
                "CHECK (replay_of IS NULL OR replay_of <> job_id)"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_replay_not_self_check"
            ),
        },
        {
            "name": "queue_job_delivery_post_hook_lease_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_post_hook_lease_check CHECK ("
                "(post_hooks_lease_token IS NULL AND "
                "post_hooks_lease_expires_at IS NULL) OR "
                "(post_hooks_lease_token IS NOT NULL AND "
                "post_hooks_lease_expires_at IS NOT NULL "
                "AND status IN ('completed','dead_lettered','expired') "
                "AND post_hooks_completed_at IS NULL "
                "AND post_hooks_quarantined_at IS NULL)"
                ")"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_post_hook_lease_check"
            ),
        },
        {
            "name": "queue_job_delivery_post_hook_completed_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_post_hook_completed_check CHECK ("
                "post_hooks_completed_at IS NULL OR "
                "(post_hooks_lease_token IS NULL AND "
                "post_hooks_lease_expires_at IS NULL AND "
                "post_hooks_quarantined_at IS NULL)"
                ")"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_post_hook_completed_check"
            ),
        },
        {
            "name": "queue_job_delivery_post_hook_quarantine_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_post_hook_quarantine_check CHECK ("
                "post_hooks_quarantined_at IS NULL OR "
                "(post_hooks_completed_at IS NULL AND "
                "post_hooks_lease_token IS NULL AND "
                "post_hooks_lease_expires_at IS NULL)"
                ")"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_post_hook_quarantine_check"
            ),
        },
        {
            "name": "queue_job_delivery_replay_audit_check",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_replay_audit_check CHECK ("
                "(replay_of IS NULL AND replay_requested_by IS NULL "
                "AND replay_reason IS NULL) OR "
                "(replay_of IS NOT NULL AND replay_requested_by IS NOT NULL "
                "AND replay_reason IS NOT NULL)"
                ")"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_replay_audit_check"
            ),
        },
        {
            "name": "queue_job_delivery_db_job_id_foreign",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_db_job_id_foreign FOREIGN KEY "
                "(db_job_id) REFERENCES job(id) ON DELETE RESTRICT"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_db_job_id_foreign"
            ),
        },
        {
            "name": "queue_job_delivery_replay_of_foreign",
            "up": (
                "ALTER TABLE queue_job_delivery "
                "ADD CONSTRAINT queue_job_delivery_replay_of_foreign FOREIGN KEY "
                "(replay_of) REFERENCES queue_job_delivery(job_id) "
                "ON DELETE RESTRICT"
            ),
            "down": (
                "ALTER TABLE queue_job_delivery "
                "DROP CONSTRAINT IF EXISTS queue_job_delivery_replay_of_foreign"
            ),
        },
        {
            "name": "queue_job_delivery_publish_due_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_publish_due_idx "
                "ON queue_job_delivery "
                "(publish_status, publish_retry_at, available_at)"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_publish_due_idx",
        },
        {
            "name": "queue_job_delivery_priority_head_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_priority_head_idx "
                "ON queue_job_delivery "
                "(queue, priority, available_at, created_at) "
                "WHERE status = 'pending' "
                "AND publish_status IN ('pending','processing')"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_priority_head_idx",
        },
        {
            "name": "queue_job_delivery_broker_window_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_broker_window_idx "
                "ON queue_job_delivery "
                "(queue, publish_status, publish_lease_expires_at) "
                "WHERE status IN ('pending','processing') "
                "AND publish_status IN ('processing','published')"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_broker_window_idx",
        },
        {
            "name": "queue_job_delivery_execution_lease_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_execution_lease_idx "
                "ON queue_job_delivery (status, lease_expires_at)"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_execution_lease_idx",
        },
        {
            "name": "queue_job_delivery_active_stats_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_active_stats_idx "
                "ON queue_job_delivery (queue, status) INCLUDE "
                "(available_at, publish_status, publish_lease_expires_at, "
                "lease_expires_at) WHERE status IN ('pending','processing')"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_active_stats_idx",
        },
        {
            "name": "queue_job_delivery_expiry_status_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_expiry_status_idx "
                "ON queue_job_delivery (expires_at, status)"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_expiry_status_idx",
        },
        {
            "name": "queue_job_delivery_terminal_retention_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_terminal_retention_idx "
                "ON queue_job_delivery (status, completed_at) "
                "WHERE completed_at IS NOT NULL"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_terminal_retention_idx",
        },
        {
            "name": "queue_job_delivery_queue_terminal_stats_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS "
                "queue_job_delivery_queue_terminal_stats_idx "
                "ON queue_job_delivery (queue, completed_at, status) "
                "WHERE completed_at IS NOT NULL"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_queue_terminal_stats_idx",
        },
        {
            "name": "queue_job_delivery_post_hook_due_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_post_hook_due_idx "
                "ON queue_job_delivery "
                "(post_hooks_lease_expires_at, completed_at) "
                "WHERE post_hooks_completed_at IS NULL "
                "AND post_hooks_quarantined_at IS NULL "
                "AND status IN ('completed','dead_lettered','expired')"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_post_hook_due_idx",
        },
        {
            "name": "queue_job_delivery_post_hook_stats_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_post_hook_stats_idx "
                "ON queue_job_delivery (queue, post_hooks_quarantined_at) "
                "WHERE post_hooks_completed_at IS NULL "
                "AND status IN ('completed','dead_lettered','expired')"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_post_hook_stats_idx",
        },
        {
            "name": "queue_job_delivery_db_job_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_db_job_idx "
                "ON queue_job_delivery (db_job_id)"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_db_job_idx",
        },
        {
            "name": "queue_job_delivery_tenant_audit_idx",
            "up": (
                "CREATE INDEX IF NOT EXISTS queue_job_delivery_tenant_audit_idx "
                "ON queue_job_delivery (tenant_id, created_at) "
                "WHERE tenant_mode = 'tenant'"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_tenant_audit_idx",
        },
        {
            "name": "queue_job_delivery_replay_idx",
            "up": (
                "CREATE UNIQUE INDEX IF NOT EXISTS queue_job_delivery_replay_idx "
                "ON queue_job_delivery (replay_of) WHERE replay_of IS NOT NULL"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_replay_idx",
        },
        {
            "name": "queue_job_delivery_open_unique_key_idx",
            "up": (
                "CREATE UNIQUE INDEX IF NOT EXISTS "
                "queue_job_delivery_open_unique_key_idx "
                "ON queue_job_delivery "
                "(tenant_mode, COALESCE(tenant_id, 0), unique_key) "
                "WHERE unique_key IS NOT NULL "
                "AND status IN ('pending','processing')"
            ),
            "down": "DROP INDEX IF EXISTS queue_job_delivery_open_unique_key_idx",
        },
    ]
