"""Durable delivery ledger and transactional AMQP publish outbox."""

from __future__ import annotations

from typing import Any

from cara.exceptions import QueueException
from cara.queues.serializers import SignedJsonJobSerializer

from . import (
    _DeliveryConfiguration,
    _DeliveryDiagnostics,
    _DeliveryHooks,
    _DeliveryPublishing,
    _DeliveryRecovery,
    _DeliverySettlement,
)


class QueueJobDeliveryStore:
    """Single source of truth for queue publication and execution state."""

    STATUS_PENDING = "pending"
    STATUS_PROCESSING = "processing"
    STATUS_COMPLETED = "completed"
    STATUS_RETRY_SCHEDULED = "retry_scheduled"
    STATUS_DEAD_LETTERED = "dead_lettered"
    STATUS_EXPIRED = "expired"
    TERMINAL_STATUSES = frozenset(
        {
            STATUS_COMPLETED,
            STATUS_RETRY_SCHEDULED,
            STATUS_DEAD_LETTERED,
            STATUS_EXPIRED,
        }
    )
    HOOK_TERMINAL_STATUSES = frozenset(
        {
            STATUS_COMPLETED,
            STATUS_DEAD_LETTERED,
            STATUS_EXPIRED,
        }
    )

    PUBLISH_PENDING = "pending"
    PUBLISH_PROCESSING = "processing"
    PUBLISH_PUBLISHED = "published"
    PUBLISH_FAILED = "failed"

    #: The ONE definition of "this row is due for terminal-hook delivery".
    #: ``{now}`` is the caller's time expression — ``NOW()`` for pure-SQL
    #: aggregate reads, ``%s`` where the caller binds an application clock.
    #: Shared by :meth:`due_terminal_hook_ids` (the hooks worker's claim
    #: scan) and :meth:`outbox_health_metrics` (the scheduler-side
    #: watchdog) so the two can never disagree about what "due" means.
    _HOOK_DUE_FILTER_TEMPLATE = (
        "status = ANY(%s) AND post_hooks_completed_at IS NULL "
        "AND post_hooks_quarantined_at IS NULL "
        "AND (post_hooks_lease_token IS NULL OR "
        "post_hooks_lease_expires_at IS NULL OR "
        "post_hooks_lease_expires_at <= {now})"
    )

    #: The ONE definition of "this row is committed and past its release
    #: time but has not reached the broker". ``{now}`` is the caller's time
    #: expression, mirroring ``_HOOK_DUE_FILTER_TEMPLATE``; it binds two
    #: parameters in order — ``STATUS_PENDING`` then ``PUBLISH_PUBLISHED``.
    #:
    #: Deliberately COARSER than ``_claim_next_publish``'s claim gate: this
    #: answers "how much work has not been published", not "which row may I
    #: lease right now". Publish backoff, a live publish lease, expiry and
    #: the per-queue broker window all narrow CLAIMABILITY but must not
    #: narrow BACKLOG — an age-gated alarm that drops backing-off rows
    #: collapses ``MIN(available_at)`` to NULL and flaps between fired and
    #: resolved. Every read-model site formats this constant; the string was
    #: previously hand-copied at six of them while this method's own
    #: docstring claimed they all read one source.
    _PUBLISH_BACKLOG_FILTER_TEMPLATE = (
        "status = %s AND publish_status != %s AND available_at <= {now}"
    )

    _PUBLISH_BACKOFF_SECONDS = (1, 5, 30, 60, 300)
    _HOOK_BACKOFF_SECONDS = (60, 300, 900, 3600, 21600, 86400)
    _SETTLEMENT_BACKOFF_SECONDS = (0.05, 0.25, 1.0, 2.0, 5.0)
    _PRIORITY_RANKS = {
        "critical": 0,
        "high": 1,
        "default": 2,
        "low": 3,
    }
    _PUBLISH_CLAIM_ADVISORY_LOCK = 7_190_342_541
    DEFAULT_JOB_TIMEOUT_SECONDS = 300
    POST_HOOK_LEASE_SECONDS = 300

    def __init__(self, application: Any, driver: Any, options: dict[str, Any]):
        self.application = application
        self.driver = driver
        self.options = options
        self.table = str(options.get("delivery_table") or "queue_job_delivery")
        if self.table != "queue_job_delivery":
            raise QueueException(
                "AMQP delivery_table must be the canonical 'queue_job_delivery'."
            )
        # One-shot latch for _require_ledger_schema — see that method.
        self._ledger_schema_verified = False
        canonical_queues = getattr(driver, "_canonical_queues", None) or options.get(
            "canonical_queues"
        )
        self.canonical_queues = tuple(
            sorted(
                {
                    str(queue).strip()
                    for queue in (canonical_queues or ())
                    if str(queue).strip()
                }
            )
        )
        if not self.canonical_queues:
            raise QueueException("AMQP delivery ledger requires canonical_queues.")
        self.claim_batch = self._bounded_int(
            options.get("delivery_claim_batch", 100),
            minimum=1,
            maximum=1000,
            field="delivery_claim_batch",
        )
        self.publish_lease_seconds = self._bounded_int(
            options.get("delivery_publish_lease_seconds", 300),
            minimum=30,
            maximum=3600,
            field="delivery_publish_lease_seconds",
        )
        self.priority_aging_seconds = self._bounded_int(
            options.get("delivery_priority_aging_seconds", 300),
            minimum=30,
            maximum=86400,
            field="delivery_priority_aging_seconds",
        )
        self.broker_window_per_queue = self._bounded_int(
            options.get("delivery_broker_window_per_queue", 2),
            minimum=1,
            maximum=10000,
            field="delivery_broker_window_per_queue",
        )
        self.execution_lease_seconds = self._bounded_int(
            options.get("delivery_execution_lease_seconds", 7200),
            minimum=60,
            maximum=86400,
            field="delivery_execution_lease_seconds",
        )
        self.execution_lease_grace_seconds = self._bounded_int(
            options.get("delivery_execution_lease_grace_seconds", 300),
            minimum=30,
            maximum=3600,
            field="delivery_execution_lease_grace_seconds",
        )
        self.default_job_timeout_seconds = self._bounded_int(
            options.get(
                "delivery_default_job_timeout_seconds",
                self.DEFAULT_JOB_TIMEOUT_SECONDS,
            ),
            minimum=1,
            maximum=86399,
            field="delivery_default_job_timeout_seconds",
        )
        if (
            self.default_job_timeout_seconds + self.execution_lease_grace_seconds
            > self.execution_lease_seconds
        ):
            raise QueueException(
                "delivery_default_job_timeout_seconds plus "
                "delivery_execution_lease_grace_seconds must not exceed "
                "delivery_execution_lease_seconds."
            )
        self.audit_retention_days = self._bounded_int(
            options.get("delivery_audit_retention_days", 90),
            minimum=1,
            maximum=3650,
            field="delivery_audit_retention_days",
        )
        self.audit_safety_days = self._bounded_int(
            options.get("delivery_audit_safety_days", 7),
            minimum=1,
            maximum=365,
            field="delivery_audit_safety_days",
        )
        self.hook_timeout_seconds = self._bounded_int(
            options.get("delivery_hook_timeout_seconds", 60),
            minimum=1,
            maximum=self.POST_HOOK_LEASE_SECONDS - 1,
            field="delivery_hook_timeout_seconds",
        )
        self.hook_max_attempts = self._bounded_int(
            options.get("delivery_hook_max_attempts", 10),
            minimum=1,
            maximum=100,
            field="delivery_hook_max_attempts",
        )
        envelope_max_age_seconds = self._bounded_int(
            options.get(
                "envelope_max_age_seconds",
                SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
            ),
            minimum=300,
            maximum=10 * 365 * 24 * 60 * 60,
            field="envelope_max_age_seconds",
        )
        if self.audit_retention_days * 86400 <= (
            envelope_max_age_seconds + self.audit_safety_days * 86400
        ):
            raise QueueException(
                "delivery_audit_retention_days must exceed the envelope "
                "maximum age plus delivery_audit_safety_days."
            )

    register = _DeliveryConfiguration._delivery_register
    publish_after_commit = _DeliveryConfiguration._delivery_publish_after_commit
    replay_from_ledger = _DeliveryConfiguration._delivery_replay_from_ledger
    publish_one = _DeliveryConfiguration._delivery_publish_one
    _claim_publish = _DeliveryConfiguration._delivery_claim_publish

    _claim_next_publish = _DeliveryPublishing._delivery_claim_next_publish
    publish_due = _DeliveryPublishing._delivery_publish_due
    _publish_claimed = _DeliveryPublishing._delivery_publish_claimed
    _release_publish = _DeliveryPublishing._delivery_release_publish
    _quarantine_publish = _DeliveryPublishing._delivery_quarantine_publish
    claim_execution = _DeliveryPublishing._delivery_claim_execution
    _reconcile_broker_receipt = _DeliveryPublishing._delivery_reconcile_broker_receipt
    complete = _DeliveryPublishing._delivery_complete
    complete_with_tracker = _DeliveryPublishing._delivery_complete_with_tracker
    dead_letter = _DeliveryPublishing._delivery_dead_letter

    dead_letter_with_tracker = _DeliverySettlement._delivery_dead_letter_with_tracker
    _settle_execution_with_tracker = (
        _DeliverySettlement._delivery_settle_execution_with_tracker
    )
    reconcile_terminal_tracker = _DeliverySettlement._delivery_reconcile_terminal_tracker
    mark_retry_scheduled = _DeliverySettlement._delivery_mark_retry_scheduled
    abandon_execution = _DeliverySettlement._delivery_abandon_execution
    claim_terminal_hooks = _DeliverySettlement._delivery_claim_terminal_hooks
    process_terminal_hooks = _DeliverySettlement._delivery_process_terminal_hooks
    complete_terminal_hooks = _DeliverySettlement._delivery_complete_terminal_hooks
    defer_terminal_hooks = _DeliverySettlement._delivery_defer_terminal_hooks
    defer_terminal_hook_process_failure = (
        _DeliverySettlement._delivery_defer_terminal_hook_process_failure
    )
    _defer_terminal_hook = _DeliverySettlement._delivery_defer_terminal_hook

    due_terminal_hook_ids = _DeliveryHooks._delivery_due_terminal_hook_ids
    retry_quarantined_terminal_hooks = (
        _DeliveryHooks._delivery_retry_quarantined_terminal_hooks
    )
    _settle_with_retry = _DeliveryHooks._delivery_settle_with_retry
    _settle = _DeliveryHooks._delivery_settle
    expire_due = _DeliveryHooks._delivery_expire_due
    recover_stale_executions = _DeliveryHooks._delivery_recover_stale_executions
    prune_terminal = _DeliveryHooks._delivery_prune_terminal
    execution_timeout_for = _DeliveryHooks._delivery_execution_timeout_for

    _expire_job_if_due = _DeliveryRecovery._delivery_expire_job_if_due
    _expire_publish = _DeliveryRecovery._delivery_expire_publish
    backlog_metrics_if_installed = (
        _DeliveryRecovery._delivery_backlog_metrics_if_installed
    )
    backlog_metrics = _DeliveryRecovery._delivery_backlog_metrics
    outbox_health_metrics_if_installed = (
        _DeliveryRecovery._delivery_outbox_health_metrics_if_installed
    )
    outbox_health_metrics = _DeliveryRecovery._delivery_outbox_health_metrics
    delivery_stats = _DeliveryRecovery._delivery_delivery_stats

    delivery_metrics = _DeliveryDiagnostics._delivery_delivery_metrics
    verify_schema = _DeliveryDiagnostics._delivery_verify_schema
    _mark_tracker_failed = staticmethod(
        _DeliveryDiagnostics._delivery_mark_tracker_failed
    )
    _set_tracker_status = staticmethod(_DeliveryDiagnostics._delivery_set_tracker_status)
    _db = _DeliveryDiagnostics._delivery_db
    _require_ledger_schema = _DeliveryDiagnostics._delivery_require_ledger_schema
    _tenant_scope = staticmethod(_DeliveryDiagnostics._delivery_tenant_scope)
    _safe_error = staticmethod(_DeliveryDiagnostics._delivery_safe_error)
    _safe_persisted_text = staticmethod(
        _DeliveryDiagnostics._delivery_safe_persisted_text
    )
    _envelope_bytes = staticmethod(_DeliveryDiagnostics._delivery_envelope_bytes)
    _row_value = staticmethod(_DeliveryDiagnostics._delivery_row_value)
    _affected = staticmethod(_DeliveryDiagnostics._delivery_affected)
    _as_datetime = staticmethod(_DeliveryDiagnostics._delivery_as_datetime)
    _bounded_int = staticmethod(_DeliveryDiagnostics._delivery_bounded_int)
    _bounded_text = staticmethod(_DeliveryDiagnostics._delivery_bounded_text)


_DeliveryConfiguration._bind_store(QueueJobDeliveryStore)
_DeliveryPublishing._bind_store(QueueJobDeliveryStore)
_DeliverySettlement._bind_store(QueueJobDeliveryStore)
_DeliveryHooks._bind_store(QueueJobDeliveryStore)
_DeliveryRecovery._bind_store(QueueJobDeliveryStore)
_DeliveryDiagnostics._bind_store(QueueJobDeliveryStore)
