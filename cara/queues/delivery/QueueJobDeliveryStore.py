"""Durable delivery ledger and transactional AMQP publish outbox."""

from __future__ import annotations

from typing import Any

from cara.configuration import config
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
    #: Terminal statuses with no terminal callback to run: ``failed()`` fires
    #: only for dead-lettered and expired work. Every write that settles a
    #: row into one of them stamps ``post_hooks_completed_at`` in the same
    #: statement, so the hooks runner — one isolated interpreter boot per
    #: claimed row — never spends a boot on a row with nothing to run, real
    #: ``failed()`` hooks never queue behind them, and retention prunes them
    #: on schedule.
    HOOKLESS_TERMINAL_STATUSES = frozenset(
        {
            STATUS_COMPLETED,
            STATUS_RETRY_SCHEDULED,
        }
    )

    PUBLISH_PENDING = "pending"
    PUBLISH_PROCESSING = "processing"
    PUBLISH_PUBLISHED = "published"
    PUBLISH_FAILED = "failed"

    #: How many executions of ONE ledger row may end with the worker dead
    #: before the row is settled ``dead_lettered`` instead of re-run. This is
    #: deliberately not the job's ``max_attempts``: that budget is spent by
    #: failures the worker lives to route and rides the envelope, across
    #: retry rows, while ``attempts`` here counts one row's executions. A
    #: graceful shutdown hands its claim back (:meth:`abandon_execution`), so
    #: a deploy costs nothing; only a real death counts. Past the ceiling
    #: ``failed()`` runs, and the hourly dead-letter pass replays the row
    #: once, which is what gives a row that merely shared a worker with the
    #: real killer its next chance.
    CRASH_RECLAIM_CEILING = 3
    CRASH_LOOP_REASON = f"crash_loop: worker died in {CRASH_RECLAIM_CEILING} executions"

    #: How many stall budgets an aged row may wait on a queue that is still
    #: moving before it counts as stuck anyway: bounded excuse, not a mute.
    DRAINING_AGE_MULTIPLE = 6

    #: Alternates this relay's claims between fair order and oldest-overdue
    #: first (``_claim_next_publish``). Process-local on purpose: each relay
    #: replica alternating on its own still guarantees the oldest rows half
    #: of its claims, and losing it costs nothing.
    _serve_oldest_next = False

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

    #: The ONE definition of "this terminal row awaits replay": no replay
    #: child names it. ``{table}`` is the ledger table, unaliased in the outer
    #: query. A replay inserts a child and never touches its source, so a
    #: recovered source keeps ``dead_lettered`` until retention prunes it.
    #: Read by the operator tools (``QueueOperationsStore``) and the ledger
    #: gauge alerting reads (:meth:`delivery_metrics`).
    _AWAITING_REPLAY_FILTER_TEMPLATE = (
        "NOT EXISTS (SELECT 1 FROM {table} AS replay_child "
        "WHERE replay_child.replay_of = {table}.job_id)"
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
        # The most broker-window slots ONE tenant may hold in one queue
        # (0 = no per-tenant cap; only the fair claim order applies). Kept
        # below the broker window, it leaves a slot that another tenant's
        # newly arrived job takes at once instead of waiting for a
        # backlogged tenant's in-flight work to finish.
        self.tenant_window_per_queue = self._bounded_int(
            options.get("delivery_tenant_window_per_queue", 0),
            minimum=0,
            maximum=10000,
            field="delivery_tenant_window_per_queue",
        )
        # Slots ABOVE the tenant window that only a tenant's URGENT rows —
        # queued ``high`` or ``critical`` — may take (0 = off). A tenant whose
        # long default/low jobs fill its tenant window otherwise starts its
        # own urgent job only after one of them ends: a sale's stock push
        # waited minutes behind the same seller's scheduled pulls. The
        # DECLARED priority decides, never the aged tier, so a backlog that
        # aged up cannot fill these slots too. Default/low rows still stop at
        # the tenant window, and the broker window still bounds the queue.
        self.tenant_urgent_slots_per_queue = self._bounded_int(
            options.get("delivery_tenant_urgent_slots_per_queue", 0),
            minimum=0,
            maximum=10000,
            field="delivery_tenant_urgent_slots_per_queue",
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
        # The execution lease a worker holds is a HEARTBEAT lease of this
        # length, renewed every third of it while the job runs
        # (``renew_execution``). ``timeout + grace`` is only its ceiling. A
        # claim used to take the whole ``timeout + grace`` at once, so a
        # worker that died mid-job left its row ``processing`` — holding a
        # slot of its queue's broker window — for up to 65 minutes (live,
        # 2026-10-30); now the row returns to the outbox within this lease.
        self.execution_heartbeat_seconds = self._bounded_int(
            options.get("delivery_execution_heartbeat_seconds", 120),
            minimum=30,
            maximum=3600,
            field="delivery_execution_heartbeat_seconds",
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

    def outbox_health_metrics(self) -> dict[str, float]:
        """The fleet-wide snapshot plus whether the aged rows are moving.

        ``draining`` (1.0) means rows are past the stall budget but every
        queue holding them published within it: a burst is working through a
        bounded broker window at the speed of its workers, and the relay
        is doing exactly what it should. Only a publication counts as motion:
        a completion frees a slot a live relay fills within seconds, so
        workers still finishing a window say nothing for a dead relay.
        ``behind_full_window`` counts the rows of the queues that did NOT
        move and whose window is full, so the alert names the workers
        rather than the relay.

        A queue can also stand below that window on purpose. On a stuck queue
        with broker room, ``behind_tenant_window`` counts the aged rows
        ``_claim_next_publish`` refuses because their seller already holds its
        whole tenant window (the same slot, urgent-slot and central-row rules),
        and ``behind_moving_queue`` the rows of a queue that published within
        the budget: a backlog past the age ceiling. Neither is a dead relay.
        ``behind_no_window`` is what is left on a queue that did not move: aged
        rows the claim would have published, so nothing but the relay explains
        them, whatever one seller's window explains beside them.
        """
        snapshot = _DeliveryRecovery._delivery_outbox_health_metrics(self)
        budget = max(int(config("queue.outbox_stall_age_seconds", 300)), 1)
        due = self._PUBLISH_BACKLOG_FILTER_TEMPLATE.format(now="NOW()")
        row = self._db().select_one(
            "WITH aged AS (SELECT queue, COUNT(*) AS waiting, COUNT(*) FILTER "
            "(WHERE available_at < NOW() - make_interval(secs => %s)) AS overdue, "
            f"MIN(available_at) AS oldest FROM {self.table} WHERE {due} "
            "GROUP BY queue HAVING MIN(available_at) "
            "< NOW() - make_interval(secs => %s)), "
            "held AS (SELECT slot.queue, COALESCE(slot.tenant_id, 0) AS tenant_key, "
            f"COUNT(*) AS slots FROM {self.table} AS slot "
            "WHERE slot.queue IN (SELECT queue FROM aged) "
            "AND slot.status IN ('pending', 'processing') "
            "AND (slot.publish_status = 'published' "
            "OR (slot.publish_status = 'processing' "
            "AND slot.publish_lease_expires_at > NOW())) "
            "GROUP BY slot.queue, COALESCE(slot.tenant_id, 0)), "
            "judged AS (SELECT aged.queue, aged.waiting, aged.overdue, "
            "aged.oldest < NOW() - make_interval(secs => %s) AS past_ceiling, "
            f"EXISTS (SELECT 1 FROM {self.table} AS moved "
            "WHERE moved.queue = aged.queue "
            "AND moved.published_at > NOW() - make_interval(secs => %s)) AS moved, "
            "COALESCE((SELECT SUM(held.slots) FROM held "
            "WHERE held.queue = aged.queue), 0) >= %s AS window_full FROM aged), "
            "refused AS (SELECT candidate.queue, COUNT(*) AS denied "
            f"FROM {self.table} AS candidate JOIN held "
            "ON held.queue = candidate.queue AND held.tenant_key = candidate.tenant_id "
            f"WHERE {due} AND available_at < NOW() - make_interval(secs => %s) "
            "AND %s > 0 AND held.slots >= %s + CASE WHEN candidate.priority "
            "IN ('critical', 'high') THEN %s ELSE 0 END GROUP BY candidate.queue) "
            "SELECT COUNT(*) FILTER (WHERE past_ceiling OR NOT moved) AS stuck_queues, "
            "COUNT(*) AS aged_queues, COALESCE(SUM(waiting) FILTER "
            "(WHERE (past_ceiling OR NOT moved) AND window_full), 0) "
            "AS behind_full_window, COALESCE(SUM(denied) FILTER "
            "(WHERE (past_ceiling OR NOT moved) AND NOT window_full), 0) "
            "AS behind_tenant_window, COALESCE(SUM(waiting) FILTER "
            "(WHERE past_ceiling AND moved AND NOT window_full), 0) "
            "AS behind_moving_queue, COALESCE(SUM(overdue - COALESCE(denied, 0)) "
            "FILTER (WHERE NOT moved AND NOT window_full), 0) AS behind_no_window "
            "FROM judged LEFT JOIN refused USING (queue)",
            [
                budget,
                self.STATUS_PENDING,
                self.PUBLISH_PUBLISHED,
                budget,
                budget * self.DRAINING_AGE_MULTIPLE,
                budget,
                self.broker_window_per_queue,
                self.STATUS_PENDING,
                self.PUBLISH_PUBLISHED,
                budget,
                self.tenant_window_per_queue,
                self.tenant_window_per_queue,
                self.tenant_urgent_slots_per_queue,
            ],
        )
        aged_queues = int(self._row_value(row, "aged_queues") or 0)
        stuck_queues = int(self._row_value(row, "stuck_queues") or 0)
        return {
            **snapshot,
            "draining": 1.0 if aged_queues and not stuck_queues else 0.0,
            "behind_full_window": float(self._row_value(row, "behind_full_window") or 0),
            "behind_tenant_window": float(
                self._row_value(row, "behind_tenant_window") or 0
            ),
            "behind_moving_queue": float(
                self._row_value(row, "behind_moving_queue") or 0
            ),
            "behind_no_window": float(self._row_value(row, "behind_no_window") or 0),
        }

    def abandon_execution(self, job_id: str, lease_token: str) -> None:
        """Release an interrupted lease and hand its claim back.

        Graceful release is not a crash, so it must not spend the row's
        :attr:`CRASH_RECLAIM_CEILING`.
        """
        database = self._db()
        with database.transaction():
            # Before the release and on the lease itself: the release also
            # answers a row already released, which has nothing to hand back.
            database.statement(
                f"UPDATE {self.table} SET attempts = attempts - 1 "
                "WHERE job_id = %s AND status = %s AND lease_token = %s "
                "AND attempts > 0",
                [job_id, self.STATUS_PROCESSING, lease_token],
            )
            _DeliverySettlement._delivery_abandon_execution(self, job_id, lease_token)

    @staticmethod
    def _tracker_is_open(database: Any, db_job_id: int) -> bool:
        """Whether a crashed row's tracker still says its body never finished."""
        row = database.select_one("SELECT status FROM job WHERE id = %s", [db_job_id])
        return QueueJobDeliveryStore._row_value(row, "status") in {
            "pending",
            "processing",
            "retrying",
        }

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
    renew_execution = _DeliveryPublishing._delivery_renew_execution
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
