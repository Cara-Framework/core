"""Execution settlement and terminal-hook deferral operations."""

from __future__ import annotations

import asyncio
import inspect
import time
import uuid
from typing import Any

import pendulum

from cara.context import Tenancy
from cara.exceptions import QueueException
from cara.facades import Log
from cara.queues.JobInstantiation import instantiate_job
from cara.queues.serializers import SignedJsonJobSerializer

from .DeliveryLeaseLost import DeliveryLeaseLost
from .TerminalHookClaim import TerminalHookClaim

QueueJobDeliveryStore: type


def _bind_store(store_type: type) -> None:
    global QueueJobDeliveryStore
    QueueJobDeliveryStore = store_type


def _delivery_dead_letter_with_tracker(
    self,
    job_id: str,
    lease_token: str,
    *,
    db_job_id: int,
    reason: str,
) -> None:
    self._settle_execution_with_tracker(
        job_id,
        lease_token,
        db_job_id=db_job_id,
        status=self.STATUS_DEAD_LETTERED,
        tracker_status="failed",
        reason=reason,
    )


def _delivery_settle_execution_with_tracker(
    self,
    job_id: str,
    lease_token: str,
    *,
    db_job_id: int,
    status: str,
    tracker_status: str,
    reason: str | None = None,
) -> None:
    last_error: Exception | None = None
    for attempt, delay in enumerate(
        (0.0, *self._SETTLEMENT_BACKOFF_SECONDS),
        start=1,
    ):
        if delay:
            time.sleep(delay)
        try:
            database = self._db()
            with database.transaction():
                self._settle(
                    job_id,
                    lease_token,
                    status,
                    db=database,
                    reason=reason,
                )
                self._set_tracker_status(
                    database,
                    db_job_id,
                    tracker_status,
                )
            return
        except DeliveryLeaseLost:
            raise
        except Exception as exc:
            last_error = exc
            if attempt <= len(self._SETTLEMENT_BACKOFF_SECONDS):
                Log.warning(
                    "Queue delivery %s atomic terminal settlement attempt "
                    "%s failed; retrying: %s",
                    job_id,
                    attempt,
                    exc,
                    category="cara.queue.delivery",
                )
    raise QueueException(
        f"Queue delivery {job_id} atomic terminal settlement remained unavailable."
    ) from last_error


def _delivery_reconcile_terminal_tracker(
    self,
    job_id: str,
    *,
    db_job_id: int,
    delivery_status: str,
) -> None:
    tracker_status = "completed" if delivery_status == self.STATUS_COMPLETED else "failed"
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"SELECT status, db_job_id FROM {self.table} WHERE job_id = %s FOR UPDATE",
            [job_id],
        )
        if (
            row is None
            or self._row_value(row, "status") != delivery_status
            or self._row_value(row, "db_job_id") != db_job_id
        ):
            raise QueueException(
                f"Queue delivery {job_id} terminal tracker recovery "
                "does not match the ledger."
            )
        self._set_tracker_status(
            database,
            db_job_id,
            tracker_status,
        )


def _delivery_mark_retry_scheduled(
    self,
    job_id: str,
    lease_token: str,
    *,
    db: Any | None = None,
) -> None:
    self._settle(
        job_id,
        lease_token,
        self.STATUS_RETRY_SCHEDULED,
        db=db or self._db(),
    )


def _delivery_abandon_execution(self, job_id: str, lease_token: str) -> None:
    """Hand one interrupted execution back to the publication outbox.

    The broker copy cannot be counted on: a replica that met this run's live
    lease acknowledged it away on the promise that the relay owns the retry,
    and a row left ``published`` is one the relay will not touch, so it stood
    for two hours holding a broker-window slot with no copy anywhere.

    Back in the outbox the relay restores it, but only after one heartbeat
    lease, the wait a dead worker's row gets. The process releasing this row
    may still be running the body (a job that ignored cancellation dies at
    the forced exit), and the row must not start elsewhere before it is gone.
    A broker copy that did survive returns when that process's connection
    closes and is claimed at once; its receipt takes the row out of the
    outbox, so the usual shutdown still leaves one copy of the job.
    """
    now = pendulum.now("UTC")
    affected = self._db().statement(
        f"UPDATE {self.table} SET status = %s, lease_token = NULL, "
        "lease_expires_at = NULL, publish_status = %s, publish_retry_at = %s, "
        "published_at = NULL, publish_lease_token = NULL, "
        "publish_lease_expires_at = NULL, updated_at = %s "
        "WHERE job_id = %s AND status = %s AND lease_token = %s",
        [
            self.STATUS_PENDING,
            self.PUBLISH_PENDING,
            now.add(seconds=self.execution_heartbeat_seconds),
            now,
            job_id,
            self.STATUS_PROCESSING,
            lease_token,
        ],
    )
    if not self._affected(affected):
        row = self._db().select_one(
            f"SELECT status, lease_token FROM {self.table} WHERE job_id = %s",
            [job_id],
        )
        if (
            self._row_value(row, "status") == self.STATUS_PENDING
            and self._row_value(row, "lease_token") is None
        ):
            return
        raise DeliveryLeaseLost(
            f"Queue delivery {job_id} lost its interrupted execution lease."
        )


def _delivery_claim_terminal_hooks(self, job_id: str) -> TerminalHookClaim:
    """CAS-claim one durable terminal-hook outbox row."""
    now = pendulum.now("UTC")
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"SELECT job_id, status, terminal_reason, signed_envelope, "
            "post_hooks_completed_at, post_hooks_lease_token, "
            "post_hooks_lease_expires_at, post_hooks_attempts, "
            f"post_hooks_quarantined_at FROM {self.table} "
            "WHERE job_id = %s FOR UPDATE",
            [job_id],
        )
        if row is None:
            raise QueueException(
                f"Queue delivery {job_id} is absent during terminal hooks."
            )
        status = str(self._row_value(row, "status"))
        if status not in self.HOOK_TERMINAL_STATUSES:
            raise QueueException(f"Queue delivery {job_id} is not hook-eligible.")
        if self._row_value(row, "post_hooks_completed_at") is not None:
            return TerminalHookClaim("completed", status=status)
        if self._row_value(row, "post_hooks_quarantined_at") is not None:
            return TerminalHookClaim("quarantined", status=status)

        lease_expiry = self._as_datetime(
            self._row_value(row, "post_hooks_lease_expires_at")
        )
        if (
            self._row_value(row, "post_hooks_lease_token") is not None
            and lease_expiry is not None
            and lease_expiry > now
        ):
            return TerminalHookClaim("live_lease", status=status)

        token = uuid.uuid4().hex
        affected = database.statement(
            f"UPDATE {self.table} SET post_hooks_lease_token = %s, "
            "post_hooks_lease_expires_at = %s, "
            "post_hooks_last_error = NULL, updated_at = %s "
            "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
            "AND post_hooks_quarantined_at IS NULL "
            "AND (post_hooks_lease_token IS NULL OR "
            "post_hooks_lease_expires_at IS NULL OR "
            "post_hooks_lease_expires_at <= %s)",
            [
                token,
                now.add(seconds=self.POST_HOOK_LEASE_SECONDS),
                now,
                job_id,
                now,
            ],
        )
        if not self._affected(affected):
            return TerminalHookClaim("live_lease", status=status)
        return TerminalHookClaim(
            "claimed",
            lease_token=token,
            signed_envelope=self._envelope_bytes(self._row_value(row, "signed_envelope")),
            status=status,
            terminal_reason=self._row_value(row, "terminal_reason"),
        )


def _verified_terminal_hook_payload(self, signed_envelope: bytes) -> dict[str, Any]:
    """Verify a row's signed envelope and resolve the job class it names."""
    envelope = SignedJsonJobSerializer.inspect_envelope(
        signed_envelope,
        signing_keys=self.options.get("signing_keys", {}),
        clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
        max_age_seconds=int(
            self.options.get(
                "envelope_max_age_seconds",
                SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
            )
        ),
        allow_not_before=True,
        allow_expired=True,
    )
    return SignedJsonJobSerializer.deserialize_verified(
        envelope["payload"],
        allowed_prefixes=self.options.get("allowed_job_prefixes"),
    )


def _has_terminal_callback(self, status: str | None, hook_owner: Any) -> bool:
    """The ONE definition of "this row has a terminal callback to run"."""
    return status in {
        self.STATUS_DEAD_LETTERED,
        self.STATUS_EXPIRED,
    } and hasattr(hook_owner, "failed")


def _delivery_settle_callbackless_terminal_hook(self, job_id: str) -> str:
    """Settle one due row in the calling process when it has no callback.

    The hooks runner isolates a row because a ``failed()`` body may hang or
    take its interpreter with it. A row with no ``failed()`` runs no body, so
    it has nothing to isolate and must not pay an interpreter boot.

    Classifies before claiming: a row needing a child keeps its lease free
    for that child, so this returns ``"isolate"`` having written nothing.
    Otherwise ``"completed"``, or ``"skipped"`` when another runner owns it.
    """
    row = self._db().select_one(
        f"SELECT status, signed_envelope FROM {self.table} WHERE job_id = %s",
        [job_id],
    )
    if row is None:
        return "skipped"
    try:
        payload = _verified_terminal_hook_payload(
            self,
            self._envelope_bytes(self._row_value(row, "signed_envelope")),
        )
    except Exception:
        # Tampered, unknown-key or unimportable: the isolated path owns the
        # deferral and quarantine that a row like this must go through.
        return "isolate"
    if _has_terminal_callback(self, self._row_value(row, "status"), payload.get("obj")):
        return "isolate"

    claim = self.claim_terminal_hooks(job_id)
    if claim.outcome != "claimed":
        return "skipped"
    try:
        self.complete_terminal_hooks(job_id, claim.lease_token)
    except DeliveryLeaseLost:
        return "skipped"
    return "completed"


def _delivery_release_interrupted_terminal_hook(
    self,
    job_id: str,
    *,
    spawned_at: Any,
) -> bool:
    """Make a hook row due again after its child was stopped for shutdown.

    Shutdown is not the hook failing, so unlike
    ``defer_terminal_hook_process_failure`` this spends no attempt and adds
    no backoff. The parent never saw the child's lease token, so the fences
    are what identify that lease: claiming stamps ``claim time + lease
    seconds`` and clears the last error, which excludes a lease that predates
    this child and a child that already recorded its own deferral.
    """
    now = pendulum.now("UTC")
    affected = self._db().statement(
        f"UPDATE {self.table} SET post_hooks_lease_token = NULL, "
        "post_hooks_lease_expires_at = NULL, updated_at = %s "
        "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
        "AND post_hooks_quarantined_at IS NULL "
        "AND post_hooks_last_error IS NULL "
        "AND post_hooks_lease_token IS NOT NULL "
        "AND post_hooks_lease_expires_at >= %s",
        [now, job_id, spawned_at.add(seconds=self.POST_HOOK_LEASE_SECONDS)],
    )
    return self._affected(affected)


def _delivery_process_terminal_hooks(
    self,
    job_id: str,
    *,
    instance: Any | None = None,
    message: dict[str, Any] | None = None,
    error: Exception | None = None,
) -> bool:
    """Run one claimed hook outbox row with a stable idempotency key."""
    claim = self.claim_terminal_hooks(job_id)
    if claim.outcome == "completed":
        return True
    if claim.outcome == "live_lease":
        return False
    if claim.outcome == "quarantined":
        raise QueueException(f"Queue delivery {job_id} terminal hooks are quarantined.")
    if claim.lease_token is None or claim.signed_envelope is None:
        raise QueueException("Terminal hook claim is missing its lease data.")

    try:
        payload = message
        job_instance = instance
        if payload is None:
            payload = _verified_terminal_hook_payload(self, claim.signed_envelope)

        if payload is None:
            raise QueueException(
                f"Queue delivery {job_id} terminal hook payload is invalid."
            )

        # Most jobs do not declare a terminal callback. Their signed
        # constructor state is irrelevant here and may predate the current
        # constructor contract, so do not instantiate work that cannot run.
        hook_owner = job_instance or payload.get("obj")
        if not _has_terminal_callback(self, claim.status, hook_owner):
            self.complete_terminal_hooks(job_id, claim.lease_token)
            return True

        if job_instance is None:
            job_instance = instantiate_job(
                self.application,
                payload.get("obj"),
                payload.get("args", ()),
                payload.get("init_kwargs", {}),
            )
        if job_instance is None:
            raise QueueException(
                f"Queue delivery {job_id} terminal hook payload is invalid."
            )

        mode = payload.get("_tenant_mode")
        if mode == "central":
            tenant_scope = Tenancy.central()
        elif mode == "tenant" and payload.get("_tenant") is not None:
            tenant_scope = Tenancy.as_tenant(payload["_tenant"])
        else:
            raise QueueException("Terminal hooks require verified tenant mode.")

        with tenant_scope:

            async def _run_hooks() -> None:
                failed_hook = job_instance.failed
                if not inspect.iscoroutinefunction(failed_hook):
                    raise QueueException(
                        f"{type(job_instance).__name__}.failed must "
                        "be async and idempotency-aware."
                    )
                hook_error = error or RuntimeError(
                    claim.terminal_reason or str(claim.status)
                )
                await failed_hook(
                    payload,
                    str(hook_error),
                    idempotency_key=(f"queue-delivery:{job_id}:failed"),
                )

            asyncio.run(
                asyncio.wait_for(
                    _run_hooks(),
                    timeout=self.hook_timeout_seconds,
                )
            )
    except Exception as exc:
        self.defer_terminal_hooks(
            job_id,
            claim.lease_token,
            error=str(exc),
        )
        raise

    self.complete_terminal_hooks(job_id, claim.lease_token)
    return True


def _delivery_complete_terminal_hooks(self, job_id: str, lease_token: str) -> None:
    now = pendulum.now("UTC")
    affected = self._db().statement(
        f"UPDATE {self.table} SET post_hooks_completed_at = %s, "
        "post_hooks_lease_token = NULL, "
        "post_hooks_lease_expires_at = NULL, "
        "post_hooks_last_error = NULL, updated_at = %s "
        "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
        "AND post_hooks_lease_token = %s",
        [now, now, job_id, lease_token],
    )
    if not self._affected(affected):
        row = self._db().select_one(
            f"SELECT post_hooks_completed_at FROM {self.table} WHERE job_id = %s",
            [job_id],
        )
        if self._row_value(row, "post_hooks_completed_at") is not None:
            return
        raise DeliveryLeaseLost(f"Queue delivery {job_id} lost its terminal-hook lease.")


def _delivery_defer_terminal_hooks(
    self,
    job_id: str,
    lease_token: str,
    *,
    error: str,
) -> None:
    self._defer_terminal_hook(
        job_id,
        error=error,
        expected_lease_token=lease_token,
        skip_if_already_deferred=False,
    )


def _delivery_defer_terminal_hook_process_failure(
    self,
    job_id: str,
    *,
    error: str,
) -> str:
    """Back off a hook child that was killed or exited abnormally."""
    return self._defer_terminal_hook(
        job_id,
        error=error,
        expected_lease_token=None,
        skip_if_already_deferred=True,
    )


def _delivery_defer_terminal_hook(
    self,
    job_id: str,
    *,
    error: str,
    expected_lease_token: str | None,
    skip_if_already_deferred: bool,
) -> str:
    now = pendulum.now("UTC")
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"SELECT post_hooks_completed_at, post_hooks_lease_token, "
            "post_hooks_attempts, post_hooks_quarantined_at, "
            "post_hooks_last_error "
            f"FROM {self.table} WHERE job_id = %s FOR UPDATE",
            [job_id],
        )
        if row is None:
            raise QueueException(
                f"Queue delivery {job_id} is absent during hook deferral."
            )
        if self._row_value(row, "post_hooks_completed_at") is not None:
            return "completed"
        if self._row_value(row, "post_hooks_quarantined_at") is not None:
            return "quarantined"
        current_token = self._row_value(row, "post_hooks_lease_token")
        if current_token is None or (
            expected_lease_token is not None and current_token != expected_lease_token
        ):
            raise DeliveryLeaseLost(
                f"Queue delivery {job_id} lost its terminal-hook retry lease."
            )
        if (
            skip_if_already_deferred
            and self._row_value(row, "post_hooks_last_error") is not None
        ):
            return "already_deferred"

        attempts = int(self._row_value(row, "post_hooks_attempts") or 0) + 1
        safe_error = self._safe_error(error)
        if attempts >= self.hook_max_attempts:
            affected = database.statement(
                f"UPDATE {self.table} SET post_hooks_attempts = %s, "
                "post_hooks_quarantined_at = %s, "
                "post_hooks_lease_token = NULL, "
                "post_hooks_lease_expires_at = NULL, "
                "post_hooks_last_error = %s, updated_at = %s "
                "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
                "AND post_hooks_lease_token = %s",
                [
                    attempts,
                    now,
                    safe_error,
                    now,
                    job_id,
                    current_token,
                ],
            )
            outcome = "quarantined"
        else:
            index = min(
                attempts - 1,
                len(self._HOOK_BACKOFF_SECONDS) - 1,
            )
            affected = database.statement(
                f"UPDATE {self.table} SET post_hooks_attempts = %s, "
                "post_hooks_lease_expires_at = %s, "
                "post_hooks_last_error = %s, updated_at = %s "
                "WHERE job_id = %s AND post_hooks_completed_at IS NULL "
                "AND post_hooks_lease_token = %s",
                [
                    attempts,
                    now.add(seconds=self._HOOK_BACKOFF_SECONDS[index]),
                    safe_error,
                    now,
                    job_id,
                    current_token,
                ],
            )
            outcome = "deferred"
        if not self._affected(affected):
            raise DeliveryLeaseLost(
                f"Queue delivery {job_id} lost its terminal-hook retry lease."
            )
        return outcome
