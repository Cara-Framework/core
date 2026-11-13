"""Worker-side receipt: execution claim, lease renewal, completion and dead-letter."""

from __future__ import annotations

import uuid
from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.facades import Log
from cara.queues.serializers import SignedJsonJobSerializer

from .DeliveryClaim import DeliveryClaim
from .DeliveryLeaseLost import DeliveryLeaseLost

QueueJobDeliveryStore: type


def _bind_store(store_type: type) -> None:
    global QueueJobDeliveryStore
    QueueJobDeliveryStore = store_type


#: Tracker statuses that mean the body already finished its work.
_TRACKER_FINISHED = ("completed", "success")


def _stamp_tracker_processing(
    database: Any,
    db_job_id: Any,
    now: pendulum.DateTime,
    *,
    reclaimed: bool,
) -> None:
    """Mark the tracked job ``processing`` inside the claim's transaction.

    It used to be a separate write after the claim (``JobTracker`` read,
    save, read back): three more round trips and a third WAL flush per job,
    the largest fixed cost of a short job (capacity.md, 2026-11-13). A
    reclaimed row may belong to a body that already finished before its
    process died; that tracker stays finished, and the worker's completion
    lookup settles the row without running the body again. A missing tracker
    on a fresh claim rolls the claim back: durable execution needs the fence.
    """
    if db_job_id is None:
        return
    sql = "UPDATE job SET status = %s, started_at = %s, updated_at = %s WHERE id = %s"
    bindings: list[Any] = ["processing", now, now, int(db_job_id)]
    if reclaimed:
        sql += " AND status <> ALL(%s)"
        bindings.append(list(_TRACKER_FINISHED))
    affected = database.statement(sql, bindings)
    if not reclaimed and not QueueJobDeliveryStore._affected(affected):
        raise QueueException(f"Tracked queue job {db_job_id} does not exist.")


def _delivery_claim_execution(
    self,
    *,
    body: bytes | str | dict[str, Any],
    payload: dict[str, Any],
) -> DeliveryClaim:
    """Atomically validate identity and lease one delivery for execution."""
    digest = SignedJsonJobSerializer.canonical_envelope_sha256(body)
    job_id = str(payload["job_id"])
    # The signed budget fitted the lease policy it was signed under; a later,
    # shorter policy wins instead of failing every redelivery of trusted work.
    # The worker runs the job for min(signed, current class timeout), which
    # ``execution_timeout_for`` keeps inside the current lease, so the clamped
    # lease still covers the whole execution.
    timeout_seconds = min(
        self._bounded_int(
            payload.get("timeout_seconds"),
            minimum=1,
            maximum=86400,
            field="timeout_seconds",
        ),
        self.execution_lease_seconds - self.execution_lease_grace_seconds,
    )
    # ``timeout + grace`` is the run's whole budget and so the lease's
    # CEILING; the lease itself is the short heartbeat lease the worker
    # renews while the job runs (``renew_execution``), so a dead worker's
    # row lapses within one heartbeat lease, not within the whole budget.
    budget_seconds = timeout_seconds + self.execution_lease_grace_seconds
    lease_seconds = min(budget_seconds, self.execution_heartbeat_seconds)
    now = pendulum.now("UTC")
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"SELECT job_id, db_job_id, payload_sha256, tenant_mode, "
            f"tenant_id, status, attempts, "
            f"lease_token, lease_expires_at, expires_at, available_at, "
            f"terminal_reason "
            f"FROM {self.table} "
            "WHERE job_id = %s FOR UPDATE",
            [job_id],
        )
        if row is None:
            return DeliveryClaim("unknown")
        if (
            self._row_value(row, "payload_sha256") != digest
            or self._row_value(row, "db_job_id") != payload.get("db_job_id")
            or self._row_value(row, "tenant_mode") != payload.get("_tenant_mode")
            or self._row_value(row, "tenant_id") != payload.get("_tenant")
        ):
            return DeliveryClaim("mismatch")

        status = str(self._row_value(row, "status"))
        available_at = self._as_datetime(self._row_value(row, "available_at"))
        if available_at is None:
            return DeliveryClaim("mismatch")
        if available_at > now:
            affected = database.statement(
                f"UPDATE {self.table} SET publish_status = %s, "
                "publish_retry_at = %s, published_at = NULL, "
                "publish_lease_token = NULL, "
                "publish_lease_expires_at = NULL, updated_at = %s "
                "WHERE job_id = %s AND status = %s",
                [
                    self.PUBLISH_PENDING,
                    available_at,
                    now,
                    job_id,
                    self.STATUS_PENDING,
                ],
            )
            if not self._affected(affected):
                raise QueueException(
                    f"Queue delivery {job_id} early-publication recovery was lost."
                )
            return DeliveryClaim("not_ready")

        self._reconcile_broker_receipt(database, job_id, now)
        if status in self.TERMINAL_STATUSES:
            return DeliveryClaim(
                status,
                terminal_reason=self._row_value(row, "terminal_reason"),
            )
        reclaimed = False
        if status == self.STATUS_PROCESSING:
            lease_expiry = self._as_datetime(self._row_value(row, "lease_expires_at"))
            if lease_expiry is not None and lease_expiry > now:
                return DeliveryClaim("live_lease")
            reclaimed = True
        elif status != self.STATUS_PENDING:
            return DeliveryClaim("mismatch")

        expires_at = self._as_datetime(self._row_value(row, "expires_at"))
        if expires_at is None:
            return DeliveryClaim("mismatch")
        refused: tuple[str, str] | None = None
        if expires_at <= now:
            refused = (self.STATUS_EXPIRED, "envelope_expired_before_execution")
        elif (
            reclaimed
            and int(self._row_value(row, "attempts") or 0) >= self.CRASH_RECLAIM_CEILING
            and self._tracker_is_open(database, int(payload["db_job_id"]))
        ):
            # A surviving broker copy reached the row before the relay's
            # recovery did: the same ceiling, on the other way back in. A
            # finished tracker is not refused; the worker reconciles it.
            refused = (self.STATUS_DEAD_LETTERED, self.CRASH_LOOP_REASON)
        if refused is not None:
            refused_status, refused_reason = refused
            affected = database.statement(
                f"UPDATE {self.table} SET status = %s, completed_at = %s, "
                "terminal_reason = %s, lease_token = NULL, "
                "lease_expires_at = NULL, publish_lease_token = NULL, "
                "publish_lease_expires_at = NULL, updated_at = %s "
                "WHERE job_id = %s AND status IN (%s, %s)",
                [
                    refused_status,
                    now,
                    refused_reason,
                    now,
                    job_id,
                    self.STATUS_PENDING,
                    self.STATUS_PROCESSING,
                ],
            )
            if not self._affected(affected):
                raise QueueException("Queue delivery expiry settlement was lost.")
            self._mark_tracker_failed(
                database,
                int(payload["db_job_id"]),
                now,
            )
            Log.error(
                "Queue delivery %s refused before execution (%s); retained in "
                "the ledger for audit and operator replay.",
                job_id,
                refused_reason,
                category="cara.queue.delivery",
            )
            return DeliveryClaim(refused_status, terminal_reason=refused_reason)

        token = uuid.uuid4().hex
        affected = database.statement(
            f"UPDATE {self.table} SET status = %s, attempts = attempts + 1, "
            "lease_token = %s, lease_expires_at = %s, updated_at = %s "
            "WHERE job_id = %s AND status IN (%s, %s)",
            [
                self.STATUS_PROCESSING,
                token,
                now.add(seconds=lease_seconds),
                now,
                job_id,
                self.STATUS_PENDING,
                self.STATUS_PROCESSING,
            ],
        )
        if not self._affected(affected):
            raise QueueException("Queue delivery execution lease was lost.")
        _stamp_tracker_processing(
            database,
            payload.get("db_job_id"),
            now,
            reclaimed=reclaimed,
        )
    return DeliveryClaim(
        "claimed",
        token,
        reclaimed=reclaimed,
        lease_deadline=now.add(seconds=budget_seconds),
    )


def _delivery_renew_execution(
    self,
    job_id: str,
    lease_token: str,
    *,
    deadline: Any | None = None,
) -> bool:
    """Heartbeat: extend this worker's execution lease while its job runs.

    Compare-and-set on ``lease_token`` while the row is still
    ``processing``: a lease that lapsed and was recovered (the relay
    returned the row to the outbox) or re-claimed by another worker is never
    extended. ``deadline`` — the claim's ``timeout + grace`` budget — caps
    the extension, so the heartbeat can never hold a row longer than the
    pre-heartbeat lease did. Returns False once the lease is no longer this
    worker's, or the budget is spent; the heartbeat then stops.
    """
    now = pendulum.now("UTC")
    expiry = now.add(seconds=self.execution_heartbeat_seconds)
    if deadline is not None:
        if deadline <= now:
            return False
        expiry = min(expiry, deadline)
    affected = self._db().statement(
        f"UPDATE {self.table} SET lease_expires_at = %s, updated_at = %s "
        "WHERE job_id = %s AND status = %s AND lease_token = %s",
        [
            expiry,
            now,
            job_id,
            self.STATUS_PROCESSING,
            lease_token,
        ],
    )
    return self._affected(affected)


def _delivery_reconcile_broker_receipt(
    self,
    database: Any,
    job_id: str,
    now: pendulum.DateTime,
) -> None:
    """Close the publisher-confirm/ledger-CAS crash window on receipt."""
    affected = database.statement(
        f"UPDATE {self.table} SET publish_status = %s, "
        "published_at = COALESCE(published_at, %s), "
        "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
        "last_publish_error = NULL, updated_at = %s "
        "WHERE job_id = %s AND publish_status IN (%s, %s, %s, %s)",
        [
            self.PUBLISH_PUBLISHED,
            now,
            now,
            job_id,
            self.PUBLISH_PENDING,
            self.PUBLISH_PROCESSING,
            self.PUBLISH_PUBLISHED,
            self.PUBLISH_FAILED,
        ],
    )
    if not self._affected(affected):
        raise DeliveryLeaseLost(
            f"Queue delivery {job_id} broker receipt was not reconciled."
        )


def _delivery_complete(self, job_id: str, lease_token: str) -> None:
    self._settle_with_retry(job_id, lease_token, self.STATUS_COMPLETED)


def _delivery_complete_with_tracker(
    self,
    job_id: str,
    lease_token: str,
    *,
    db_job_id: int,
) -> None:
    self._settle_execution_with_tracker(
        job_id,
        lease_token,
        db_job_id=db_job_id,
        status=self.STATUS_COMPLETED,
        tracker_status="completed",
    )


def _delivery_dead_letter(
    self,
    job_id: str,
    lease_token: str,
    *,
    reason: str,
) -> None:
    self._settle_with_retry(
        job_id,
        lease_token,
        self.STATUS_DEAD_LETTERED,
        reason=reason,
    )
