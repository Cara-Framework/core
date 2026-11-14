"""Relay-side publication: the iteration loop, batch broker publish, settle and quarantine.

The tenant-fair claim itself (plan + reserve) lives in ``_DeliveryClaim``.
"""

from __future__ import annotations

import json
from typing import Any

import pendulum

from cara.exceptions import QueueException
from cara.facades import Log
from cara.queues.serializers import SignedJsonJobSerializer

from .DeliveryEnvelopeExpired import DeliveryEnvelopeExpired
from .DeliveryEnvelopeMismatch import DeliveryEnvelopeMismatch

QueueJobDeliveryStore: type


def _bind_store(store_type: type) -> None:
    global QueueJobDeliveryStore
    QueueJobDeliveryStore = store_type


def _delivery_publish_due(self) -> dict[str, int]:
    """Claim and publish up to ``claim_batch`` due rows in leased batches.

    ONE plan per iteration: the fair ranking (O(backlogged tenants)) runs
    once and every reserve of the iteration leases from it in plan order,
    so its cost is spread over the whole batch instead of paid per lease.
    A plan is at most an iteration old; the next iteration re-ranks, so a
    newly arrived tenant waits at most one iteration for its turn.
    """
    recovery = self.recover_stale_executions(self.claim_batch)
    lost_requeued = self.requeue_lost_broker_copies(self.claim_batch)
    expired = self.expire_due(self.claim_batch)
    result = {
        "claimed": 0,
        "published": 0,
        "retried": 0,
        "quarantined": 0,
        "settle_lost": 0,
        "expired": expired,
        "stale_requeued": recovery["requeued"],
        "stale_reconciled": recovery["reconciled"],
        "unclaimed_requeued": recovery["unclaimed"],
        "lost_requeued": lost_requeued,
    }
    remaining = self.claim_batch
    planned = self._plan_publish(self.claim_batch)
    while remaining > 0 and planned:
        claimed = self._reserve_publish(planned, remaining)
        if claimed is None:
            break
        rows, token = claimed
        taken = {str(self._row_value(row, "job_id")) for row in rows}
        planned = [entry for entry in planned if entry[0] not in taken]
        remaining -= len(rows)
        result["claimed"] += len(rows)
        stop = self._publish_batch(rows, token, result)
        if stop:
            break
    return result


def _delivery_publish_batch(self, rows: list[Any], token: str, result: dict) -> bool:
    """Publish one claimed batch, then settle every broker-confirmed row at once.

    Returns True when the relay iteration must stop (a systemic publish
    failure, or a settlement this relay lost). Rows after a failed publish
    were never attempted: they go straight back to ``pending`` without a
    publish attempt counted against them.

    The rows go out one after another on the relay's one confirm channel;
    every row is broker-confirmed before it is settled. Publishing a batch
    on several threads (one confirm channel and broker connection each) was
    measured and rejected (integration stress 2026-11-12): with the batch
    claim the relay out-publishes the workers, so 8 confirm threads changed
    nothing at 4 workers (397/385 vs 394/386 rows/s) and cost 8 broker
    connections per relay — the resource whose exhaustion took the broker
    down that day.
    """
    outcomes = self._publish_rows(rows)
    sent: list[str] = []
    stop = False
    untried: list[str] = []
    for row, exc in zip(rows, outcomes, strict=True):
        job_id = str(self._row_value(row, "job_id"))
        if exc is _UNTRIED:
            untried.append(job_id)
            continue
        try:
            if exc is not None:
                raise exc
        except DeliveryEnvelopeExpired as exc:
            self._expire_publish(
                job_id,
                token,
                str(exc),
            )
            result["expired"] += 1
        except DeliveryEnvelopeMismatch as exc:
            self._quarantine_publish(
                job_id,
                token,
                str(exc),
            )
            result["quarantined"] += 1
            Log.error(
                "Queue outbox row %s is invalid and was quarantined: %s",
                self._row_value(row, "job_id"),
                exc,
                category="cara.queue.delivery",
            )
        except Exception as exc:
            released = self._release_publish(
                job_id,
                token,
                int(self._row_value(row, "publish_attempts") or 0),
                # pika's AMQPConnectionError stringifies to nothing.
                str(exc) or repr(exc),
            )
            result["retried" if released else "settle_lost"] += 1
            Log.warning(
                "Queue outbox publish failed for %s%s: %s",
                self._row_value(row, "job_id"),
                ("" if released else " and its retry lease was concurrently lost"),
                exc,
                category="cara.queue.delivery",
            )
            # A generic publish exception is treated as a systemic relay
            # failure. Continuing would multiply reconnect/socket timeout
            # cost by claim_batch and stampede the broker during outages:
            # rows not yet started are skipped (``_UNTRIED``) and handed back.
            stop = True
        else:
            sent.append(job_id)
    if untried:
        self._unclaim_publish(untried, token)
        result["claimed"] -= len(untried)
    if sent:
        settled = self._mark_published(sent, token)
        result["published"] += len(settled)
        lost = len(sent) - len(settled)
        if lost:
            result["settle_lost"] += lost
            stop = True
    return stop


#: A row the batch never started because an earlier publish failed.
_UNTRIED = object()


def _delivery_publish_rows(self, rows: list[Any]) -> list[Any]:
    """Publish ``rows``; return per row None (confirmed), its error, or ``_UNTRIED``.

    Every envelope is verified first; the valid ones go to the broker as ONE
    batch whose confirms are awaited together
    (``driver._publish_registered_envelopes``), so a batch costs one
    quorum-queue confirm wait instead of one per row. A per-message refusal
    (unroutable, nack) is that row's error. A transport failure is systemic:
    the first row carries it (one publish attempt counted per outage, not
    per row) and the rest are ``_UNTRIED`` — handed back unpenalised. A
    message the broker did receive before the failure is still settled by
    the worker's receipt, as with any lost confirm.
    """
    outcomes: list[Any] = [None] * len(rows)
    sendable: list[tuple[int, bytes, dict[str, Any]]] = []
    for index, row in enumerate(rows):
        try:
            body, payload = self._verified_envelope(row)
        except (DeliveryEnvelopeExpired, DeliveryEnvelopeMismatch) as exc:
            outcomes[index] = exc
        else:
            sendable.append((index, body, payload))
    if not sendable:
        return outcomes
    try:
        confirmed = self.driver._publish_registered_envelopes(
            [(body, payload) for _index, body, payload in sendable],
            capability=self,
        )
    except Exception as exc:
        outcomes[sendable[0][0]] = exc
        for index, _body, _payload in sendable[1:]:
            outcomes[index] = _UNTRIED
        return outcomes
    for (index, _body, _payload), outcome in zip(sendable, confirmed, strict=True):
        outcomes[index] = outcome
    return outcomes


def _delivery_publish_claimed(self, row: Any, token: str) -> bool:
    """Publish one claimed row and settle it (the single-row path)."""
    (outcome,) = self._publish_rows([row])
    if outcome is not None:
        raise outcome
    job_id = str(self._row_value(row, "job_id"))
    return job_id in self._mark_published([job_id], token)


def _delivery_mark_published(self, job_ids: list[str], token: str) -> set[str]:
    """Settle broker-confirmed rows in one statement; return the settled ids.

    A worker that already received a row settles its publication itself
    (the receipt closes the confirm/ledger crash window), so a row this
    UPDATE misses still counts when it reads ``published``.
    """
    now = pendulum.now("UTC")
    database = self._db()
    done = database.select_one(
        f"WITH settled AS (UPDATE {self.table} SET publish_status = %s, "
        "published_at = %s, "
        "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
        "last_publish_error = NULL, updated_at = %s "
        "WHERE job_id = ANY(%s::varchar[]) AND publish_status = %s "
        "AND publish_lease_token = %s RETURNING job_id) "
        "SELECT COALESCE(json_agg(settled.job_id), '[]'::json) AS ids FROM settled",
        [
            self.PUBLISH_PUBLISHED,
            now,
            now,
            list(job_ids),
            self.PUBLISH_PROCESSING,
            token,
        ],
    )
    settled = self._json_ids(self._row_value(done, "ids") if done is not None else None)
    missing = [job_id for job_id in job_ids if job_id not in settled]
    if missing:
        current = database.select_one(
            "SELECT COALESCE(json_agg(job_id), '[]'::json) AS ids "
            f"FROM {self.table} WHERE job_id = ANY(%s::varchar[]) "
            "AND publish_status = %s AND published_at IS NOT NULL",
            [missing, self.PUBLISH_PUBLISHED],
        )
        settled |= self._json_ids(
            self._row_value(current, "ids") if current is not None else None
        )
    return settled


def _delivery_json_ids(value: Any) -> set[str]:
    if isinstance(value, (str, bytes)):
        value = json.loads(value)
    return {str(item) for item in (value or ())}


def _delivery_unclaim_publish(self, job_ids: list[str], token: str) -> None:
    """Hand claimed rows this relay never tried back to the outbox, unpenalised."""
    now = pendulum.now("UTC")
    self._db().statement(
        f"UPDATE {self.table} SET publish_status = %s, "
        "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
        "updated_at = %s WHERE job_id = ANY(%s::varchar[]) "
        "AND publish_status = %s AND publish_lease_token = %s",
        [self.PUBLISH_PENDING, now, list(job_ids), self.PUBLISH_PROCESSING, token],
    )


def _delivery_verified_envelope(self, row: Any) -> tuple[bytes, dict[str, Any]]:
    """One claimed row's signed envelope, verified against its immutable columns."""
    body = self._envelope_bytes(self._row_value(row, "signed_envelope"))
    try:
        envelope = SignedJsonJobSerializer.inspect_envelope(
            body,
            signing_keys=self.options.get("signing_keys", {}),
            clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
            max_age_seconds=int(
                self.options.get(
                    "envelope_max_age_seconds",
                    SignedJsonJobSerializer.DEFAULT_MAX_AGE_SECONDS,
                )
            ),
            allow_expired=True,
        )
    except QueueException as exc:
        raise DeliveryEnvelopeMismatch(str(exc)) from exc
    if SignedJsonJobSerializer.is_expired(
        envelope,
        clock_skew_seconds=int(self.options.get("clock_skew_seconds", 30)),
    ):
        raise DeliveryEnvelopeExpired(
            "Signed queue envelope expired before broker publication."
        )
    payload = envelope["payload"]
    digest = SignedJsonJobSerializer.canonical_envelope_sha256(body)
    checks = {
        "job_id": str(payload["job_id"]),
        "db_job_id": payload.get("db_job_id"),
        "payload_sha256": digest,
        "tenant_mode": payload.get("_tenant_mode"),
        "tenant_id": payload.get("_tenant"),
        "queue": payload["queue"],
        "priority": payload["priority"],
    }
    for field, value in checks.items():
        stored = self._row_value(row, field)
        if field == "job_id":
            stored = str(stored)
        if stored != value:
            raise DeliveryEnvelopeMismatch(
                f"Queue outbox immutable {field} does not match signed envelope."
            )
    return body, payload


def _delivery_release_publish(
    self,
    job_id: str,
    token: str,
    attempts: int,
    error: str,
) -> bool:
    next_attempt = max(attempts, 0) + 1
    index = min(next_attempt - 1, len(self._PUBLISH_BACKOFF_SECONDS) - 1)
    now = pendulum.now("UTC")
    affected = self._db().statement(
        f"UPDATE {self.table} SET publish_status = %s, "
        "publish_attempts = %s, publish_retry_at = %s, "
        "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
        "last_publish_error = %s, updated_at = %s "
        "WHERE job_id = %s AND publish_status = %s "
        "AND publish_lease_token = %s",
        [
            self.PUBLISH_PENDING,
            next_attempt,
            now.add(seconds=self._PUBLISH_BACKOFF_SECONDS[index]),
            self._safe_error(error),
            now,
            job_id,
            self.PUBLISH_PROCESSING,
            token,
        ],
    )
    return self._affected(affected)


def _delivery_quarantine_publish(self, job_id: str, token: str, error: str) -> None:
    now = pendulum.now("UTC")
    safe_error = self._safe_error(error)
    database = self._db()
    with database.transaction():
        row = database.select_one(
            f"UPDATE {self.table} SET status = %s, completed_at = %s, "
            "terminal_reason = %s, publish_status = %s, "
            "publish_lease_token = NULL, publish_lease_expires_at = NULL, "
            "last_publish_error = %s, "
            "post_hooks_quarantined_at = %s, "
            "post_hooks_last_error = %s, updated_at = %s "
            "WHERE job_id = %s AND publish_status = %s "
            "AND publish_lease_token = %s RETURNING db_job_id",
            [
                self.STATUS_DEAD_LETTERED,
                now,
                self._safe_error(f"{self.PUBLISH_QUARANTINE_REASON_PREFIX}{safe_error}"),
                self.PUBLISH_FAILED,
                safe_error,
                now,
                "terminal hooks skipped: signed envelope is invalid",
                now,
                job_id,
                self.PUBLISH_PROCESSING,
                token,
            ],
        )
        if row is None:
            raise QueueException(f"Queue delivery {job_id} lost its quarantine lease.")
        self._mark_tracker_failed(
            database,
            int(self._row_value(row, "db_job_id")),
            now,
        )
