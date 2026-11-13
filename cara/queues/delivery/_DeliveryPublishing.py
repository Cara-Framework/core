"""Relay-side publication: the tenant-fair batch claim, broker publish, settle and quarantine."""

from __future__ import annotations

import json
import uuid
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


def _delivery_claim_next_publish(self) -> tuple[Any, str] | None:
    """Lease the single next due row — :meth:`_claim_publish_batch` of one."""
    claimed = self._claim_publish_batch(1)
    if claimed is None:
        return None
    rows, token = claimed
    return rows[0], token


def _delivery_claim_publish_batch(self, limit: int) -> tuple[list[Any], str] | None:
    """Atomically lease up to ``limit`` due rows: fair across tenants, then by aged priority.

    ONE statement leases a whole batch. Finding the tenants with due work
    costs a few index probes per tenant (below), so leasing one row per
    statement made every published row pay for walking every backlogged
    tenant: at 1,000 tenants with a backlog the claim took 12-20 ms and the
    relay — the one publisher of the whole fleet — topped out at 35-65
    rows/s however many workers or window slots there were (stress
    2026-11-12). The batch takes each tenant's single best head (at most
    one row per tenant per queue per statement, so the tenant window is
    re-checked by the next statement), ranks those heads in the fair order
    below, and fills each queue's FREE broker-window slots from the top.
    A batch of one is exactly the old single-row pick.

    A bounded per-queue broker window keeps the backlog in this ledger
    instead of preloading RabbitMQ, so THIS statement is the scheduler: the
    order it picks rows in is the order work starts.

    Every second claim first serves the oldest head past
    half a ``priority_aging_seconds``, whoever owns it. Round robin alone
    keeps a big tenant's tail behind every other tenant's newer rows (live
    2026-11-08: 12:00 rows published at 12:08 after 12:04 rows); alternating
    guarantees the oldest rows progress and costs a newcomer one row at most.

    Within one queue the pick is otherwise, in order:

    1. **Tenant fair share.** Each tenant with due work offers its own head
       row (one per priority lane); the tenant holding the FEWEST rows in
       this queue's broker window goes first, and among those the tenant
       this queue served LEAST RECENTLY (its newest ``published_at``; never
       served = first) — a round robin over tenants. One tenant's
       10,000-row backlog, or a hundred tenants' backlogs, therefore never
       sit ahead of another tenant's single job: the newcomer holds nothing
       and was never served, so its head is the next row published. Central
       (tenantless) rows share one fair-share key.
    2. **Aged priority.** Between a tenant's own rows, the highest
       effective tier wins; each ``priority_aging_seconds`` of waiting
       promotes a row one tier, so a tenant's low work reaches the critical
       tier after three intervals and cannot starve behind its own urgent
       traffic. Priority is deliberately NOT ranked above the round robin:
       an aged backlog would otherwise outrank every other tenant's fresh
       work again.
    3. **Age.** Oldest ``available_at`` / ``created_at`` first.

    ``tenant_window_per_queue`` (when > 0) additionally caps how many
    window slots one tenant may hold in a queue, so a backlogged tenant
    alone never fills the whole window and a newly arrived tenant's job
    finds a free slot at once rather than after an in-flight job finishes.

    ``tenant_urgent_slots_per_queue`` (when > 0) lets a tenant's URGENT row
    — declared ``high`` or ``critical`` — go that many slots past the
    tenant window, so the tenant's own long default/low jobs filling its
    window cannot hold back its urgent job until one of them ends (a sale's
    stock push behind the same seller's 40-second pulls). Default/low rows
    still stop at the tenant window, so bulk work alone never fills the
    broker window; the urgent slot is taken only while the broker window
    has room, and the fair order above still serves the tenant holding the
    fewest slots first. The declared priority decides, not the aged tier:
    a backlog that aged up does not earn the urgent slots.

    A DECLARED ``critical`` row is the tenant's own best head before any
    aged row (only the oldest-overdue turn goes before it), and
    ``critical_reserve_per_queue`` (when > 0) gives it that many slots no
    other tier may take: past the tenant window, the urgent slots and the
    broker window. While a queue's window is full only the ``critical`` lane
    is scanned, so the reserve costs a full queue one index probe. Live
    2026-11-13: one channel's 9,488 ``high`` ends aged to the top tier and
    held the seller's whole window; the same seller's stock push on another
    channel waited 15 minutes behind them.

    The tenants with due work are found by a recursive skip scan over
    ``queue_job_delivery_tenant_head_idx`` and each one's last service by
    ``queue_job_delivery_tenant_served_idx`` — a few index probes per tenant
    per priority lane, never a walk of a backlogged tenant's rows. Relay
    replicas serialize this short reservation statement with a PostgreSQL
    advisory transaction lock; ``publish_status=processing`` is the durable
    window reservation, so replicas cannot overfill RabbitMQ concurrently.
    """
    limit = max(1, int(limit))
    token = uuid.uuid4().hex
    now = pendulum.now("UTC")
    database = self._db()
    serve_oldest = "TRUE" if self._serve_oldest_next else "FALSE"
    self._serve_oldest_next = not self._serve_oldest_next
    due_row = (
        "AND delivery.status = 'pending' "
        "AND delivery.publish_status IN ('pending','processing') "
        "AND delivery.available_at <= settings.now "
        "AND delivery.publish_retry_at <= settings.now "
        "AND delivery.expires_at > settings.now "
        "AND (delivery.publish_status = 'pending' OR "
        "(delivery.publish_status = 'processing' AND "
        "(delivery.publish_lease_expires_at IS NULL OR "
        "delivery.publish_lease_expires_at <= settings.now))) "
    )
    open_row = (
        "AND delivery.status = 'pending' "
        "AND delivery.publish_status IN ('pending','processing') "
    )
    with database.transaction():
        database.select_one(
            "SELECT pg_advisory_xact_lock(%s) AS locked",
            [self._PUBLISH_CLAIM_ADVISORY_LOCK],
        )
        row = database.select_one(
            "WITH RECURSIVE settings AS (SELECT %s::timestamptz AS now, "
            "%s::numeric AS aging_seconds, %s::integer AS broker_window, "
            "%s::integer AS tenant_window, %s::integer AS urgent_slots, "
            "%s::integer AS batch_limit, "
            # A bounded config integer, inlined so the bind order is unchanged.
            f"{int(self.critical_reserve_per_queue)}::integer AS critical_reserve), "
            "priority_lanes(priority, base_rank) AS (VALUES "
            "('critical', 0), ('high', 1), ('default', 2), ('low', 3)), "
            "outstanding AS (SELECT outstanding.queue, "
            "COALESCE(outstanding.tenant_id, 0) AS tenant_key, COUNT(*) AS held "
            f"FROM {self.table} AS outstanding CROSS JOIN settings "
            "WHERE outstanding.queue = ANY(%s::varchar[]) "
            "AND outstanding.status IN ('pending', 'processing') "
            "AND (outstanding.publish_status = 'published' OR "
            "(outstanding.publish_status = 'processing' AND "
            "outstanding.publish_lease_expires_at > settings.now)) "
            "GROUP BY outstanding.queue, COALESCE(outstanding.tenant_id, 0)), "
            "queue_room AS (SELECT candidate.queue, settings.broker_window - "
            "COALESCE((SELECT SUM(outstanding.held) FROM outstanding "
            "WHERE outstanding.queue = candidate.queue), 0) AS room "
            "FROM unnest(%s::varchar[]) AS candidate(queue) CROSS JOIN settings), "
            "eligible_queues AS (SELECT queue_room.queue, queue_room.room "
            "FROM queue_room CROSS JOIN settings "
            "WHERE queue_room.room + settings.critical_reserve > 0), "
            "tenant_keys(queue, priority, base_rank, tenant_key) AS ("
            "SELECT eligible_queues.queue, priority_lanes.priority, "
            "priority_lanes.base_rank, first_key.tenant_key "
            "FROM eligible_queues CROSS JOIN priority_lanes CROSS JOIN LATERAL ("
            "SELECT COALESCE(delivery.tenant_id, 0) AS tenant_key "
            f"FROM {self.table} AS delivery "
            "WHERE delivery.queue = eligible_queues.queue "
            "AND delivery.priority = priority_lanes.priority "
            f"{open_row}"
            "ORDER BY COALESCE(delivery.tenant_id, 0) LIMIT 1) AS first_key "
            # A full window is scanned for the reserve's lane alone.
            "WHERE eligible_queues.room > 0 OR priority_lanes.base_rank = 0 "
            "UNION ALL "
            "SELECT tenant_keys.queue, tenant_keys.priority, "
            "tenant_keys.base_rank, next_key.tenant_key "
            "FROM tenant_keys CROSS JOIN LATERAL ("
            "SELECT COALESCE(delivery.tenant_id, 0) AS tenant_key "
            f"FROM {self.table} AS delivery "
            "WHERE delivery.queue = tenant_keys.queue "
            "AND delivery.priority = tenant_keys.priority "
            f"{open_row}"
            "AND COALESCE(delivery.tenant_id, 0) > tenant_keys.tenant_key "
            "ORDER BY COALESCE(delivery.tenant_id, 0) LIMIT 1) AS next_key), "
            "heads AS (SELECT head.job_id, head.available_at, head.created_at, "
            "tenant_keys.queue, tenant_keys.tenant_key, "
            "(tenant_keys.base_rank = 0) AS declared_critical, "
            "COALESCE(outstanding.held, 0) AS held, "
            "last_served.published_at AS last_served_at, "
            "GREATEST(tenant_keys.base_rank - FLOOR(EXTRACT(EPOCH FROM "
            "(settings.now - head.available_at)) / "
            "settings.aging_seconds), 0) AS aged_rank "
            "FROM tenant_keys CROSS JOIN settings "
            "LEFT JOIN outstanding ON outstanding.queue = tenant_keys.queue "
            "AND outstanding.tenant_key = tenant_keys.tenant_key "
            "LEFT JOIN LATERAL (SELECT served.published_at "
            f"FROM {self.table} AS served "
            "WHERE served.queue = tenant_keys.queue "
            "AND COALESCE(served.tenant_id, 0) = tenant_keys.tenant_key "
            "AND served.published_at IS NOT NULL "
            "ORDER BY served.published_at DESC LIMIT 1) AS last_served ON TRUE "
            "CROSS JOIN LATERAL ("
            "SELECT delivery.job_id, delivery.available_at, delivery.created_at "
            f"FROM {self.table} AS delivery "
            "WHERE delivery.queue = tenant_keys.queue "
            "AND delivery.priority = tenant_keys.priority "
            "AND COALESCE(delivery.tenant_id, 0) = tenant_keys.tenant_key "
            f"{due_row}"
            "ORDER BY delivery.available_at, delivery.created_at LIMIT 1"
            ") AS head "
            "WHERE settings.tenant_window <= 0 OR tenant_keys.tenant_key = 0 "
            "OR COALESCE(outstanding.held, 0) < settings.tenant_window "
            # The urgent slots and the critical reserve: by DECLARED tier
            # (base_rank), never aged.
            "+ CASE WHEN tenant_keys.base_rank = "
            f"{int(self._PRIORITY_RANKS['critical'])} "
            "THEN settings.urgent_slots + settings.critical_reserve "
            "WHEN tenant_keys.base_rank = "
            f"{int(self._PRIORITY_RANKS['high'])} "
            "THEN settings.urgent_slots ELSE 0 END), "
            # The oldest-overdue turn: this head ranks first in its batch.
            "oldest AS (SELECT heads.job_id FROM heads CROSS JOIN settings "
            f"WHERE {serve_oldest} AND 2 * EXTRACT(EPOCH FROM "
            "(settings.now - heads.available_at)) >= settings.aging_seconds "
            "ORDER BY heads.available_at LIMIT 1), "
            # One head per tenant per queue: its own best by aged tier, age.
            "tenant_best AS (SELECT DISTINCT ON (heads.queue, heads.tenant_key) "
            "heads.*, (oldest.job_id IS NOT NULL) AS is_oldest "
            "FROM heads LEFT JOIN oldest ON oldest.job_id = heads.job_id "
            "ORDER BY heads.queue, heads.tenant_key, "
            "(oldest.job_id IS NOT NULL) DESC, heads.declared_critical DESC, "
            "heads.aged_rank, heads.available_at, heads.created_at), "
            "ranked AS (SELECT tenant_best.job_id, tenant_best.queue, "
            "tenant_best.declared_critical, "
            "row_number() OVER (PARTITION BY tenant_best.queue "
            f"ORDER BY {_FAIR_ORDER}) AS queue_rank, "
            f"row_number() OVER (ORDER BY {_FAIR_ORDER}) AS claim_order "
            "FROM tenant_best), "
            # Each queue's FREE window slots, filled from the top; then the
            # critical reserve, for declared-critical rows the room missed.
            "placed AS (SELECT ranked.*, queue_room.room, SUM(CASE WHEN "
            "ranked.declared_critical AND ranked.queue_rank > queue_room.room "
            "THEN 1 ELSE 0 END) OVER (PARTITION BY ranked.queue "
            "ORDER BY ranked.queue_rank ROWS UNBOUNDED PRECEDING) AS reserve_rank "
            "FROM ranked JOIN queue_room ON queue_room.queue = ranked.queue), "
            "candidate AS (SELECT placed.job_id, placed.claim_order "
            "FROM placed CROSS JOIN settings WHERE placed.queue_rank <= placed.room "
            "OR (placed.declared_critical AND placed.reserve_rank <= LEAST("
            "settings.critical_reserve, placed.room + settings.critical_reserve)) "
            "ORDER BY placed.claim_order LIMIT (SELECT batch_limit FROM settings)), "
            f"claimed AS (UPDATE {self.table} AS delivery "
            "SET publish_status = 'processing', "
            "publish_lease_token = %s, publish_lease_expires_at = %s, "
            "updated_at = %s FROM candidate, settings "
            "WHERE delivery.job_id = candidate.job_id "
            # Re-checked under the row lock: the head was read without one.
            f"{due_row}"
            "RETURNING delivery.job_id, delivery.db_job_id, "
            "delivery.payload_sha256, delivery.signed_envelope, "
            "delivery.tenant_mode, delivery.tenant_id, delivery.queue, "
            "delivery.priority, delivery.publish_attempts, "
            "candidate.claim_order) "
            "SELECT COALESCE(json_agg(claimed ORDER BY claimed.claim_order), "
            "'[]'::json) AS claimed_rows FROM claimed",
            [
                now,
                self.priority_aging_seconds,
                self.broker_window_per_queue,
                self.tenant_window_per_queue,
                self.tenant_urgent_slots_per_queue,
                limit,
                list(self.canonical_queues),
                list(self.canonical_queues),
                token,
                now.add(seconds=self.publish_lease_seconds),
                now,
            ],
        )
    rows = self._row_value(row, "claimed_rows") if row is not None else None
    if isinstance(rows, (str, bytes)):
        rows = json.loads(rows)
    if not rows:
        return None
    return list(rows), token


#: The relay's fair order (see :meth:`_claim_publish_batch`), over
#: ``tenant_best``: the oldest-overdue turn, fewest held window slots, least
#: recently served, aged tier, then age.
_FAIR_ORDER = (
    "tenant_best.is_oldest DESC, tenant_best.held, "
    "tenant_best.last_served_at NULLS FIRST, tenant_best.aged_rank, "
    "tenant_best.available_at, tenant_best.created_at"
)


def _delivery_publish_due(self) -> dict[str, int]:
    """Claim and publish up to ``claim_batch`` due rows in leased batches."""
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
    while remaining > 0:
        claimed = self._claim_publish_batch(remaining)
        if claimed is None:
            break
        rows, token = claimed
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

    After the first error other than an invalid/expired envelope, the rest
    are skipped: a broker outage costs one publish attempt, not one per row.
    """
    outcomes: list[Any] = []
    failed = False
    for row in rows:
        if failed:
            outcomes.append(_UNTRIED)
            continue
        try:
            self._publish_to_broker(row)
        except (DeliveryEnvelopeExpired, DeliveryEnvelopeMismatch) as exc:
            outcomes.append(exc)
        except Exception as exc:
            failed = True
            outcomes.append(exc)
        else:
            outcomes.append(None)
    return outcomes


def _delivery_publish_claimed(self, row: Any, token: str) -> bool:
    """Publish one claimed row and settle it (the single-row path)."""
    job_id = self._publish_to_broker(row)
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


def _delivery_publish_to_broker(self, row: Any) -> str:
    """Verify one claimed row's signed envelope and publish it with confirms."""
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
    self.driver._publish_registered_envelope(
        body,
        payload,
        capability=self,
    )
    return str(payload["job_id"])


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
