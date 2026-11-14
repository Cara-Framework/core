"""The relay's tenant-fair claim: an unlocked PLAN, then a per-queue RESERVE.

Split in two so the expensive half — ranking every backlogged tenant's head
row, O(backlogged tenants) — runs without a lock, once per relay iteration,
and in parallel across relay replicas, while the half that must be serialized
(counting a queue's broker window and leasing rows into it) is a short
statement under one advisory lock PER QUEUE: O(window + candidates), whatever
the backlog. Before (one statement under one global lock), every replica
queued on that lock and a second relay added +36 %, a third and fourth
nothing (1M-row stress 2026-11-13).
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pendulum


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
    2026-11-12). The batch takes each tenant's best rows — one each when
    many tenants have work, up to an equal share of the batch when few do
    (one per tenant made a lone seller's backlog drain at ONE row per relay
    iteration, 77 rows/s, 2026-11-14) — ranks them in the fair order below,
    where a tenant's n-th row ranks as if it already held n-1 more slots,
    and fills each queue's FREE broker-window slots from the top; each row
    counts the same tenant's rows ahead of it against the tenant window.
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

    Between 1 and 2, a tenant's rows take turns across its ``lane_key``
    lanes (a seller's channels, ``Queueable.relay_lane``): the lane holding
    the fewest of the tenant's window slots goes first, so one channel's
    aged backlog never holds another channel's row — that row is the
    tenant's next turn, after a declared-critical row and at most one
    oldest-overdue turn. Live 2026-11-13: a non-stock push of one channel
    waited behind another channel's 9,488 aged ends.

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

    The claim is two statements (:meth:`_plan_publish` then
    :meth:`_reserve_publish`); this method runs both, so one call is the
    fair pick of this instant. The relay loop (:meth:`publish_due`) plans
    once per iteration and reserves from that plan until it is used up.
    """
    limit = max(1, int(limit))
    return self._reserve_publish(self._plan_publish(limit), limit)


def _delivery_plan_publish(self, limit: int) -> list[tuple[str, str]]:
    """Rank the next due rows in the fair order: turn by turn across tenants.

    The EXPENSIVE half of the claim, and it takes no lock: the tenants with
    a due head are found by a recursive skip scan over
    ``queue_job_delivery_due_lane_idx`` (tenants holding only future rows
    are skipped inside the index), each tenant's due ``lane_key`` lanes by a
    second one over the same index (likewise), each lane's head over
    ``queue_job_delivery_lane_head_idx``, and each tenant's
    last service by ``queue_job_delivery_tenant_served_idx`` — a few index
    probes per tenant lane per priority lane, never a walk of a backlogged
    tenant's rows, but O(due tenant lanes) per plan: 20-60 ms at 1,000
    tenants over a 1M-row backlog (scratch stack 2026-11-13); tenants with
    nothing due cost index entries, not probes. The result is a list of job ids in
    claim order, at most ``2 * limit`` per queue (the slack covers rows a
    concurrent relay reserves first); :meth:`_reserve_publish` turns it into
    leases (``(job_id, queue)`` pairs). Nothing here is trusted later: the reserve re-reads every
    candidate, the window and the tenant counts under its lock.
    """
    limit = max(1, int(limit))
    now = pendulum.now("UTC")
    serve_oldest = "TRUE" if self._serve_oldest_next else "FALSE"
    self._serve_oldest_next = not self._serve_oldest_next
    open_row = (
        "AND delivery.status = 'pending' "
        "AND delivery.publish_status IN ('pending','processing') "
    )
    # The plan's entry point: only tenants with a DUE head row. Over every
    # tenant with an OPEN row, tenants holding nothing but future-dated rows
    # (a retry storm's backoffs, a debounce wave, ``later()``, a publish
    # backoff) each cost the plan a probe and diluted a due seller's share
    # to ONE row per iteration (perf c2, 2026-11-14: 471 ms and 1 row at
    # 50k such tenants). ``queue_job_delivery_due_lane_idx`` carries the
    # due instant as a key column, so a not-due tenant is skipped inside the
    # index walk, never visited by the recursion.
    # A due tenant's lanes are found the same way, over its DUE rows: the
    # central tenant holds one lane per shop (webhooks, ``source:<shop>``),
    # so a webhook retry storm left thousands of lanes holding only a
    # future-dated retry, and visiting them cost 474 ms per plan at 50k
    # (``test_shops_waiting_out_a_retry_cost_the_plan_nothing``).
    due_tenant_row = f"{open_row}AND {_DUE_AT} <= settings.now "
    # A lane's due rows, addressed by its full lane key: the open-row
    # conditions prove the partial lane index, the due conditions ride as ONE
    # opaque recheck (see :data:`_DUE_RECHECK`), so no index over the whole
    # (queue, priority) lane can pass for a range scan of its head. One did
    # (``priority_head_idx``, dropped): a stale-statistics plan walked the
    # lane once per tenant lane (``test_relay_claim_plan_stability``).
    lane_due_row = f"{open_row}AND {_DUE_RECHECK} "
    due_lane_row = due_tenant_row
    row = self._db().select_one(
        "WITH RECURSIVE settings AS (SELECT %s::timestamptz AS now, "
        "%s::numeric AS aging_seconds, %s::integer AS broker_window, "
        "%s::integer AS tenant_window, %s::integer AS urgent_slots, "
        "%s::integer AS plan_limit, "
        # A bounded config integer, inlined so the bind order is unchanged.
        f"{int(self.critical_reserve_per_queue)}::integer AS critical_reserve), "
        "priority_lanes(priority, base_rank) AS (VALUES "
        "('critical', 0), ('high', 1), ('default', 2), ('low', 3)), "
        f"{_OUTSTANDING_LANES_CTE.format(table=self.table)}, "
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
        "FROM eligible_queues CROSS JOIN priority_lanes CROSS JOIN settings "
        "CROSS JOIN LATERAL ("
        "SELECT COALESCE(delivery.tenant_id, 0) AS tenant_key "
        f"FROM {self.table} AS delivery "
        "WHERE delivery.queue = eligible_queues.queue "
        "AND delivery.priority = priority_lanes.priority "
        f"{due_tenant_row}"
        "ORDER BY COALESCE(delivery.tenant_id, 0) LIMIT 1) AS first_key "
        # A full window is scanned for the reserve's lane alone.
        "WHERE eligible_queues.room > 0 OR priority_lanes.base_rank = 0 "
        "UNION ALL "
        "SELECT tenant_keys.queue, tenant_keys.priority, "
        "tenant_keys.base_rank, next_key.tenant_key "
        "FROM tenant_keys CROSS JOIN settings CROSS JOIN LATERAL ("
        "SELECT COALESCE(delivery.tenant_id, 0) AS tenant_key "
        f"FROM {self.table} AS delivery "
        "WHERE delivery.queue = tenant_keys.queue "
        "AND delivery.priority = tenant_keys.priority "
        f"{due_tenant_row}"
        "AND COALESCE(delivery.tenant_id, 0) > tenant_keys.tenant_key "
        "ORDER BY COALESCE(delivery.tenant_id, 0) LIMIT 1) AS next_key), "
        # Each tenant's lanes (``lane_key``: a seller's channel) in the same
        # (queue, priority) lane: a second skip scan, so one channel's
        # backlog never hides another channel's head from the plan.
        "lane_keys(queue, priority, base_rank, tenant_key, lane_key) AS ("
        "SELECT tenant_keys.queue, tenant_keys.priority, tenant_keys.base_rank, "
        "tenant_keys.tenant_key, first_lane.lane_key "
        "FROM tenant_keys CROSS JOIN settings CROSS JOIN LATERAL ("
        "SELECT delivery.lane_key "
        f"FROM {self.table} AS delivery "
        "WHERE delivery.queue = tenant_keys.queue "
        "AND delivery.priority = tenant_keys.priority "
        "AND COALESCE(delivery.tenant_id, 0) = tenant_keys.tenant_key "
        f"{due_lane_row}"
        "ORDER BY delivery.lane_key LIMIT 1) AS first_lane "
        "UNION ALL "
        "SELECT lane_keys.queue, lane_keys.priority, lane_keys.base_rank, "
        "lane_keys.tenant_key, next_lane.lane_key "
        "FROM lane_keys CROSS JOIN settings CROSS JOIN LATERAL ("
        "SELECT delivery.lane_key "
        f"FROM {self.table} AS delivery "
        "WHERE delivery.queue = lane_keys.queue "
        "AND delivery.priority = lane_keys.priority "
        "AND COALESCE(delivery.tenant_id, 0) = lane_keys.tenant_key "
        f"{due_lane_row}"
        "AND delivery.lane_key > lane_keys.lane_key "
        "ORDER BY delivery.lane_key LIMIT 1) AS next_lane), "
        # Each tenant's share of the plan: one row each when the plan is
        # crowded, more when few tenants have work. One head per tenant
        # meant ONE row per relay iteration for a lone seller's backlog
        # (77 rows/s, scratch stack 2026-11-14). Counted over tenants with a
        # DUE head only (``tenant_keys``): tenants waiting out a backoff
        # offer nothing, so they no longer shrink anyone's turn.
        "tenant_share AS (SELECT tenant_keys.queue, GREATEST(1, CEIL("
        "MIN(settings.plan_limit)::numeric / "
        "COUNT(DISTINCT tenant_keys.tenant_key)))::integer AS share "
        "FROM tenant_keys CROSS JOIN settings GROUP BY tenant_keys.queue), "
        "heads AS (SELECT head.job_id, head.available_at, head.created_at, "
        "lane_keys.queue, lane_keys.tenant_key, lane_keys.lane_key, "
        "tenant_share.share, "
        "(lane_keys.base_rank = 0) AS declared_critical, "
        "COALESCE(outstanding.held, 0) AS held, "
        "COALESCE(outstanding_lanes.held, 0) AS lane_held, "
        "last_served.published_at AS last_served_at, "
        "GREATEST(lane_keys.base_rank - FLOOR(EXTRACT(EPOCH FROM "
        "(settings.now - head.available_at)) / "
        "settings.aging_seconds), 0) AS aged_rank "
        "FROM lane_keys CROSS JOIN settings "
        "JOIN tenant_share ON tenant_share.queue = lane_keys.queue "
        "LEFT JOIN outstanding ON outstanding.queue = lane_keys.queue "
        "AND outstanding.tenant_key = lane_keys.tenant_key "
        "LEFT JOIN outstanding_lanes ON outstanding_lanes.queue = lane_keys.queue "
        "AND outstanding_lanes.tenant_key = lane_keys.tenant_key "
        "AND outstanding_lanes.lane_key = lane_keys.lane_key "
        "LEFT JOIN LATERAL (SELECT served.published_at "
        f"FROM {self.table} AS served "
        "WHERE served.queue = lane_keys.queue "
        "AND COALESCE(served.tenant_id, 0) = lane_keys.tenant_key "
        "AND served.published_at IS NOT NULL "
        "ORDER BY served.published_at DESC LIMIT 1) AS last_served ON TRUE "
        "CROSS JOIN LATERAL ("
        "SELECT delivery.job_id, delivery.available_at, delivery.created_at "
        f"FROM {self.table} AS delivery "
        "WHERE delivery.queue = lane_keys.queue "
        "AND delivery.priority = lane_keys.priority "
        "AND COALESCE(delivery.tenant_id, 0) = lane_keys.tenant_key "
        "AND delivery.lane_key = lane_keys.lane_key "
        f"{lane_due_row}"
        "ORDER BY delivery.available_at, delivery.created_at "
        # Up to the tenant's share, never past what its tenant window
        # leaves this lane (the reserve re-checks it under the lock).
        f"LIMIT {_LANE_ROOM}"
        ") AS head "
        f"WHERE {_TENANT_WINDOW_OPEN.format(held='COALESCE(outstanding.held, 0)', rank='lane_keys.base_rank', tenant='lane_keys.tenant_key', critical=int(self._PRIORITY_RANKS['critical']), high=int(self._PRIORITY_RANKS['high']))}), "
        # The oldest-overdue turn: this head ranks first in its plan.
        "oldest AS (SELECT heads.job_id FROM heads CROSS JOIN settings "
        f"WHERE {serve_oldest} AND 2 * EXTRACT(EPOCH FROM "
        "(settings.now - heads.available_at)) >= settings.aging_seconds "
        "ORDER BY heads.available_at LIMIT 1), "
        # A lane's rows in its own order (aged tier, age).
        "lane_best AS (SELECT heads.*, (oldest.job_id IS NOT NULL) AS is_oldest, "
        "row_number() OVER (PARTITION BY heads.queue, heads.tenant_key, "
        "heads.lane_key ORDER BY (oldest.job_id IS NOT NULL) DESC, "
        "heads.declared_critical DESC, heads.aged_rank, heads.available_at, "
        "heads.created_at) AS lane_turn "
        "FROM heads LEFT JOIN oldest ON oldest.job_id = heads.job_id), "
        # A tenant's rows: a declared-critical row first, then its lanes in
        # turn — the lane holding the FEWEST window slots, counting its rows
        # ahead in this plan, goes first — then aged tier, age. One
        # channel's aged backlog therefore never sits ahead of another
        # channel's single row (backlog P2, 2026-11-13): that row is the
        # tenant's next turn but for the oldest-overdue turn.
        "tenant_best AS (SELECT lane_best.*, "
        "row_number() OVER (PARTITION BY lane_best.queue, lane_best.tenant_key "
        "ORDER BY lane_best.is_oldest DESC, lane_best.declared_critical DESC, "
        "lane_best.lane_held + lane_best.lane_turn, lane_best.aged_rank, "
        "lane_best.available_at, lane_best.created_at) AS tenant_turn "
        "FROM lane_best), "
        "ranked AS (SELECT tenant_best.job_id, tenant_best.queue, "
        "row_number() OVER (PARTITION BY tenant_best.queue "
        f"ORDER BY {_FAIR_ORDER}) AS queue_rank, "
        f"row_number() OVER (ORDER BY {_FAIR_ORDER}) AS claim_order "
        "FROM tenant_best WHERE tenant_best.tenant_turn <= tenant_best.share) "
        "SELECT COALESCE(json_agg(json_build_array(ranked.job_id, ranked.queue) "
        "ORDER BY ranked.claim_order), "
        "'[]'::json) AS planned FROM ranked CROSS JOIN settings "
        "WHERE ranked.queue_rank <= settings.plan_limit",
        [
            now,
            self.priority_aging_seconds,
            self.broker_window_per_queue,
            self.tenant_window_per_queue,
            self.tenant_urgent_slots_per_queue,
            2 * limit,
            list(self.canonical_queues),
            list(self.canonical_queues),
        ],
    )
    planned = self._row_value(row, "planned") if row is not None else None
    if isinstance(planned, (str, bytes)):
        planned = json.loads(planned)
    return [(str(job_id), str(queue)) for job_id, queue in planned or ()]


def _delivery_reserve_publish(
    self,
    planned: list[tuple[str, str]],
    limit: int,
) -> tuple[list[Any], str] | None:
    """Lease up to ``limit`` of ``planned`` ``(job_id, queue)`` pairs, in order.

    The CHEAP half of the claim, and the only part that is serialized: it
    takes one advisory transaction lock per canonical queue (DB
    ``advisory_xact_lock``), so replicas reserving different queues never
    wait on each other and replicas of one queue wait only for this
    statement — the window count plus primary-key reads of the candidates,
    O(window + candidates), whatever the backlog. Under the lock it
    re-reads every candidate (still due, unleased), each queue's free
    broker-window slots and each tenant's held slots, then applies exactly
    the placement rules of the documented claim: the tenant window, the
    urgent slots, the critical reserve, free room filled in plan order.
    ``publish_status=processing`` stays the durable window reservation, so
    replicas cannot overfill RabbitMQ.
    """
    if not planned:
        return None
    limit = max(1, int(limit))
    token = uuid.uuid4().hex
    queues = sorted({queue for _job_id, queue in planned})
    database = self._db()
    with database.transaction():
        database.advisory_xact_lock(
            *(f"{self.table}:publish-window:{queue}" for queue in queues)
        )
        # Read AFTER the locks: this statement's snapshot sees every
        # reservation another replica committed while this one waited.
        now = pendulum.now("UTC")
        row = database.select_one(
            "WITH settings AS (SELECT %s::timestamptz AS now, "
            "%s::integer AS broker_window, %s::integer AS tenant_window, "
            "%s::integer AS urgent_slots, %s::integer AS batch_limit, "
            f"{int(self.critical_reserve_per_queue)}::integer AS critical_reserve), "
            f"{_OUTSTANDING_CTE.format(table=self.table)}, "
            "queue_room AS (SELECT candidate.queue, settings.broker_window - "
            "COALESCE((SELECT SUM(outstanding.held) FROM outstanding "
            "WHERE outstanding.queue = candidate.queue), 0) AS room "
            "FROM unnest(%s::varchar[]) AS candidate(queue) CROSS JOIN settings), "
            "planned AS (SELECT planned.job_id, planned.claim_order "
            "FROM unnest(%s::varchar[]) WITH ORDINALITY "
            "AS planned(job_id, claim_order)), "
            "live AS (SELECT planned.claim_order, delivery.job_id, delivery.queue, "
            "delivery.row_ctid, (delivery.priority = 'critical') AS declared_critical, "
            "COALESCE(delivery.tenant_id, 0) AS tenant_key, "
            f"{_DECLARED_RANK} AS declared_rank, "
            "COALESCE(outstanding.held, 0) AS held "
            "FROM planned CROSS JOIN settings CROSS JOIN LATERAL ("
            "SELECT delivery.ctid AS row_ctid, delivery.job_id, delivery.queue, "
            f"delivery.priority, delivery.tenant_id FROM {self.table} AS delivery "
            # One primary-key probe per candidate. ``LIMIT 1`` keeps the
            # planner from flattening this into a hash join over the whole
            # open set, which the unsampled state columns price at one row
            # (676 ms at 1M open rows, scratch stack 2026-11-13).
            "WHERE delivery.job_id = planned.job_id "
            f"AND {_DUE_RECHECK} LIMIT 1) AS delivery "
            "LEFT JOIN outstanding ON outstanding.queue = delivery.queue "
            "AND outstanding.tenant_key = COALESCE(delivery.tenant_id, 0)), "
            # A plan may offer several rows of one tenant: each counts the
            # tenant's rows ahead of it in this batch, by declared tier.
            "counted AS (SELECT live.*, "
            "COUNT(*) FILTER (WHERE live.declared_rank = 0) OVER tenant_ahead "
            "AS ahead_critical, "
            "COUNT(*) FILTER (WHERE live.declared_rank = 1) OVER tenant_ahead "
            "AS ahead_high, "
            "COUNT(*) FILTER (WHERE live.declared_rank >= 2) OVER tenant_ahead "
            "AS ahead_bulk FROM live WINDOW tenant_ahead AS ("
            "PARTITION BY live.queue, live.tenant_key ORDER BY live.claim_order "
            "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)), "
            "eligible AS (SELECT counted.* FROM counted CROSS JOIN settings "
            f"WHERE {_TENANT_WINDOW_OPEN.format(held=_HELD_WITH_AHEAD, rank='counted.declared_rank', tenant='counted.tenant_key', critical=int(self._PRIORITY_RANKS['critical']), high=int(self._PRIORITY_RANKS['high']))}), "
            "placed AS (SELECT eligible.*, queue_room.room, row_number() OVER "
            "(PARTITION BY eligible.queue ORDER BY eligible.claim_order) AS queue_rank "
            "FROM eligible JOIN queue_room ON queue_room.queue = eligible.queue), "
            # Each queue's FREE window slots, filled in plan order; then the
            # critical reserve, for declared-critical rows the room missed.
            "reserved AS (SELECT placed.*, SUM(CASE WHEN "
            "placed.declared_critical AND placed.queue_rank > placed.room "
            "THEN 1 ELSE 0 END) OVER (PARTITION BY placed.queue "
            "ORDER BY placed.queue_rank ROWS UNBOUNDED PRECEDING) AS reserve_rank "
            "FROM placed), "
            "candidate AS (SELECT reserved.job_id, reserved.row_ctid, "
            "reserved.claim_order "
            "FROM reserved CROSS JOIN settings "
            "WHERE reserved.queue_rank <= reserved.room "
            "OR (reserved.declared_critical AND reserved.reserve_rank <= LEAST("
            "settings.critical_reserve, reserved.room + settings.critical_reserve)) "
            "ORDER BY reserved.claim_order LIMIT (SELECT batch_limit FROM settings)), "
            f"claimed AS (UPDATE {self.table} AS delivery "
            "SET publish_status = 'processing', "
            "publish_lease_token = %s, publish_lease_expires_at = %s, "
            "updated_at = %s FROM candidate, settings "
            "WHERE delivery.job_id = candidate.job_id "
            # Addressed by the row versions ``live`` read: a TID scan, never
            # a plan over the open set, whatever the planner thinks the
            # unsampled state columns select. A row updated since then has a
            # new version, fails this match and is simply not claimed.
            "AND delivery.ctid = ANY(ARRAY(SELECT candidate.row_ctid "
            "FROM candidate)) "
            # Re-checked under the row lock.
            f"AND {_DUE_RECHECK} "
            "RETURNING delivery.job_id, delivery.db_job_id, "
            "delivery.payload_sha256, delivery.signed_envelope, "
            "delivery.tenant_mode, delivery.tenant_id, delivery.queue, "
            "delivery.priority, delivery.publish_attempts, "
            "candidate.claim_order) "
            "SELECT COALESCE(json_agg(claimed ORDER BY claimed.claim_order), "
            "'[]'::json) AS claimed_rows FROM claimed",
            [
                now,
                self.broker_window_per_queue,
                self.tenant_window_per_queue,
                self.tenant_urgent_slots_per_queue,
                limit,
                queues,
                queues,
                [job_id for job_id, _queue in planned],
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


#: "Leasable now": pending, past its release and publish backoff, unexpired,
#: and not under a live publish lease. ``settings.now`` is the statement's
#: clock.
_DUE_ROW = (
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

#: The instant a row becomes leasable but for its lease and expiry: past its
#: release AND its publish backoff. Written exactly as the key expression of
#: ``queue_job_delivery_due_lane_idx`` so the plan's tenant and lane walks
#: match it.
_DUE_AT = "GREATEST(delivery.available_at, delivery.publish_retry_at)"

#: :data:`_DUE_ROW` as ONE opaque boolean, for statements that address their
#: rows by key. ``IS TRUE`` keeps the planner from matching its state
#: conditions to an index: ``status``/``publish_status`` carry no statistics
#: (``queue_job_delivery_queue_state_statistics``), so it prices them at one
#: row and prefers walking ``queue_job_delivery_publish_due_idx`` over the
#: whole open set (290-680 ms per reserve at 1M open rows, scratch stack
#: 2026-11-13) to the handful of key probes. It still re-checks every row,
#: under the row lock too, exactly like the plain conditions.
_DUE_RECHECK = "(TRUE " + _DUE_ROW + ") IS TRUE"

#: Broker-window slots held per (queue, tenant): published or under a live
#: publish lease, and not yet terminal. Bounded by the window, never the
#: backlog (``queue_job_delivery_broker_window_idx``).
_OUTSTANDING_CTE = (
    "outstanding AS (SELECT outstanding.queue, "
    "COALESCE(outstanding.tenant_id, 0) AS tenant_key, COUNT(*) AS held "
    "FROM {table} AS outstanding CROSS JOIN settings "
    "WHERE outstanding.queue = ANY(%s::varchar[]) "
    "AND outstanding.status IN ('pending', 'processing') "
    "AND (outstanding.publish_status = 'published' OR "
    "(outstanding.publish_status = 'processing' AND "
    "outstanding.publish_lease_expires_at > settings.now)) "
    "GROUP BY outstanding.queue, COALESCE(outstanding.tenant_id, 0))"
)

#: :data:`_OUTSTANDING_CTE` per (queue, tenant, ``lane_key``) as
#: ``outstanding_lanes``, and summed per (queue, tenant) as ``outstanding``:
#: the plan ranks a tenant's lanes by the slots each holds.
_OUTSTANDING_LANES_CTE = (
    "outstanding_lanes AS (SELECT outstanding.queue, "
    "COALESCE(outstanding.tenant_id, 0) AS tenant_key, outstanding.lane_key, "
    "COUNT(*) AS held "
    "FROM {table} AS outstanding CROSS JOIN settings "
    "WHERE outstanding.queue = ANY(%s::varchar[]) "
    "AND outstanding.status IN ('pending', 'processing') "
    "AND (outstanding.publish_status = 'published' OR "
    "(outstanding.publish_status = 'processing' AND "
    "outstanding.publish_lease_expires_at > settings.now)) "
    "GROUP BY outstanding.queue, COALESCE(outstanding.tenant_id, 0), "
    "outstanding.lane_key), "
    "outstanding AS (SELECT outstanding_lanes.queue, outstanding_lanes.tenant_key, "
    "SUM(outstanding_lanes.held)::bigint AS held FROM outstanding_lanes "
    "GROUP BY outstanding_lanes.queue, outstanding_lanes.tenant_key)"
)

#: Whether a tenant holding ``held`` slots may take one more for a row of
#: DECLARED tier ``rank``: under the tenant window, plus the urgent slots for
#: ``high``/``critical`` and the critical reserve for ``critical`` — by the
#: declared tier, never the aged one. Central rows (tenant key 0) are uncapped.
_TENANT_WINDOW_OPEN = (
    "(settings.tenant_window <= 0 OR {tenant} = 0 "
    "OR {held} < settings.tenant_window "
    "+ CASE WHEN {rank} = {critical} "
    "THEN settings.urgent_slots + settings.critical_reserve "
    "WHEN {rank} = {high} THEN settings.urgent_slots ELSE 0 END)"
)

#: The slots a tenant holds once the batch rows ahead of this one are leased:
#: an UPPER bound, so a batch never overfills a tenant window. A row ahead of
#: a lower tier counts in full (it can be refused only where this row would
#: be too); rows ahead of a HIGHER tier count at most the slots their own
#: tier could take, so a tenant's bulk rows refused at a full tenant window
#: never cost its urgent or critical row the slot reserved for it.
_HELD_WITH_AHEAD = (
    "(counted.held + counted.ahead_critical "
    "+ CASE WHEN counted.declared_rank = 0 THEN LEAST(counted.ahead_high, "
    "GREATEST(settings.tenant_window + settings.urgent_slots - counted.held, 0)) "
    "ELSE counted.ahead_high END "
    "+ CASE WHEN counted.declared_rank <= 1 THEN LEAST(counted.ahead_bulk, "
    "GREATEST(settings.tenant_window - counted.held, 0)) "
    "ELSE counted.ahead_bulk END)"
)

#: A delivery row's declared tier as the claim's ``base_rank``.
_DECLARED_RANK = (
    "(CASE delivery.priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1 "
    "WHEN 'default' THEN 2 ELSE 3 END)"
)


#: How many rows one tenant's lane may offer the plan: its share, never more
#: than the tenant window (plus urgent slots / critical reserve for the
#: declared tier) leaves it. Central rows and an unset tenant window are
#: bounded by the share alone.
_LANE_ROOM = (
    "GREATEST(1, CASE WHEN settings.tenant_window <= 0 "
    "OR lane_keys.tenant_key = 0 THEN tenant_share.share "
    "ELSE LEAST(tenant_share.share, settings.tenant_window "
    "+ CASE lane_keys.base_rank "
    "WHEN 0 THEN settings.urgent_slots + settings.critical_reserve "
    "WHEN 1 THEN settings.urgent_slots ELSE 0 END "
    "- COALESCE(outstanding.held, 0)) END)"
)


#: The fair order over every tenant's planned rows: the oldest-overdue turn,
#: then FEWEST held slots counting the rows ahead of it in this plan (a
#: tenant's n-th row ranks as if it held n-1 more), so turn 1 of every
#: tenant at the same held count goes before anyone's turn 2 — the round
#: robin the one-row-per-statement claim made — then least recently served,
#: aged tier, age.
_FAIR_ORDER = (
    "tenant_best.is_oldest DESC, tenant_best.held + tenant_best.tenant_turn, "
    "tenant_best.last_served_at NULLS FIRST, tenant_best.aged_rank, "
    "tenant_best.available_at, tenant_best.created_at"
)
