"""Startup-time answer to "is anything actually publishing my dispatches?".

WHY THIS EXISTS (2026-07-20 incident)
-------------------------------------
``Bus.dispatch`` does not talk to RabbitMQ. It commits a row to the
``queue_job_delivery`` outbox, and ``queue:relay`` is the ONLY process
that turns those rows into broker messages. ``queue:work`` consumes the
other end.

The two names read like a matched pair, so an operator who starts
``queue:work`` reasonably concludes "the queue is being processed" — and
gets a worker politely listening to an empty broker while every dispatch
piles up unpublished. Every trigger reports "started", nothing runs, and
no surface contradicts it. That is exactly how 1250 jobs accumulated
silently.

DIVISION OF LABOUR
------------------
``QueueOutboxHealth`` (app-side, scheduler-driven, once a minute) is the
CONTINUOUS alarm for this condition. This probe is the STARTUP one: it
speaks at the one moment the operator is actually looking at a terminal,
having just typed the command they believe is sufficient. The continuous
alarm cannot cover that moment — a fresh worker start is precisely when
the human is present and the feedback is cheapest.

Both read the SAME due predicate (``QueueJobDeliveryStore.backlog_metrics``)
and the SAME thresholds (``queue.outbox_stall_*``), so the two surfaces
can never disagree about what "stalled" means.

ADVISORY ONLY — NEVER FATAL
---------------------------
This probe never raises, never exits, never blocks. The same incident
included a worker role that could not start AT ALL because a metrics port
was contended (cara a92e293): a diagnostic that can kill its own host
process is a worse bug than the one it reports. Every failure mode here
degrades to silence.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Any

from cara.configuration import config
from cara.facades import Log


class PublicationBacklogProbe:
    """Sample, judge and announce outbox backlog from a NON-relay process."""

    #: Thresholds are deliberately the same knobs the continuous
    #: scheduler-side alarm uses. One definition of "stalled", one place
    #: for an operator to tune it.
    AGE_BUDGET_CONFIG_KEY = "queue.outbox_stall_age_seconds"
    MIN_PENDING_CONFIG_KEY = "queue.outbox_stall_min_pending"

    # ── configuration ────────────────────────────────────────────────
    @classmethod
    def age_budget_seconds(cls) -> int:
        """How long a due row may wait before the backlog is suspicious."""
        return max(int(config(cls.AGE_BUDGET_CONFIG_KEY, 300)), 1)

    @classmethod
    def min_pending(cls) -> int:
        """How many aged rows must be present before we interrupt anyone."""
        return max(int(config(cls.MIN_PENDING_CONFIG_KEY, 1)), 1)

    # ── sampling ─────────────────────────────────────────────────────
    @staticmethod
    def sample() -> dict[str, Any] | None:
        """Return the due-backlog snapshot, or ``None`` if unavailable.

        ``None`` covers two different "cannot answer" cases on purpose —
        a product that does not deploy the delivery ledger at all, and a
        transient failure to reach it. Neither is something a startup
        advisory may escalate.
        """
        from cara.facades import Queue

        store = Queue.driver("amqp").delivery_store
        return store.backlog_metrics_if_installed()

    # ── judgement ────────────────────────────────────────────────────
    @staticmethod
    def advisory(
        snapshot: dict[str, Any] | None,
        *,
        age_budget_seconds: int,
        min_pending: int,
    ) -> str | None:
        """Pure verdict. ``None`` means nothing worth interrupting for.

        BOTH gates must trip. The AGE gate is what keeps ordinary traffic
        quiet: a worker started in the middle of a healthy 5000-job burst
        must say nothing, or operators learn to ignore the banner and we
        have rebuilt the original silence with extra steps.
        """
        if not snapshot:
            return None
        try:
            count = int(snapshot.get("count") or 0)
            age = float(snapshot.get("age") or 0.0)
        except TypeError, ValueError:
            return None
        if count < min_pending or age < age_budget_seconds:
            return None
        return (
            f"{count} job(s) are committed to the queue publication outbox "
            f"and the oldest has been due for {int(age)}s "
            f"(budget {int(age_budget_seconds)}s).\n"
            "`queue:work` only CONSUMES from RabbitMQ — it never publishes. "
            "Dispatched work reaches the broker ONLY while `craft queue:relay` "
            "is also running, so a backlog this old means the relay is not.\n"
            "This worker will keep running and will pick the jobs up the "
            "moment the relay drains the outbox."
        )

    # ── orchestration ────────────────────────────────────────────────
    @classmethod
    def announce(cls, emit: Callable[[str], None] | None = None) -> str | None:
        """Sample → judge → announce. Returns the message, or ``None``.

        ``emit`` is the operator-facing channel (the command's console).
        It is separate from the log line because the two have different
        jobs: the console is what the human reads in the next two
        seconds, the log is what survives to the postmortem.
        """
        try:
            snapshot = cls.sample()
        except Exception as exc:  # noqa: BLE001 — advisory must never be fatal
            Log.debug(
                "Queue publication backlog probe could not sample the "
                "delivery ledger; skipping the startup advisory: %s",
                exc,
                category="cara.queue.delivery",
            )
            return None

        message = cls.advisory(
            snapshot,
            age_budget_seconds=cls.age_budget_seconds(),
            min_pending=cls.min_pending(),
        )
        if message is None:
            return None

        Log.warning(
            "Queue publication outbox is backlogged at worker startup: %s",
            message.replace("\n", " "),
            category="cara.queue.delivery",
        )
        if emit is not None:
            # A broken console must not cost us the log line above, nor
            # the worker start below.
            with contextlib.suppress(Exception):
                emit(message)
        return message


__all__ = ["PublicationBacklogProbe"]
