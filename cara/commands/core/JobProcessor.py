"""Public queue job processor facade."""

from __future__ import annotations

import asyncio
import os
import threading
import time

from cara.context import Tenancy
from cara.exceptions import QueueException
from cara.facades import Log
from cara.facades import Queue as _Queue
from cara.queues import DEFAULT_MAX_ATTEMPTS as _RETRY_DEFAULT_MAX_ATTEMPTS
from cara.queues import (
    DEFAULT_MAX_THROTTLE_ATTEMPTS as _RETRY_DEFAULT_MAX_THROTTLE_ATTEMPTS,
)
from cara.queues import DEFAULT_RETRY_BACKOFF_SECONDS as _RETRY_DEFAULT_BACKOFF_SECONDS
from cara.queues import MAX_AMQP_JOB_PAYLOAD_BYTES

from . import _JobExecution
from .ActiveJobCancellationRegistry import ActiveJobCancellationRegistry
from .DeliverySettlementError import DeliverySettlementError


class JobProcessor:
    """Processes individual jobs from queue messages (Single Responsibility)."""

    # Class-level constants for job execution
    DEFAULT_JOB_TIMEOUT = 300
    MAX_PAYLOAD_SIZE = MAX_AMQP_JOB_PAYLOAD_BYTES
    _SETTLEMENT_BACKOFF_SECONDS = (0.05, 0.25, 1.0, 2.0, 5.0)

    @staticmethod
    def _broker_ack(channel, delivery_tag) -> None:
        try:
            channel.basic_ack(delivery_tag=delivery_tag)
        except Exception as exc:
            raise DeliverySettlementError("Broker ACK outcome is unknown.") from exc

    @staticmethod
    def _broker_nack(channel, delivery_tag, *, requeue: bool) -> None:
        try:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
        except Exception as exc:
            raise DeliverySettlementError("Broker NACK outcome is unknown.") from exc

    @staticmethod
    def _retry_settlement_step(description: str, callback) -> None:
        last_error: Exception | None = None
        for attempt, delay in enumerate(
            (0.0, *JobProcessor._SETTLEMENT_BACKOFF_SECONDS),
            start=1,
        ):
            if delay:
                time.sleep(delay)
            try:
                callback()
                return
            except Exception as exc:
                last_error = exc
                if attempt <= len(JobProcessor._SETTLEMENT_BACKOFF_SECONDS):
                    Log.warning(
                        "%s failed on settlement attempt %s; retrying: %s",
                        description,
                        attempt,
                        exc,
                        category="cara.queue.delivery",
                    )
        raise DeliverySettlementError(
            f"{description} remained unavailable."
        ) from last_error

    def __init__(
        self,
        cancellation_registry: ActiveJobCancellationRegistry | None = None,
    ) -> None:
        self.cancellation_registry = cancellation_registry

    @staticmethod
    def _execute_async_job_with_timeout(
        method_to_call,
        init_args,
        timeout_seconds,
        cancellation_registry: ActiveJobCancellationRegistry | None = None,
    ):
        """Execute async job with timeout enforcement.

        Wraps the coroutine in ``asyncio.wait_for(...)`` so a hung
        ``await`` (a stuck HTTP call without its own timeout, a DB
        operation behind a dead pool) actually surfaces as a
        ``TimeoutError`` after ``timeout_seconds``. Pre-fix this
        helper accepted ``timeout_seconds`` but only used it in the
        exception message — ``asyncio.run(method_to_call(...))`` ran
        without a cap, so an async job that genuinely hung held its
        worker slot indefinitely. The active worker dispatch path
        (``handle`` body) already wraps in ``wait_for``; this static
        is here so anyone reaching for it (debug shim, alternative
        worker mode, unit test) gets the same contract.
        """

        async def _run_registered():
            token = (
                cancellation_registry.register_current()
                if cancellation_registry is not None
                else None
            )
            try:
                return await asyncio.wait_for(
                    method_to_call(*init_args), timeout=timeout_seconds
                )
            finally:
                if token is not None:
                    cancellation_registry.unregister(token)

        hard_kill = threading.Timer(
            float(timeout_seconds) + 5.0,
            JobProcessor._hard_kill_uncooperative_timeout,
            kwargs={"timeout_seconds": timeout_seconds},
        )
        hard_kill.daemon = True
        hard_kill.start()
        try:
            return asyncio.run(_run_registered())
        except TimeoutError as e:
            raise TimeoutError(f"Async job exceeded timeout of {timeout_seconds}s") from e
        finally:
            hard_kill.cancel()

    @staticmethod
    def _hard_kill_uncooperative_timeout(*, timeout_seconds: float) -> None:
        """Kill the worker if coroutine cancellation cannot stop the handler."""
        Log.error(
            "Queue handler ignored cancellation after its %ss timeout; "
            "terminating the worker before its DB lease can be recovered.",
            timeout_seconds,
            category="cara.queue.delivery",
        )
        os._exit(getattr(os, "EX_TEMPFAIL", 75))

    # Framework-default retry policy used when the failing job does not
    # declare its own ``max_attempts`` / ``retry_backoff``. SINGLE-SOURCED
    # from ``cara.queues.retry.Policy`` so this production worker and
    # ``AMQPDriver`` can no longer drift apart — they previously kept
    # hand-copied constants "in lockstep" by comment only.
    DEFAULT_MAX_ATTEMPTS = _RETRY_DEFAULT_MAX_ATTEMPTS
    DEFAULT_RETRY_BACKOFF_SECONDS = _RETRY_DEFAULT_BACKOFF_SECONDS
    DEFAULT_MAX_THROTTLE_ATTEMPTS = _RETRY_DEFAULT_MAX_THROTTLE_ATTEMPTS

    @staticmethod
    def _envelope_counter(msg, key: str) -> int:
        """Read a non-negative integer counter out of a job envelope.

        Envelopes signed before a counter existed simply do not carry the
        key, and a corrupted producer can stamp it ``None`` or a string;
        both must read as 0 rather than explode inside the failure router,
        because the router is the last thing standing between a failed job
        and a lost delivery. Negative values are clamped: a negative index
        would silently select the LAST backoff entry instead of the first.
        """
        if not msg:
            return 0
        try:
            raw = msg.get(key, 0)
            return max(0, int(raw if raw is not None else 0))
        except TypeError, ValueError:
            return 0

    @staticmethod
    def _should_retry_job(msg, instance, exc: Exception) -> bool:
        """Decide whether a failed message should be republished with a delay.

        ``msg["attempts"]`` is the *attempts-already-made* counter
        (AMQPDriver.push stamps it 0; each retry republish bumps it).
        The cap is whatever the job class declares via ``max_attempts``
        (default :data:`DEFAULT_MAX_ATTEMPTS`).

        Throttles are budgeted SEPARATELY. A throttle deliberately leaves
        ``attempts`` frozen (see _requeue_with_delay), which used to mean
        the budget check below was permanently ``1 < 3`` → True: a job
        starved by a sustained concurrency limit re-queued itself forever,
        writing a delivery row plus a job UPDATE and a broker publish every
        few seconds with no terminal signal to anyone. ``throttle_attempts``
        is the throttle lane's own counter, capped by the job's
        ``max_throttle_attempts`` (default
        :data:`DEFAULT_MAX_THROTTLE_ATTEMPTS`), so sustained starvation now
        ends in the DLQ where operators can see it.

        Pre-fix this read ``msg["attempt"]`` (singular — a key nothing
        ever set) and compared it to ``msg["attempts"]`` as if that
        held the cap, so the comparison was always ``1 < 0`` → False
        and every first failure was ACKed straight to the DLQ, bypassing
        the worker's retry schedule.
        """
        if not msg:
            return False
        if getattr(exc, "is_throttle", False):
            # The two budgets are ORTHOGONAL, and that is safe only because
            # ``throttle_attempts`` survives the signed envelope: this branch
            # bounds starvation on its own counter, so it is not a bypass of
            # the failure budget. It also cannot buy the job an extra
            # EXECUTION — a throttle means the job never ran, so no throttle
            # chain can produce more than ``max_attempts`` actual runs. Making
            # the two conjunctive instead would dead-letter a job that still
            # has a run coming to it, purely because a gate happened to be shut
            # when its turn arrived.
            # If the envelope ever stops carrying the counter, this read is
            # permanently 0 and the bound becomes fiction — that hop is pinned
            # in tests/queues/test_signed_json_job_serializer.py.
            max_throttle_attempts = int(
                getattr(instance, "max_throttle_attempts", None)
                or JobProcessor.DEFAULT_MAX_THROTTLE_ATTEMPTS
            )
            throttle_done = JobProcessor._envelope_counter(msg, "throttle_attempts")
            return throttle_done + 1 < max_throttle_attempts
        attempts_done = JobProcessor._envelope_counter(msg, "attempts")
        max_attempts = int(
            getattr(instance, "max_attempts", None) or JobProcessor.DEFAULT_MAX_ATTEMPTS
        )
        # ``do_not_retry`` on the failing exception is honoured one
        # level up (see _requeue_with_delay) — we only answer the
        # "budget remaining" question here.
        return attempts_done + 1 < max_attempts

    @staticmethod
    def _requeue_with_delay(
        channel,
        method_frame,
        msg,
        instance,
        exc: Exception,
        queue_name: str | None,
        delivery_lease_token: str | None,
        tracker,
        db_job_id: int,
    ) -> None:
        """Atomically accept a new delayed delivery, then ACK the source.

        ``basic_nack(requeue=True)`` puts the message back on the queue
        head immediately. With ``prefetch=1`` the same worker thread
        re-claims it on the very next iteration — a poison message
        loops at 100% CPU. The Cara contract is ``republish-with-
        backoff`` (1s / 5s / 30s by default, jittered), which only
        works when we persist the retry in the durable delayed-job
        outbox. We ACK the original delivery only after that database
        commit and stamp the new message with
        ``attempts = attempts_done + 1`` so the next failure can decide
        budget correctly.

        The envelope carries TWO counters, not one. ``attempts`` is the
        failure budget; ``throttle_attempts`` is the starvation budget.
        They were the same integer once, and because a throttle correctly
        leaves ``attempts`` frozen, the backoff index derived from it was
        frozen too — every throttled redelivery came back after the FIRST
        schedule entry (1 second) forever, and the failure budget never
        moved, so nothing ever dead-lettered. Two counters means the
        throttle lane escalates through the same 1s/5s/30s schedule and
        still never spends the failure budget.
        """
        # The delivery ledger commits the new row and the source
        # ``retry_scheduled`` terminal transition in ONE DB transaction. Only
        # after that durable acceptance may the broker source be ACKed.
        attempts_done = JobProcessor._envelope_counter(msg, "attempts")
        throttle_attempts = JobProcessor._envelope_counter(msg, "throttle_attempts")
        # Throttle-class exceptions (``ConcurrencyExceeded`` raised by
        # the ``ConcurrencyLimited`` middleware, future per-host rate-
        # limit middleware) signal "the job never got a slot, try again
        # later" — not a job failure. Bumping ``attempts`` for these
        # would DLQ healthy jobs purely from losing the slot lottery.
        # The middleware contract is the source of truth (it declares
        # ``is_throttle = True`` on the exception class); we read it
        # via ``getattr`` so the check stays loose-coupled and future
        # throttle classes opt in for free.
        is_throttle = bool(getattr(exc, "is_throttle", False))
        next_attempt = attempts_done if is_throttle else attempts_done + 1
        # A real failure clears the starvation counter: the job DID get a
        # slot and ran, so whatever starvation preceded it is history.
        next_throttle = throttle_attempts + 1 if is_throttle else 0

        backoff_schedule = getattr(
            instance,
            "retry_backoff",
            JobProcessor.DEFAULT_RETRY_BACKOFF_SECONDS,
        )
        if not isinstance(backoff_schedule, (list, tuple)) or not backoff_schedule:
            backoff_schedule = JobProcessor.DEFAULT_RETRY_BACKOFF_SECONDS
        # Both counters advance the SAME schedule — a redelivery is a
        # redelivery, whether it was caused by a fault or by starvation.
        idx = min(attempts_done + throttle_attempts, len(backoff_schedule) - 1)
        base_delay = int(backoff_schedule[idx])
        # A throttle that knows WHEN its gate reopens outranks the generic
        # schedule. ``ThrottlesExceptions`` opens after ``retry_after``
        # seconds (300 by default); coming back on the 1s/5s/30s schedule
        # just spends the starvation budget knocking on a closed door.
        retry_after = getattr(exc, "retry_after", None)
        if is_throttle and isinstance(retry_after, (int, float)) and retry_after > 0:
            base_delay = max(base_delay, int(retry_after))

        try:
            driver = _Queue.driver()
            # ``AMQPDriver`` exposes ``_apply_retry_jitter`` for the
            # full-jitter spread; fall back to the unjittered delay if
            # we're running against a different driver.
            delay_seconds = base_delay
            apply_jitter = getattr(driver, "_apply_retry_jitter", None)
            if callable(apply_jitter):
                try:
                    delay_seconds = apply_jitter(base_delay, instance)
                except Exception:
                    delay_seconds = base_delay

            retry_options = {
                "queue": queue_name or msg.get("queue") or "default",
                "attempts": next_attempt,
                "throttle_attempts": next_throttle,
                "_otel": msg.get("_otel") or {},
                "db_job_id": msg.get("db_job_id"),
                "source_delivery_job_id": msg.get("job_id"),
                "source_delivery_lease_token": delivery_lease_token,
                # Both counters are in the dedup key. With ``attempts``
                # alone a throttle chain minted the SAME key on every
                # cycle (the counter is deliberately frozen) and only
                # stayed unique by accident, because the source job_id
                # happened to rotate.
                "deduplication_key": (
                    f"retry:{msg.get('job_id')}:{next_attempt}:{next_throttle}"
                ),
                "unique_key": msg.get("unique_key"),
            }
            if msg.get("_tenant_mode") == "tenant":
                retry_options["tenant_id"] = msg.get("_tenant")
            # Carry the original ``callback`` / ``args`` through to the
            # republished payload. Pre-fix the retry options ONLY held
            # ``queue`` and ``attempts``, so AMQPDriver.push fell back
            # to defaults (``callback="handle"``, ``args=()``). Any job
            # dispatched with ``Bus.dispatch(job, callback="custom",
            # args=(123,))`` retried against ``handle()`` with no args
            # — silent semantic drift on every retry path. Only
            # propagate keys the original payload actually set so we
            # don't override driver defaults with empty values.
            if "callback" in msg:
                retry_options["callback"] = msg["callback"]
            if "args" in msg:
                retry_options["args"] = msg["args"]
            if "created" in msg:
                retry_options["created"] = msg["created"]
            # ``later`` is the Laravel-compatible delay entry point. The AMQP
            # driver commits a signed row to PostgreSQL; only then may this
            # worker acknowledge the failed source delivery.
            if msg.get("_tenant_mode") == "central":
                with Tenancy.central():
                    _Queue.later(delay_seconds, instance, **retry_options)
            else:
                with Tenancy.as_tenant(msg.get("_tenant")):
                    _Queue.later(delay_seconds, instance, **retry_options)
            Log.info(
                "↻ Durable retry scheduled for %s "
                "(attempt %s, throttle %s, +%ss, reason=%s)",
                instance.__class__.__name__,
                next_attempt,
                next_throttle,
                delay_seconds,
                type(exc).__name__,
            )
            JobProcessor._retry_settlement_step(
                f"Tracked queue job {db_job_id} retry settlement",
                lambda: tracker.require_job_status_strict(
                    db_job_id,
                    "retrying",
                ),
            )
            # Retry is durably accepted in the DB outbox and source settlement
            # committed atomically — ACK the original now. Broker publication
            # may happen later via the reconciler without changing semantics.
            JobProcessor._broker_ack(channel, method_frame.delivery_tag)
        except Exception as republish_err:
            Log.error(
                "Retry republish failed for %s: %s. "
                "Leaving the original delivery unacknowledged for broker "
                "redelivery.",
                instance.__class__.__name__,
                republish_err,
                exc_info=True,
            )
            raise

    @staticmethod
    def _route_failed_message(
        *,
        channel,
        method_frame,
        msg,
        instance,
        exc: Exception,
        queue_name: str | None,
        delivery_store,
        delivery_lease_token: str | None,
        tracker,
        db_job_id: int,
    ) -> str:
        """Single failure router: retry-with-delay OR dead-letter.

        Centralises three rules that the two ``except`` branches
        previously duplicated and routinely diverged on:

        * ``do_not_retry`` exceptions skip straight to DLQ — no point
          burning the backoff budget on a 404 that won't come back.
        * Retry settlement transactionally transfers the database uniqueness
          fence from the processing source to its delayed child.
        * Terminal failure removes the row from the open-delivery index, so a
          later legitimate dispatch can proceed.

        The budget question needs the exception, because a throttle
        ("never got a slot") and a fault ("ran and blew up") spend
        different budgets — see ``_should_retry_job``. A job that exhausts
        the throttle budget dead-letters with a ``throttle_exhausted``
        reason so DLQ triage can tell capacity starvation apart from job
        failure without re-reading the job's own error text.
        """
        do_not_retry = bool(getattr(exc, "do_not_retry", False))
        can_retry = (
            msg
            and instance is not None
            and not do_not_retry
            and JobProcessor._should_retry_job(msg, instance, exc)
        )
        terminal_reason = str(exc)
        if not can_retry and getattr(exc, "is_throttle", False):
            terminal_reason = f"throttle_exhausted: {exc}"

        if can_retry:
            if delivery_store is None or delivery_lease_token is None:
                raise QueueException(
                    "Cannot retry a queue delivery without a durable execution lease."
                )
            JobProcessor._requeue_with_delay(
                channel=channel,
                method_frame=method_frame,
                msg=msg,
                instance=instance,
                exc=exc,
                queue_name=queue_name,
                delivery_lease_token=delivery_lease_token,
                tracker=tracker,
                db_job_id=db_job_id,
            )
            # The ledger transaction moved the uniqueness fence from the
            # processing source to the delayed retry child.
            return "retry_scheduled"

        # Terminal — give up the slot.
        JobProcessor._ack_to_dlq(
            channel,
            method_frame,
            msg,
            terminal_reason,
            instance=instance,
            delivery_store=delivery_store,
            delivery_lease_token=delivery_lease_token,
            tracker=tracker,
            db_job_id=db_job_id,
        )
        return "dead_lettered"

    @staticmethod
    def _ack_to_dlq(
        channel,
        method_frame,
        msg,
        error_msg,
        *,
        instance=None,
        delivery_store=None,
        delivery_lease_token: str | None = None,
        tracker=None,
        db_job_id: int | None = None,
    ):
        """Settle trusted failures in PostgreSQL; quarantine untrusted bytes."""
        queue_name = msg.get("queue", "unknown") if msg else "unknown"
        job_id = msg.get("job_id", "unknown") if msg else "unknown"
        Log.error(
            "Job dead-lettered: %s | Queue: %s | Error: %s",
            job_id,
            queue_name,
            error_msg,
        )
        if delivery_store is not None and delivery_lease_token is not None:
            if tracker is None or db_job_id is None:
                raise DeliverySettlementError(
                    "Terminal queue settlement requires a persistent tracker."
                )
            try:
                delivery_store.dead_letter_with_tracker(
                    str(job_id),
                    delivery_lease_token,
                    db_job_id=db_job_id,
                    reason=str(error_msg),
                )
            except Exception as exc:
                raise DeliverySettlementError(
                    "Could not atomically persist terminal queue failure."
                ) from exc
            JobProcessor._broker_ack(channel, method_frame.delivery_tag)
            return
        JobProcessor._broker_nack(channel, method_frame.delivery_tag, requeue=False)

    process_message = staticmethod(_JobExecution._process_message)


_JobExecution._bind_job_processor(JobProcessor)
