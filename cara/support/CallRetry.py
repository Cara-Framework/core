"""The one retry loop for an outbound call, with one wait budget per call.

Every outbound client used to hand-roll its own loop: a different schedule
per client, a ``Retry-After`` honoured up to 30 s PER sleep, and no cap on the
sum — so one fetch could sleep minutes inside a job whose timeout assumed
seconds. This module is the product-blind rule both products' connectors
share:

- a THROTTLE (``throttled(answer)``, 429 by default) is repeated always —
  the upstream refused it unprocessed, so even a write may go again;
- a gateway failure (``retryable_status``, 500/502/503/504 by default) and a
  transport break (``transport_errors``) are repeated only for an
  ``idempotent`` call, because the first attempt may have landed;
- each wait follows :func:`backoff_seconds` (exponential, jittered, capped)
  but never under the upstream's own floor (``floor(answer)``, the
  ``Retry-After`` by default);
- the waits of one call come out of ONE :class:`CallWaitBudget`. A wait the
  budget cannot afford is not taken: the loop returns the last answer (or
  re-raises the last transport break), and the caller's durable retry —
  the queue's backoff, a scheduler tick — takes over.

Product vocabulary stays out: metrics, breaker feeding and outcome counters
are callbacks (``on_answer``, ``on_transport``), and anything the product
reads off an answer (a marketplace's business-code throttle, a gateway's
upstream-status header) is the ``throttled`` / ``floor`` / ``status``
callables. ``send_with_retry`` is the sync loop, ``send_with_retry_async``
the async one; both share every rule above.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .CallWaitBudget import CallWaitBudget
from .HttpStatus import http_status
from .RetryAfter import retry_after_seconds

RETRY_BASE_SECONDS = 1.0
RETRY_CAP_SECONDS = 60.0
RETRY_ATTEMPTS = 6
#: ± spread on each wait, so callers that failed together do not retry together.
RETRY_JITTER_FRACTION = 0.25
#: The longest ``Retry-After`` the default floor honours.
RETRY_AFTER_CAP_S = 3600.0

THROTTLED_STATUS = 429
#: Gateway failures only an idempotent call may repeat.
GATEWAY_FAILURES = frozenset({500, 502, 503, 504})
#: A connection that dropped or timed out with no answer. Clients raise their
#: own families (``httpx.TransportError``, ``requests.ConnectionError``) —
#: pass them as ``transport_errors``; the builtins cover raw sockets.
TRANSPORT_ERRORS: tuple[type[BaseException], ...] = (ConnectionError, TimeoutError)


def backoff_seconds(
    attempt: int,
    *,
    floor: float = 0.0,
    base: float = RETRY_BASE_SECONDS,
    cap: float = RETRY_CAP_SECONDS,
    jitter: float = RETRY_JITTER_FRACTION,
) -> float:
    """The wait after failed attempt ``attempt`` (1-based): ``base·2^(n-1)``
    jittered and capped at ``cap`` — but never under ``floor``, the
    upstream's own named wait (it wins over the ceiling: retrying sooner only
    collects another refusal)."""
    exponential = float(base) * 2 ** max(0, int(attempt) - 1)
    spread = 1.0 + random.uniform(-float(jitter), float(jitter))
    return max(float(floor), min(float(cap), exponential * spread))


def is_throttled(answer: Any) -> bool:
    """The default throttle test: the answer's status is 429."""
    return http_status(answer) == THROTTLED_STATUS


def retry_after_floor(answer: Any) -> float:
    """The default floor: the answer's ``Retry-After`` (0 when it names none)."""
    headers = getattr(answer, "headers", None)
    if headers is None:
        headers = getattr(getattr(answer, "response", None), "headers", None)
    return retry_after_seconds(headers, cap=RETRY_AFTER_CAP_S) or 0.0


class _Decision:
    """What one attempt's outcome means for the loop."""

    __slots__ = ("delay", "stop")

    def __init__(self, *, stop: bool, delay: float = 0.0) -> None:
        self.stop = stop
        self.delay = delay


class _Rules:
    """The shared decision core of the sync and async loops."""

    def __init__(
        self,
        *,
        idempotent: bool,
        throttled: Callable[[Any], bool],
        floor: Callable[[Any], float] | None,
        status: Callable[[Any], int | None],
        retryable_status: frozenset[int],
        budget: CallWaitBudget,
        attempts: int,
        schedule: Callable[..., float],
        on_answer: Callable[[Any, bool], None] | None,
        on_transport: Callable[[BaseException], None] | None,
    ) -> None:
        self.idempotent = bool(idempotent)
        self.throttled = throttled
        self.floor = floor
        self.status = status
        self.retryable_status = retryable_status
        self.budget = budget
        self.limit = max(1, int(attempts))
        self.schedule = schedule
        self.on_answer = on_answer
        self.on_transport = on_transport

    def after_answer(self, answer: Any, attempt: int) -> _Decision:
        refused = bool(self.throttled(answer))
        if self.on_answer is not None:
            self.on_answer(answer, refused)
        retryable = refused or (
            self.idempotent and self.status(answer) in self.retryable_status
        )
        if not retryable or attempt >= self.limit:
            return _Decision(stop=True)
        least = self.floor(answer) if refused and self.floor is not None else 0.0
        delay = self.schedule(attempt, floor=least)
        if not self.budget.affords(delay):
            return _Decision(stop=True)
        return _Decision(stop=False, delay=delay)

    def after_transport(self, exc: BaseException, attempt: int) -> _Decision:
        if self.on_transport is not None:
            self.on_transport(exc)
        if not self.idempotent or attempt >= self.limit:
            return _Decision(stop=True)
        delay = self.schedule(attempt)
        if not self.budget.affords(delay):
            return _Decision(stop=True)
        return _Decision(stop=False, delay=delay)


def _rules(
    idempotent: bool,
    throttled: Callable[[Any], bool],
    floor: Callable[[Any], float] | None,
    status: Callable[[Any], int | None],
    retryable_status: frozenset[int] | set[int],
    budget: CallWaitBudget | None,
    attempts: int,
    schedule: Callable[..., float] | None,
    on_answer: Callable[[Any, bool], None] | None,
    on_transport: Callable[[BaseException], None] | None,
) -> _Rules:
    return _Rules(
        idempotent=idempotent,
        throttled=throttled,
        floor=floor,
        status=status,
        retryable_status=frozenset(retryable_status),
        budget=budget if budget is not None else CallWaitBudget(),
        attempts=attempts,
        schedule=schedule or backoff_seconds,
        on_answer=on_answer,
        on_transport=on_transport,
    )


def send_with_retry(
    send: Callable[[], Any],
    *,
    idempotent: bool,
    throttled: Callable[[Any], bool] = is_throttled,
    floor: Callable[[Any], float] | None = retry_after_floor,
    status: Callable[[Any], int | None] = http_status,
    retryable_status: frozenset[int] | set[int] = GATEWAY_FAILURES,
    transport_errors: tuple[type[BaseException], ...] = TRANSPORT_ERRORS,
    budget: CallWaitBudget | None = None,
    attempts: int = RETRY_ATTEMPTS,
    schedule: Callable[..., float] | None = None,
    sleep: Callable[[float], None] | None = None,
    on_answer: Callable[[Any, bool], None] | None = None,
    on_transport: Callable[[BaseException], None] | None = None,
) -> Any:
    """Send ``send()`` until it lands or the rule says stop; the last answer.

    ``send`` is ONE attempt (re-signed, re-built each time). A transport
    break raises once the call may not, or can no longer, repeat. ``sleep``
    defaults to ``time.sleep`` read at call time; ``schedule(attempt,
    floor=…)`` defaults to :func:`backoff_seconds`.
    """
    rules = _rules(
        idempotent,
        throttled,
        floor,
        status,
        retryable_status,
        budget,
        attempts,
        schedule,
        on_answer,
        on_transport,
    )
    nap = sleep or time.sleep
    attempt = 1
    while True:
        try:
            answer = send()
        except transport_errors as exc:
            decision = rules.after_transport(exc, attempt)
            if decision.stop:
                raise
        else:
            decision = rules.after_answer(answer, attempt)
            if decision.stop:
                return answer
        nap(decision.delay)
        rules.budget.spend(decision.delay)
        attempt += 1


async def send_with_retry_async(
    send: Callable[[], Awaitable[Any]],
    *,
    idempotent: bool,
    throttled: Callable[[Any], bool] = is_throttled,
    floor: Callable[[Any], float] | None = retry_after_floor,
    status: Callable[[Any], int | None] = http_status,
    retryable_status: frozenset[int] | set[int] = GATEWAY_FAILURES,
    transport_errors: tuple[type[BaseException], ...] = TRANSPORT_ERRORS,
    budget: CallWaitBudget | None = None,
    attempts: int = RETRY_ATTEMPTS,
    schedule: Callable[..., float] | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    on_answer: Callable[[Any, bool], None] | None = None,
    on_transport: Callable[[BaseException], None] | None = None,
) -> Any:
    """The async twin of :func:`send_with_retry` — same rules, awaited sends
    and sleeps (``asyncio.sleep`` read at call time by default)."""
    rules = _rules(
        idempotent,
        throttled,
        floor,
        status,
        retryable_status,
        budget,
        attempts,
        schedule,
        on_answer,
        on_transport,
    )
    nap = sleep or asyncio.sleep
    attempt = 1
    while True:
        try:
            answer = await send()
        except transport_errors as exc:
            decision = rules.after_transport(exc, attempt)
            if decision.stop:
                raise
        else:
            decision = rules.after_answer(answer, attempt)
            if decision.stop:
                return answer
        await nap(decision.delay)
        rules.budget.spend(decision.delay)
        attempt += 1


__all__ = [
    "GATEWAY_FAILURES",
    "RETRY_ATTEMPTS",
    "RETRY_BASE_SECONDS",
    "RETRY_CAP_SECONDS",
    "RETRY_JITTER_FRACTION",
    "THROTTLED_STATUS",
    "TRANSPORT_ERRORS",
    "backoff_seconds",
    "is_throttled",
    "retry_after_floor",
    "send_with_retry",
    "send_with_retry_async",
]
