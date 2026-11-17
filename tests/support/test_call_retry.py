"""``cara.support.send_with_retry`` — one retry rule and one wait budget per call."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from cara.support import (
    CallWaitBudget,
    backoff_seconds,
    http_status,
    send_with_retry,
    send_with_retry_async,
)


def _answer(status: int, headers: dict | None = None):
    return SimpleNamespace(status_code=status, headers=headers or {})


def _scripted(*outcomes):
    queue = list(outcomes)
    calls = []

    def send():
        calls.append(1)
        item = queue.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    return send, calls


def _no_jitter(attempt, *, floor=0.0):
    return backoff_seconds(attempt, floor=floor, jitter=0.0)


def test_throttle_is_retried_even_for_a_write_and_waits_the_retry_after_floor():
    send, calls = _scripted(_answer(429, {"Retry-After": "3"}), _answer(200))
    slept = []
    answer = send_with_retry(
        send, idempotent=False, sleep=slept.append, schedule=_no_jitter
    )
    assert answer.status_code == 200
    assert len(calls) == 2
    assert slept == [3.0]


def test_gateway_failure_is_retried_only_when_idempotent():
    send, calls = _scripted(_answer(503), _answer(200))
    answer = send_with_retry(send, idempotent=False, sleep=lambda s: None)
    assert answer.status_code == 503 and len(calls) == 1

    send, calls = _scripted(_answer(503), _answer(200))
    answer = send_with_retry(send, idempotent=True, sleep=lambda s: None)
    assert answer.status_code == 200 and len(calls) == 2


def test_transport_break_reraises_for_a_write_and_retries_for_a_read():
    send, calls = _scripted(ConnectionError("reset"), _answer(200))
    with pytest.raises(ConnectionError):
        send_with_retry(send, idempotent=False, sleep=lambda s: None)
    assert len(calls) == 1

    send, calls = _scripted(ConnectionError("reset"), _answer(200))
    seen = []
    answer = send_with_retry(
        send, idempotent=True, sleep=lambda s: None, on_transport=seen.append
    )
    assert answer.status_code == 200 and len(seen) == 1


def test_a_wait_the_budget_cannot_afford_returns_the_last_answer_unslept():
    send, calls = _scripted(_answer(429, {"Retry-After": "30"}), _answer(200))
    slept = []
    answer = send_with_retry(
        send, idempotent=True, sleep=slept.append, budget=CallWaitBudget(20)
    )
    assert answer.status_code == 429
    assert slept == [] and len(calls) == 1


def test_waits_across_retries_share_one_budget():
    send, calls = _scripted(*[_answer(429, {"Retry-After": "8"})] * 5)
    slept = []
    budget = CallWaitBudget(20)
    send_with_retry(send, idempotent=True, sleep=slept.append, budget=budget)
    assert slept == [8.0, 8.0]
    assert sum(slept) <= 20 and budget.left == pytest.approx(4.0)


def test_attempts_cap_and_on_answer_sees_every_answer():
    send, calls = _scripted(*[_answer(500)] * 3)
    seen = []
    send_with_retry(
        send,
        idempotent=True,
        attempts=3,
        sleep=lambda s: None,
        on_answer=lambda a, refused: seen.append((a.status_code, refused)),
    )
    assert len(calls) == 3 and seen == [(500, False)] * 3


def test_custom_throttle_predicate_and_status_reader():
    send, calls = _scripted({"status": 200, "blocked": True}, {"status": 200})
    answer = send_with_retry(
        send,
        idempotent=True,
        throttled=lambda a: bool(a.get("blocked")),
        floor=None,
        sleep=lambda s: None,
    )
    assert answer == {"status": 200} and len(calls) == 2


def test_backoff_is_exponential_capped_and_never_under_the_floor():
    assert backoff_seconds(1, jitter=0.0) == 1.0
    assert backoff_seconds(3, jitter=0.0) == 4.0
    assert backoff_seconds(10, jitter=0.0) == 60.0
    assert backoff_seconds(1, floor=90.0, jitter=0.0) == 90.0


def test_async_loop_shares_the_rules():
    outcomes = [_answer(429), _answer(502), _answer(200)]

    async def send():
        return outcomes.pop(0)

    slept = []

    async def nap(seconds):
        slept.append(seconds)

    answer = asyncio.run(
        send_with_retry_async(send, idempotent=True, sleep=nap, schedule=_no_jitter)
    )
    assert answer.status_code == 200 and slept == [1.0, 2.0]


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (SimpleNamespace(status_code=404), 404),
        (SimpleNamespace(status=429), 429),
        (SimpleNamespace(response=SimpleNamespace(status_code=503)), 503),
        (SimpleNamespace(code=429), 429),
        (SimpleNamespace(code=999), None),
        (SimpleNamespace(code=True), None),
        ({"status": "502"}, 502),
        ({"status_code": 200}, 200),
        (None, None),
        (object(), None),
    ],
)
def test_http_status_reads_every_answer_shape(answer, expected):
    assert http_status(answer) == expected
