"""The job instance learns WHICH delivery it is, not just that it ran.

``_current_attempt`` is read by application base jobs for the retry-depth
histogram and for the wide event's ``attempt`` field. Nothing used to stamp
it, so both reported the constant 1 for every delivery while their docstrings
claimed the queue runner supplied the real number — a metric that cannot go
up is worse than no metric, because it reads as "no job ever retries".

The envelope's ``attempts`` key is the attempts-ALREADY-MADE counter
(``AMQPDriver.push`` stamps it 0, every retry republish bumps it), so the
delivery about to run is ``attempts + 1``. These tests pin that arithmetic
and the tolerance for envelopes that predate — or corrupt — the counter.
"""

from __future__ import annotations

import pytest

from cara.commands.core import _JobExecution


class _Job:
    """Bare stand-in for a queued job: an object with a ``__dict__``."""


@pytest.mark.parametrize(
    ("attempts", "expected"),
    [
        (0, 1),  # first delivery: AMQPDriver.push stamps attempts=0
        (1, 2),  # first retry
        (4, 5),
    ],
)
def test_current_attempt_is_the_delivery_number(attempts, expected):
    job = _Job()

    _JobExecution._stamp_dispatch_context(job, {"attempts": attempts})

    assert job._current_attempt == expected


@pytest.mark.parametrize("envelope", [{}, {"attempts": None}, {"attempts": "junk"}])
def test_a_missing_or_corrupt_counter_reads_as_the_first_delivery(envelope):
    """A counter the runner cannot trust must not explode before ``handle()``.

    ``_envelope_counter`` clamps these to 0, which makes this delivery 1 —
    the same value a sync dispatch (which never reaches this function) falls
    back to.
    """
    job = _Job()

    _JobExecution._stamp_dispatch_context(job, envelope)

    assert job._current_attempt == 1


def test_dispatcher_context_travels_with_the_attempt_counter():
    """Trace, tenancy and dispatch time are stamped in the same place.

    They are one contract: a job that knows its attempt but not its tenant is
    as broken as the reverse, so they must not drift into separate call sites.
    """
    job = _Job()
    envelope = {
        "attempts": 2,
        "_otel": {"traceparent": "00-abc-def-01"},
        "_tenant": 17,
        "_tenant_mode": "tenant",
        "dispatched_at": "2026-09-04T00:00:00+00:00",
    }

    _JobExecution._stamp_dispatch_context(job, envelope)

    assert job._otel_carrier == {"traceparent": "00-abc-def-01"}
    assert job._tenant_id == 17
    assert job._tenant_mode == "tenant"
    assert job._dispatched_at == "2026-09-04T00:00:00+00:00"
    assert job._current_attempt == 3
