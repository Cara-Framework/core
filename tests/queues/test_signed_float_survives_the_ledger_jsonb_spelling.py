"""A float job argument is signed the way the ledger's jsonb spells it back.

Live defect: the relay publishes the envelope bytes it re-derives from the
``signed_envelope`` jsonb column, and jsonb keeps numbers as ``numeric``: it
has no negative zero (``-0.0`` reads back ``0.0``) and it prints a float whose
repr has a positive exponent (``1e+16`` and up) as plain integer digits. An
argument signed as given no longer matched its signature at publish, so the
relay dead-lettered a job the dispatch had reported as accepted
(``publish_envelope_invalid``).

Before the fix (``git show HEAD:cara/queues/serializers/_SignedJsonValues.py``)
``json_value`` returned the float untouched: ``-0.0`` and ``1e16`` envelopes
fail verification after the jsonb round trip below.

The jsonb model here is PostgreSQL's ``numeric`` output rule, written
independently of the source's fix: every number is read as an exact decimal
and printed positionally (``format(d, 'f')``), negative zero without its sign.
No database.
"""

from __future__ import annotations

import json
import math
from decimal import Decimal
from typing import Any

import pytest

from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.delivery._DeliveryDiagnostics import _delivery_envelope_bytes
from cara.queues.serializers import SignedJsonJobSerializer
from cara.queues.serializers._SignedJsonValues import _SignedJsonValues

_KEYS = {"current": "ledger-float-test-key-" * 3}


class ValueJob(ShouldQueue, Queueable):
    def __init__(self, x):
        self.x = x
        super().__init__()
        self.queue = "sync"
        self.priority = "default"

    async def handle(self):
        return None


def _signed(value: float) -> bytes:
    return SignedJsonJobSerializer.serialize(
        {
            "obj": ValueJob(value),
            "args": (),
            "callback": "handle",
            "created": "2026-11-23T00:00:00Z",
            "job_id": "11111111-1111-4111-8111-111111111111",
            "db_job_id": 12,
            "timeout_seconds": 300,
            "attempts": 0,
            "throttle_attempts": 0,
            "_tenant": 7,
            "_tenant_mode": "tenant",
            "queue": "sync",
            "priority": "default",
            "dispatched_at": "2026-11-23T00:00:00Z",
            "replay_of": None,
        },
        signing_key_id="current",
        signing_keys=_KEYS,
        allowed_prefixes=("tests.queues",),
    )


def _numeric_out(value: Decimal) -> Any:
    """One number as PostgreSQL's ``numeric`` prints it, read back by JSON."""
    if value.is_zero():
        value = value.copy_abs()
    return json.loads(format(value, "f"))


def _through_jsonb(node: Any) -> Any:
    if isinstance(node, Decimal):
        return _numeric_out(node)
    if isinstance(node, dict):
        return {key: _through_jsonb(item) for key, item in node.items()}
    if isinstance(node, list):
        return [_through_jsonb(item) for item in node]
    return node


def _ledger_copy(body: bytes) -> bytes:
    """The bytes the relay re-derives from the ledger's jsonb column."""
    stored = json.loads(body, parse_float=Decimal, parse_int=Decimal)
    return _delivery_envelope_bytes(_through_jsonb(stored))


@pytest.mark.parametrize(
    "value", [-0.0, 1e16, -1.5e16, 1e22, 1.7976931348623157e308, 12.5, 1e-7, 0.1]
)
def test_the_envelope_still_verifies_after_the_ledger_rewrites_its_numbers(
    value: float,
) -> None:
    body = _signed(value)

    envelope = SignedJsonJobSerializer.inspect_envelope(
        _ledger_copy(body), signing_keys=_KEYS, allow_not_before=True
    )

    assert envelope["payload"]["queue"] == "sync"


def test_negative_zero_is_signed_as_the_ledgers_zero() -> None:
    stable = _SignedJsonValues.ledger_stable_float(-0.0)

    assert stable == 0.0 and math.copysign(1.0, stable) == 1.0


@pytest.mark.parametrize("value", [1e16, -1.5e16, 1e22, 1.7976931348623157e308])
def test_a_float_past_1e16_is_signed_as_its_integer_digits(value: float) -> None:
    stable = _SignedJsonValues.ledger_stable_float(value)

    assert isinstance(stable, int)
    assert str(stable) == format(Decimal(repr(value)), "f")
    assert float(stable) == value  # the job still receives the same number


@pytest.mark.parametrize("value", [12.5, 1e15, 9999999999999998.0, 1e-7, -0.1])
def test_a_float_the_ledger_keeps_is_signed_unchanged(value: float) -> None:
    stable = _SignedJsonValues.ledger_stable_float(value)

    assert type(stable) is float and stable == value
