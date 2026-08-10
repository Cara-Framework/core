"""One ``Decimal``, one wire representation, measured at every door.

``cara/support/JsonEncoding.py`` opens with a table of what each boundary
did to ``Decimal("19.99")``. A table in a docstring rots the moment
someone edits an encoder, so this module re-measures it: it pushes the
same value through the HTTP body, the JSONL chunk, the SSE frame, the
websocket frame and the broadcast publish — each through its real send
path — and asserts they land on ONE representation.

It also pins the two boundaries that were the input to the decision (the
cache codec, which tags and round-trips, and the queue serializer, which
refuses) and the one that still disagrees on purpose (the ORM), so the
documented exception cannot quietly become an undocumented one.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cara.broadcasting.drivers.RedisBroadcaster import RedisBroadcaster
from cara.cache.codecs.JsonCacheCodec import JsonCacheCodec
from cara.exceptions import QueueException
from cara.http import Response
from cara.http.response import StreamingResponse
from cara.queues.contracts import Queueable, ShouldQueue
from cara.queues.serializers import SignedJsonJobSerializer
from cara.support import decimal_to_wire
from cara.websocket.Socket import Socket

# NUMERIC(17,6) ceiling — the value ``QueryBuilder.aggregate_result``
# already carries an incident for. ``float(MONEY)`` is ``100000000000.0``.
MONEY = Decimal("99999999999.999999")
EXACT = "99999999999.999999"


async def _drain(response: Response) -> str:
    events: list[dict] = []

    async def send(event: dict) -> None:
        events.append(event)

    await response({}, None, send)
    return b"".join(event.get("body", b"") for event in events[1:]).decode("utf-8")


async def _http_json() -> object:
    response = Response(MagicMock())
    response.json({"total": MONEY})
    return json.loads(await _drain(response))["total"]


async def _jsonl_chunk() -> object:
    async def rows():
        yield {"total": MONEY}

    response = Response(MagicMock())
    response.stream_json_lines(rows())
    body = await _drain(response)
    return json.loads(body.splitlines()[0])["total"]


async def _streaming_jsonl_chunk() -> object:
    async def rows():
        yield {"total": MONEY}

    events: list[dict] = []

    async def send(event: dict) -> None:
        events.append(event)

    await StreamingResponse(Response(MagicMock())).stream_json_lines(rows(), send)
    body = b"".join(event.get("body", b"") for event in events[1:]).decode("utf-8")
    return json.loads(body.splitlines()[0])["total"]


async def _sse_frame() -> object:
    async def events_generator():
        yield {"event": "price.changed", "data": {"total": MONEY}}

    response = Response(MagicMock())
    response.stream_sse(events_generator())
    frame = await _drain(response)
    data = "".join(
        line[len("data: ") :] for line in frame.splitlines() if line.startswith("data: ")
    )
    return json.loads(data)["total"]


async def _websocket_frame() -> object:
    sent: list[dict] = []

    async def _send(message: dict) -> None:
        sent.append(message)

    async def _receive() -> dict:
        return {"type": "websocket.receive"}

    socket = Socket(
        application=None, scope={"type": "websocket"}, receive=_receive, send=_send
    )
    socket._ws_connected = True
    await socket.send_json({"total": MONEY})
    return json.loads(sent[-1]["text"])["total"]


async def _broadcast_publish() -> object:
    published: list[tuple[str, str]] = []

    class _Broker:
        async def publish(self, channel: str, payload: str) -> None:
            published.append((channel, payload))

    node = RedisBroadcaster({"websocket": {"heartbeat_interval": 0}})

    async def _redis():
        return _Broker()

    node._redis = _redis
    await node.broadcast("orders", "price.changed", {"total": MONEY})
    return json.loads(published[-1][1])["data"]["total"]


@pytest.mark.asyncio
async def test_every_outbound_boundary_lands_on_one_wire_representation() -> None:
    measured = {
        "http json body": await _http_json(),
        "jsonl chunk (Response)": await _jsonl_chunk(),
        "jsonl chunk (StreamingResponse)": await _streaming_jsonl_chunk(),
        "sse frame": await _sse_frame(),
        "websocket frame": await _websocket_frame(),
        "broadcast publish": await _broadcast_publish(),
    }

    disagreeing = {name: value for name, value in measured.items() if value != EXACT}
    assert not disagreeing, (
        f"boundaries disagree about the same Decimal: {disagreeing!r} — "
        f"every outbound wire must carry {EXACT!r}"
    )
    # And the value is still money afterwards, not a rounded double.
    assert all(Decimal(value) == MONEY for value in measured.values())
    assert float(MONEY) == 100000000000.0  # the loss these assertions prevent


def test_cache_codec_carries_the_same_digits_the_wire_rule_would() -> None:
    """The codec was one of the two boundaries that already refused to lose
    the value; the wire rule was chosen to agree with its ``v`` field."""
    codec = JsonCacheCodec("k" * 40)
    blob = codec.encode({"total": MONEY})

    assert f'"decimal","v":"{EXACT}"'.encode() in blob
    assert decimal_to_wire(MONEY) == EXACT
    # It tags the type because it is a symmetric hop and must restore one.
    assert codec.decode(blob) == {"total": MONEY}


class PricedJob(ShouldQueue, Queueable):
    def __init__(self, total):
        self.total = total
        super().__init__()
        self.queue = "sync"
        self.priority = "default"

    async def handle(self):
        return None


def test_queue_serializer_still_refuses_a_decimal_rather_than_guessing() -> None:
    """The other input to the decision: the envelope declines to carry it.

    A job body is a cross-process contract with no display consumer, so
    "refuse" is a legitimate answer there and the product converts at
    dispatch. If this ever starts succeeding, the envelope has invented a
    seventh rule and the table in ``JsonEncoding`` is stale.
    """
    key = "signed-queue-test-key-" * 3

    def _serialize(total):
        return SignedJsonJobSerializer.serialize(
            {
                "obj": PricedJob(total),
                "args": (),
                "callback": "handle",
                "created": "2026-07-16T00:00:00Z",
                "job_id": "11111111-1111-4111-8111-111111111111",
                "db_job_id": 12,
                "timeout_seconds": 300,
                "attempts": 0,
                "throttle_attempts": 0,
                "_tenant": 7,
                "_tenant_mode": "tenant",
                "queue": "sync",
                "priority": "default",
                "dispatched_at": "2026-07-16T00:00:00Z",
                "replay_of": None,
            },
            signing_key_id="current",
            signing_keys={"current": key},
            allowed_prefixes=("tests.support",),
            issued_at=1_752_643_200,
        )

    # Control first: the SAME envelope with a JSON-native amount must
    # serialize. Without it, a refusal for any unrelated reason (a bad
    # ``obj``, a missing field) would read as a Decimal refusal and this
    # test would prove nothing.
    assert _serialize(1999), "control envelope must be serializable"

    with pytest.raises(QueueException) as raised:
        _serialize(MONEY)
    assert "Decimal" in str(raised.value)


def test_model_serialization_is_the_documented_exception_not_a_new_one() -> None:
    """The ORM boundary still emits a double, and the encoder is not why.

    ``Model.serialize`` rewrites every ``Decimal`` as ``float`` while it
    builds the dict, so the value is already spent before any encoder
    runs — which is why routing ``to_json`` through the shared rule did
    not, and could not, close this row of the table. Closing it means
    migrating ``serialize`` together with both products' API resources
    and generated contracts, which type model money as ``number``.

    This test exists so that stays a stated deferral rather than a
    surprise. **When ``serialize`` is migrated, re-pin these assertions
    to the exact string** and delete the corresponding section of
    ``cara/support/JsonEncoding.py``.
    """
    from cara.eloquent import DatabaseManager
    from cara.eloquent.models.Model import Model

    DatabaseManager.get_instance().set_database_config(
        "app", {"app": {"driver": "sqlite", "database": ":memory:"}}
    )

    class _Priced(Model):
        # No ``decimal`` cast on purpose: ``DecimalCast`` defaults to
        # ``precision=2`` and would quantise the probe value before
        # ``serialize`` ever saw it, hiding the defect under a second one.
        __table__ = "test_json_encoding_priced"

    row = _Priced()
    row.total = MONEY

    # The attribute itself is untouched — exact money is still available.
    assert row.total == MONEY
    # The serialized dict is where it is spent, before any encoder.
    assert isinstance(row.to_array()["total"], float)
    assert row.to_array()["total"] == 100000000000.0
    assert json.loads(row.to_json())["total"] == 100000000000.0
