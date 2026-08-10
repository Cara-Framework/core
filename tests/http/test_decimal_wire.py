"""A ``Decimal`` must reach the client with every digit it left with.

These tests drive the REAL boundary — a controller-shaped call, then the
ASGI ``send`` callable, then the bytes — because the defect they guard
lived in the encoder argument, not in the payload. A test that calls
``json_dumps`` directly would have passed against the broken code.

The probe value is cara's own ``NUMERIC(17,6)`` ceiling, the incident
``QueryBuilder.aggregate_result`` already documents:
``float(Decimal("99999999999.999999"))`` is ``100000000000.0`` — a penny
short of a hundred billion, rounded up to exactly a hundred billion, with
no exception and no log line. Any boundary that yields a JSON number here
has spent money, so every assertion checks the parsed value is still the
string of digits and never the double.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from cara.http import Response
from cara.http.response import StreamingResponse

# NUMERIC(17,6) ceiling: the smallest realistic money value whose double
# is visibly wrong. ``float(MONEY) == 100000000000.0``.
MONEY = Decimal("99999999999.999999")
# Trailing zeros are part of a price's scale — "19.90" is not "19.9".
SCALED = Decimal("19.90")


async def _drain(response: Response) -> str:
    """Run a configured response through ASGI and return the body text."""
    events: list[dict] = []

    async def send(event: dict) -> None:
        events.append(event)

    await response({}, None, send)
    return b"".join(event.get("body", b"") for event in events[1:]).decode("utf-8")


def _assert_exact(raw: object, expected: Decimal) -> None:
    assert isinstance(raw, str), (
        f"money arrived as {type(raw).__name__} {raw!r} — a JSON number is an "
        f"IEEE-754 double in every consumer, so the digits are already gone"
    )
    assert raw == str(expected)
    assert Decimal(raw) == expected


@pytest.mark.asyncio
async def test_json_response_body_carries_exact_decimal_digits() -> None:
    response = Response(MagicMock())
    response.json({"total": MONEY, "unit": SCALED})

    body = await _drain(response)
    payload = json.loads(body)

    _assert_exact(payload["total"], MONEY)
    _assert_exact(payload["unit"], SCALED)
    # The specific corruption this guards: the double the old encoders produced.
    assert payload["total"] != float(MONEY)


@pytest.mark.asyncio
async def test_jsonl_stream_chunks_carry_exact_decimal_digits() -> None:
    async def rows():
        yield {"total": MONEY}
        yield {"total": SCALED}

    response = Response(MagicMock())
    response.stream_json_lines(rows())

    body = await _drain(response)
    lines = [json.loads(line) for line in body.splitlines() if line]

    assert len(lines) == 2
    _assert_exact(lines[0]["total"], MONEY)
    _assert_exact(lines[1]["total"], SCALED)


@pytest.mark.asyncio
async def test_streaming_response_jsonl_carries_exact_decimal_digits() -> None:
    """``StreamingResponse`` owns a second JSONL encoder; it must agree."""

    async def rows():
        yield {"total": MONEY}

    events: list[dict] = []

    async def send(event: dict) -> None:
        events.append(event)

    streaming = StreamingResponse(Response(MagicMock()))
    await streaming.stream_json_lines(rows(), send)

    body = b"".join(event.get("body", b"") for event in events[1:]).decode("utf-8")
    _assert_exact(json.loads(body)["total"], MONEY)


@pytest.mark.asyncio
async def test_sse_frame_data_carries_exact_decimal_digits() -> None:
    async def events_generator():
        yield {"event": "price.changed", "data": {"total": MONEY}}

    response = Response(MagicMock())
    response.stream_sse(events_generator())

    frame = await _drain(response)
    data_lines = [
        line[len("data: ") :] for line in frame.splitlines() if line.startswith("data: ")
    ]
    assert data_lines, f"no SSE data field in frame {frame!r}"
    _assert_exact(json.loads("".join(data_lines))["total"], MONEY)


def test_non_finite_float_never_reaches_the_client_as_bare_nan() -> None:
    """``NaN`` is not JSON — the old encoder shipped it behind a 200.

    stdlib ``json`` emits the bare token ``NaN`` unless ``allow_nan`` is
    off, so the response serialized fine in Python and threw in
    ``JSON.parse`` at the far end, where nobody owns it. The wire rule
    refuses at the boundary that produced the value, while the request
    is still the framework's to fail.
    """
    with pytest.raises(ValueError, match="Out of range float"):
        Response(MagicMock()).json({"total": float("nan")})


def test_non_finite_decimal_is_refused_with_the_reason_named() -> None:
    """§7: a NaN quantity is unknown, and unknown is NULL, not a token."""
    with pytest.raises(ValueError, match="non-finite Decimal"):
        Response(MagicMock()).json({"total": Decimal("NaN")})


def test_unknown_object_is_refused_instead_of_stringified() -> None:
    """``default=str`` was a silent catch-all with a 200 attached.

    A model instance in a payload used to reach the customer as
    ``"<_Order object at 0x10c3f2a10>"`` with a success status.
    """

    class _Order:
        pass

    with pytest.raises(TypeError, match="not JSON serializable"):
        Response(MagicMock()).json({"order": _Order()})


@pytest.mark.asyncio
async def test_streaming_nan_truncates_the_body_instead_of_emitting_it() -> None:
    """Streams cannot fail late: headers are already on the wire.

    ``stream()`` closes the body rather than change a status it can no
    longer change, so the client sees a short read — a broken download
    it can retry — instead of a chunk that ``JSON.parse`` rejects.
    """

    async def rows():
        yield {"total": MONEY}
        yield {"total": float("inf")}

    response = Response(MagicMock())
    response.stream_json_lines(rows())

    body = await _drain(response)
    assert "Infinity" not in body
    lines = [json.loads(line) for line in body.splitlines() if line]
    assert len(lines) == 1
    _assert_exact(lines[0]["total"], MONEY)
