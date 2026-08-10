"""The one JSON encode rule for every wire cara writes.

Before this module a single ``Decimal`` left the framework as four
different things depending on which door it walked out of, and the
measurement is reproducible: encode ``{"p": Decimal("19.99")}`` at each
boundary and read the bytes.

===========================================  ==============================
boundary                                     wire value
===========================================  ==============================
``ResponseFactory.json``                     ``"19.99"``   (JSON string)
``Response.stream_json_lines``               ``"19.99"``
``StreamingResponse.stream_json_lines``      ``"19.99"``
``StreamingResponse._format_sse_event``      ``"19.99"``
``Socket.send_json``                         ``19.99``     (IEEE-754 double)
``RedisBroadcaster`` publish                 ``19.99``     (IEEE-754 double)
``Model.to_json`` / ``Model.to_array``       ``19.99``     (IEEE-754 double)
``JsonCacheCodec``                           ``{"t": "decimal", "v": "19.99"}``
``SignedJsonJobSerializer``                  ``QueueException`` — refused
===========================================  ==============================

This module closed the first two float rows. **The model row is still
open and is not this module's to close** — see "The model boundary
still disagrees" below. Read that section before trusting a money field
that came out of a model.

The table is not taken on trust: ``tests/support/test_json_encoding.py``
re-measures every row through its real send path, so an edit to any
encoder that reopens a row fails there rather than in production.

The float rows are money corruption. ``QueryBuilder.aggregate_result``
already carries the incident that proves it: at cara's own
``NUMERIC(17,6)`` ceiling ``float(Decimal('99999999999.999999'))`` is
``100000000000.0`` — a penny short of a hundred billion, rounded up to
exactly a hundred billion, silently, in a value that then averages into a
report nobody re-derives. §7 says money is ``Decimal`` end-to-end; a
serializer does not get to be the exception.

Why the digits travel as a JSON **string**
------------------------------------------
The two boundaries that already refuse to corrupt were the input to this
decision, not an afterthought.

``JsonCacheCodec`` writes ``{"t": "decimal", "v": str(value)}`` and
``SignedJsonJobSerializer`` refuses a ``Decimal`` outright rather than
guess. They disagree about the *shape* — one tags the type, one declines
to carry it — but they agree about the only thing this module has to
decide: **the digits travel as text, never as a JSON number.** The codec
adds a type tag because it is a symmetric cara → Redis → cara hop and it
must hand a ``Decimal`` back on the way out; the tag is how it restores a
Python type, not how it preserves a value.

An HTTP body, a JSONL chunk, an SSE frame, a websocket frame and a
broadcast payload have no way back. Their consumer is a TypeScript
frontend, which has no decimal type at all: every JSON parser in both
products materializes a JSON number as an IEEE-754 double, so a JSON
number cannot hold a ``Decimal`` without loss *in every consumer*. That
leaves a string or a typed object, and a typed object on a public wire
buys a decoder in every frontend to restore a type JavaScript cannot
represent anyway. So: the exact digits, as a string — identical to what
the cache codec already puts in its ``v`` field, and identical to what
the four HTTP boundaries above already emitted (they reached it by
accident, via ``default=str``, but they emitted it). This module moves
the websocket and broadcast dissenters onto that incumbent contract; it
does not invent a new behaviour.

The model boundary still disagrees — deliberately, for now
----------------------------------------------------------
``Model.serialize`` walks its own attributes and rewrites every
``Decimal`` as ``float(value)`` **before** any encoder is reached
(``cara/eloquent/models/Model.py``, in the "Handle remaining datetime and
decimal types" loop). ``to_array`` is that method, and ``to_json`` is
``to_array`` plus an encoder — so by the time this module sees a model's
money it is already a double, and no change here can recover the digits.
The result is a live split that an endpoint shows in one response:

    response.json({"total": Decimal("19.99")})      -> {"total": "19.99"}
    response.json(order.to_array())                 -> {"total": 19.99}

That is not left standing because it is right. It is left standing
because it is a **product wire contract**, not a framework one: both
frontends type model-sourced money as a TypeScript ``number`` and do
arithmetic on it (dashboard ``lib/api/{orders,billing,products,profit}``
and storefront ``types/api.ts`` are the dense cases). Flipping
``serialize`` to string money is a coordinated product change — API
resources, generated contracts and every consumer in the same change,
per §5's no-shims rule — and a framework lane that flipped it
unilaterally would break two dashboards to fix a rounding error nobody
had measured yet. ``cara.support.Decimals`` documents the same trade
from the opt-in side.

Until that migration happens: hand this module a ``Decimal`` and it
survives; hand it a model's ``to_array()`` and the money was already
spent. Resources that care about exact money must read the attribute
(``order.total`` is still a ``Decimal``) rather than the serialized dict.

Timestamps
----------
The outbound sites reached the string by way of ``default=str``, which is
right for ``Decimal`` by accident and wrong for ``datetime`` on purpose:
``str(datetime)`` is space-separated (``"2026-08-09 12:00:00+00:00"``),
which is not ISO-8601 and which ``new Date(...)`` rejects outside V8. The
websocket and broadcast encoders already used ``.isoformat()``. This
module keeps ``.isoformat()`` — the majority rule and the only one the
frontends can parse.

Unknown stays unknown (§7, §9)
------------------------------
``default=str`` was a silent catch-all: a model instance in a payload
shipped ``"<Order object at 0x10c3f2a10>"`` to a customer with a 200, and
``allow_nan`` let a float ``NaN`` through as the bare token ``NaN``, which
is not JSON — ``JSON.parse`` throws, so the response was already broken,
just broken at the far end where nobody owns it. Both now raise, and
*where* they raise differs by boundary, which callers should know:

- A buffered response encodes eagerly, inside ``response.json(...)``, so
  the raise happens while the request is still the framework's to fail —
  the exception handler renders an ordinary 500 and the client gets a
  well-formed error body.
- A stream has already put its headers on the wire, so it cannot change
  status. ``StreamingResponse.stream`` logs and closes the body: the
  client sees a short read it can retry rather than a chunk that
  ``JSON.parse`` rejects. ``BaseResponse._handle_error`` names this same
  case ("JSON-encoding a Decimal that the encoder doesn't know how to
  serialise").

Either way the conversion belongs in the resource layer, where the value
still has a meaning to convert.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

__all__ = ["decimal_to_wire", "json_default", "json_dumps"]


def decimal_to_wire(value: Decimal) -> str:
    """Render a ``Decimal`` for a JSON wire: its exact digits, as text.

    ``str(Decimal)`` is exact by construction — it is the shortest string
    that reproduces the same ``Decimal`` — so the value survives the hop
    at full precision and scale, trailing zeros included
    (``Decimal("19.90")`` stays ``"19.90"``, which is what a price
    formatter downstream needs).

    A non-finite ``Decimal`` is refused rather than rendered. ``NaN`` is
    not a quantity and ``Infinity`` is not a price; both are the absence
    of a number wearing a number's clothes, and §7 says unknown is NULL.
    ``JsonCacheCodec`` already refuses them for the same reason.
    """
    if not value.is_finite():
        raise ValueError(
            f"Cannot serialize non-finite Decimal {value!r} to JSON: "
            "a NaN or Infinite quantity is unknown, and unknown is null."
        )
    return str(value)


def json_default(value: Any) -> Any:
    """``json.dumps(default=...)`` for every wire cara writes.

    Returns a value ``json`` re-encodes, so a rule may hand back another
    type this function knows (an ``Enum`` wrapping a ``Decimal`` resolves
    in two passes).
    """
    if isinstance(value, Decimal):
        return decimal_to_wire(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serializable. "
        "Convert it where its meaning is known — the resource or "
        "serializer layer — rather than at the wire."
    )


def json_dumps(obj: Any, **kwargs: Any) -> str:
    """Serialize ``obj`` with cara's wire rules.

    ``allow_nan=False`` matches ``SignedJsonJobSerializer._canonical_json``:
    stdlib ``json`` otherwise emits the bare tokens ``NaN`` / ``Infinity``,
    which no JSON parser accepts. Callers may override any of these
    (``indent`` for a human-facing dump, ``sort_keys`` for canonical
    bytes) but overriding ``default`` puts a second encode rule on the
    wire and defeats the purpose of this module.
    """
    kwargs.setdefault("ensure_ascii", False)
    kwargs.setdefault("allow_nan", False)
    kwargs.setdefault("default", json_default)
    return json.dumps(obj, **kwargs)
