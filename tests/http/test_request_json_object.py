"""``Request.json_object`` — the strict body-shape guard.

``json_dict`` turns a wrong-shaped body into ``{}`` and lets the endpoint
answer "field is required". ``json_object`` is the sibling that refuses,
so the client is told what is actually wrong with the request it sent.
"""

from __future__ import annotations

import json

import pytest

from cara.exceptions import BadRequestException
from cara.exceptions.types.ValidationException import ValidationException
from cara.http.request.Request import Request


def _request_for_body(raw: bytes) -> Request:
    async def receive() -> dict:
        return {"type": "http.request", "body": raw, "more_body": False}

    return Request(None).load({"type": "http"}, receive)


@pytest.mark.asyncio
async def test_json_object_returns_the_object_untouched() -> None:
    request = _request_for_body(json.dumps({"name": "ada", "age": 36}).encode())

    assert await request.json_object() == {"name": "ada", "age": 36}


@pytest.mark.asyncio
async def test_empty_body_is_nothing_sent_not_wrong_shape() -> None:
    """An absent body is the "no fields" case every PATCH endpoint allows."""
    request = _request_for_body(b"")

    assert await request.json_object() == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    [b"[1, 2, 3]", b'"text"', b"7", b"true", b"null"],
    ids=["array", "string", "number", "boolean", "null"],
)
async def test_non_object_bodies_are_rejected(raw: bytes) -> None:
    request = _request_for_body(raw)

    if raw == b"null":
        # JSON ``null`` decodes to ``None``: the caller sent "nothing", which
        # is the empty-body case, not a wrongly shaped payload.
        assert await request.json_object() == {}
        return

    with pytest.raises(ValidationException) as caught:
        await request.json_object()

    error = caught.value
    assert error.status_code == 422
    assert error.errors == {"body": ["Request body must be a JSON object."]}


@pytest.mark.asyncio
async def test_rejection_emits_the_canonical_422_envelope() -> None:
    """Clients read ``type`` and ``errors.<field>[0]``; both must be present."""
    request = _request_for_body(b"[1, 2]")

    with pytest.raises(ValidationException) as caught:
        await request.json_object()

    envelope = caught.value.to_dict()
    assert envelope["type"] == "validation_error"
    assert envelope["errors"]["body"] == ["Request body must be a JSON object."]
    assert envelope["meta"]["failed_fields"] == ["body"]


@pytest.mark.asyncio
async def test_malformed_json_keeps_the_framework_400_and_is_not_rewrapped() -> None:
    """One broken body must not report two error types across endpoints.

    ``FormRequest``-backed routes already surface unparseable JSON as the
    framework's 400. Re-wrapping it here would mean the same curl gets a 422
    on a hand-rolled endpoint and a 400 on a validated one.
    """
    request = _request_for_body(b"{not json")

    with pytest.raises(BadRequestException):
        await request.json_object()


@pytest.mark.asyncio
async def test_json_dict_still_coerces_so_the_strict_path_is_a_real_choice() -> None:
    request = _request_for_body(b"[1, 2]")

    assert await request.json_dict() == {}
