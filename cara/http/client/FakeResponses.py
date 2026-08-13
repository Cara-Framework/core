"""Normalize HTTP fake stubs without coupling registry state to its facade."""

from __future__ import annotations

from typing import Any

import httpx


def make_response(
    json: Any = None,
    status: int = 200,
    headers: dict[str, str] | None = None,
    body: str | bytes | None = None,
) -> httpx.Response:
    """Build a real ``httpx.Response`` stub."""
    kwargs: dict[str, Any] = {"status_code": status, "headers": headers or {}}
    if json is not None:
        kwargs["json"] = json
    elif body is not None:
        kwargs["content"] = body.encode() if isinstance(body, str) else body
    return httpx.Response(**kwargs)


def coerce(stub: Any) -> httpx.Response:
    """Normalize a registered stub into an ``httpx.Response``."""
    if isinstance(stub, httpx.Response):
        return stub
    if isinstance(stub, int):
        return make_response(status=stub)
    if isinstance(stub, (dict, list)):
        return make_response(json=stub)
    if isinstance(stub, str):
        return make_response(body=stub)
    raise TypeError(f"Unsupported fake response stub: {type(stub).__name__}")
