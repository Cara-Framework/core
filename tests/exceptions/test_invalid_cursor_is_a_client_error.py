"""A tampered pagination cursor is a 4xx, not a 500.

``cara/http/Cursor.py`` promises in its own module docstring that it
"fails closed by raising :class:`InvalidCursor`" — but ``InvalidCursor``
was a bare ``ValueError`` outside the taxonomy, so it carried no
``status_code`` and ``DefaultExceptionHandler.get_status_code`` took its
"# Default to 500 for unknown exceptions" branch. Any endpoint built on
the ORM's own ``cursor_paginate`` therefore answered an edited query
string with ``500 {"error": "Internal server error"}`` plus an
ERROR-level traceback: a client fault recorded as a server fault, and no
actionable message for the caller. Both products had to restate the
translation themselves, which is the framework-gap-restated-in-the-product
pattern §5 forbids.
"""

from __future__ import annotations

import pathlib

import pytest

from cara.exceptions.handlers.DefaultExceptionHandler import DefaultExceptionHandler
from cara.http import InvalidCursor
from cara.http.Cursor import cursor_fingerprint, decode_cursor, encode_cursor

_SECRET = "x" * 48
_SCOPE = "listings.index"


def _valid_token() -> tuple[str, str]:
    fingerprint = cursor_fingerprint({"status": "active"})
    token = encode_cursor(
        "2026-08-09T00:00:00+00:00",
        41,
        direction="desc",
        fingerprint=fingerprint,
        scope=_SCOPE,
        secret=_SECRET,
    )
    return token, fingerprint


def _decode(token: str, fingerprint: str) -> None:
    decode_cursor(
        token,
        direction="desc",
        fingerprint=fingerprint,
        scope=_SCOPE,
        secret=_SECRET,
    )


def test_a_tampered_cursor_is_answered_with_422_not_500() -> None:
    token, fingerprint = _valid_token()
    body, signature = token.split(".", 1)
    tampered = f"{body[:-1]}{'A' if body[-1] != 'A' else 'B'}.{signature}"

    with pytest.raises(InvalidCursor) as raised:
        _decode(tampered, fingerprint)

    handler = DefaultExceptionHandler()
    status = handler.get_status_code(raised.value)

    assert status == 422, (
        "A tampered cursor is bad client input. It used to fall through to "
        "the handler's unknown-exception branch and answer 500."
    )


def test_the_422_body_carries_the_validation_discriminator() -> None:
    token, fingerprint = _valid_token()

    with pytest.raises(InvalidCursor) as raised:
        _decode(token, "0" * 64)

    handler = DefaultExceptionHandler()
    body = handler.format_response(raised.value, handler.get_status_code(raised.value))

    assert body["type"] == "validation_error"
    assert body["error"] == "Cursor filters do not match this query."


def test_a_cursor_from_another_endpoint_is_also_a_client_error() -> None:
    """Cross-endpoint reuse is a tampering signal, not a server fault."""
    token, fingerprint = _valid_token()

    with pytest.raises(InvalidCursor) as raised:
        decode_cursor(
            token,
            direction="desc",
            fingerprint=fingerprint,
            scope="orders.index",
            secret=_SECRET,
        )

    assert DefaultExceptionHandler().get_status_code(raised.value) == 422


def test_invalid_cursor_stays_a_value_error() -> None:
    """Four product call sites catch ``(InvalidCursor, TypeError, ValueError)``.

    Dropping the ``ValueError`` base would turn their fail-closed 422s into
    unhandled 500s, so the dual inherit is part of the contract — not a
    shim.
    """
    assert issubclass(InvalidCursor, ValueError)


def test_the_class_is_still_reachable_under_its_historical_names() -> None:
    """Products import it from ``cara.http``; the framework from ``cara.http.Cursor``."""
    import cara.exceptions as exceptions
    import cara.http.Cursor as cursor_module

    assert cursor_module.InvalidCursor is InvalidCursor
    assert exceptions.InvalidCursor is InvalidCursor


def test_the_http_barrel_resolves_the_class_from_the_taxonomy() -> None:
    """§5 bans the shim the move left behind.

    ``cara.http``'s lazy export map went on naming ``cara.http.Cursor`` as
    ``InvalidCursor``'s home after the class had moved into the taxonomy, and
    ``cara/http/Cursor.py`` therefore had to keep an
    ``import InvalidCursor as InvalidCursor`` re-export alive purely so the
    map could resolve. Each half existed only to serve the other — a
    self-justifying compatibility hop, and the reason a reader looking for
    where the exception is DEFINED found two answers.
    """
    import cara.http as http_barrel

    assert http_barrel._LAZY_EXPORTS["InvalidCursor"] == (
        "cara.exceptions.types.InvalidCursor",
        "InvalidCursor",
    )


def test_reaching_the_exception_does_not_drag_in_the_cursor_codec() -> None:
    """The shim was not free: it made an exception import a crypto codec.

    Driven in a FRESH interpreter, because ``sys.modules`` in this one is
    already polluted by every other test that touched the codec. Pre-fix
    ``from cara.http import InvalidCursor`` imported ``cara.http.Cursor``
    (hmac, hashlib, json, decimal, the whole signing path) to hand back a
    class defined three packages away.
    """
    import subprocess
    import sys

    probe = (
        "import sys, cara.http;"
        "cls = cara.http.InvalidCursor;"
        "print('cara.http.Cursor' in sys.modules, cls.__module__)"
    )
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(pathlib.Path(__file__).resolve().parents[2]),
    )

    assert completed.stdout.split() == ["False", "cara.exceptions.types.InvalidCursor"], (
        f"unexpected probe output: {completed.stdout!r} / {completed.stderr!r}"
    )
