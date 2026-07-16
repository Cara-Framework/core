from __future__ import annotations

import pytest

from cara.http.Cursor import (
    InvalidCursor,
    cursor_fingerprint,
    cursor_rules,
    decode_cursor,
    encode_cursor,
)
from cara.validation import Validation

_SECRET = "signed-cursor-unit-secret-" * 2


def _token(**overrides) -> tuple[str, str]:
    fingerprint = cursor_fingerprint(
        overrides.pop("filters", {"tenant_id": 8, "status": "active"})
    )
    token = encode_cursor(
        overrides.pop("sort_value", "2026-07-16T12:00:00+00:00"),
        overrides.pop("primary_key", 42),
        direction=overrides.pop("direction", "desc"),
        fingerprint=fingerprint,
        scope=overrides.pop("scope", "listings.index"),
        secret=_SECRET,
    )
    assert not overrides
    return token, fingerprint


def test_signed_cursor_round_trips_composite_position() -> None:
    token, fingerprint = _token()
    payload = decode_cursor(
        token,
        direction="desc",
        fingerprint=fingerprint,
        scope="listings.index",
        secret=_SECRET,
    )
    assert payload["v"] == "2026-07-16T12:00:00+00:00"
    assert payload["id"] == 42


def test_tampered_cursor_fails_closed() -> None:
    token, fingerprint = _token()
    body, signature = token.split(".", 1)
    tampered = f"{body[:-1]}{'A' if body[-1] != 'A' else 'B'}.{signature}"
    with pytest.raises(InvalidCursor):
        decode_cursor(
            tampered,
            direction="desc",
            fingerprint=fingerprint,
            scope="listings.index",
            secret=_SECRET,
        )


@pytest.mark.parametrize("part", ["body", "signature"])
def test_noncanonical_base64url_cursor_fails_closed(part: str) -> None:
    token, fingerprint = _token()
    body, signature = token.split(".", 1)
    if part == "body":
        token = f"{body}=.{signature}"
    else:
        token = f"{body}.{signature}="

    with pytest.raises(InvalidCursor):
        decode_cursor(
            token,
            direction="desc",
            fingerprint=fingerprint,
            scope="listings.index",
            secret=_SECRET,
        )


@pytest.mark.parametrize(
    ("direction", "fingerprint", "scope"),
    [
        ("asc", None, "listings.index"),
        ("desc", cursor_fingerprint({"tenant_id": 9}), "listings.index"),
        ("desc", None, "orders.index"),
    ],
)
def test_cursor_is_bound_to_direction_filters_and_endpoint(
    direction: str, fingerprint: str | None, scope: str
) -> None:
    token, issued_fingerprint = _token()
    with pytest.raises(InvalidCursor):
        decode_cursor(
            token,
            direction=direction,
            fingerprint=fingerprint or issued_fingerprint,
            scope=scope,
            secret=_SECRET,
        )


@pytest.mark.parametrize("token", ["", "abc", "not-base64.not-base64", "x" * 4097])
def test_malformed_cursor_fails_closed(token: str) -> None:
    with pytest.raises(InvalidCursor):
        decode_cursor(
            token,
            direction="desc",
            fingerprint=cursor_fingerprint({}),
            scope="tests",
            secret=_SECRET,
        )


def test_encoder_never_emits_a_cursor_the_decoder_must_reject() -> None:
    with pytest.raises(ValueError, match="maximum token length"):
        _token(sort_value="x" * 4096)


@pytest.mark.parametrize("field", ["page", "offset"])
@pytest.mark.parametrize("value", ["", None, 0, "10"])
def test_legacy_pagination_aliases_must_be_absent(field: str, value) -> None:
    validator = Validation.make({field: value}, cursor_rules())
    assert validator.fails()


def test_omitted_cursor_is_valid_and_stays_omitted() -> None:
    validator = Validation.make({}, cursor_rules())

    assert validator.passes()
    assert "cursor" not in validator.validated()


@pytest.mark.parametrize("value", ["", " ", None, 42, False])
def test_cursor_must_be_a_non_empty_string_when_present(value) -> None:
    validator = Validation.make({"cursor": value}, cursor_rules())

    assert validator.fails()
    assert "cursor" in validator.errors()


@pytest.mark.parametrize(
    ("min_limit", "max_limit"),
    [(0, 100), (1, 101), (50, 49)],
)
def test_cursor_rule_bounds_are_rejected(min_limit: int, max_limit: int) -> None:
    with pytest.raises(ValueError):
        cursor_rules(min_limit=min_limit, max_limit=max_limit)
