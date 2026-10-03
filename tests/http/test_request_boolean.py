"""``Request.boolean()`` — a validated flag is read the way the rule meant it.

The ``boolean`` rule accepts ``"false"`` / ``"0"`` / ``"no"`` and does not
convert them, and ``bool("false")`` is ``True``. Products read validated flags
by truthiness and inverted them: an undelivered buyer reply marked delivered,
a price override cleared by ``clear_price: "false"``, ``?back=0`` paging
backwards. The accessor reads input through the rule's own vocabulary
(``safe_bool``), so what the rule admits is never read the other way round.
"""

from __future__ import annotations

import importlib
import json

import pytest

from cara.http import Request
from cara.support import safe_bool
from cara.validation import Validation
from cara.validation.rules import BooleanRule

TRUE_SPELLINGS = [True, 1, "1", "true", "TRUE", "True", "yes", "YES"]
FALSE_SPELLINGS = [False, 0, "0", "false", "FALSE", "False", "no", "No"]
NOT_BOOLEAN = [None, "", " ", "maybe", "on", "off", " true", 2, -1, 1.0, [], {}]

_body_parsing = importlib.import_module("cara.http.request.mixins.MakesBodyParsing")


@pytest.fixture(autouse=True)
def _body_limits(monkeypatch) -> None:
    """Body parsing reads boot config; pin its limits so no app is needed."""
    monkeypatch.setattr(
        _body_parsing,
        "_body_limits",
        lambda: {"MAX_BODY_SIZE": 1 << 20, "MAX_FILE_SIZE": 1 << 20, "MAX_FILES": 1},
    )


def _json_request(body: dict) -> Request:
    raw = json.dumps(body).encode()

    async def receive() -> dict:
        return {"type": "http.request", "body": raw, "more_body": False}

    headers = [(b"content-type", b"application/json")]
    return Request(None).load({"type": "http", "headers": headers}, receive)


def _query_request(query: bytes) -> Request:
    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(None).load({"type": "http", "query_string": query}, receive)


@pytest.mark.parametrize("value", TRUE_SPELLINGS)
def test_safe_bool_reads_every_true_spelling(value) -> None:
    assert safe_bool(value) is True


@pytest.mark.parametrize("value", FALSE_SPELLINGS)
def test_safe_bool_reads_every_false_spelling(value) -> None:
    assert safe_bool(value) is False


@pytest.mark.parametrize("value", NOT_BOOLEAN)
def test_safe_bool_answers_none_outside_the_vocabulary(value) -> None:
    assert safe_bool(value) is None


@pytest.mark.parametrize("value", TRUE_SPELLINGS + FALSE_SPELLINGS + NOT_BOOLEAN)
def test_the_rule_and_the_conversion_share_one_vocabulary(value) -> None:
    """Whatever the rule admits converts to a bool; whatever it refuses doesn't."""
    admitted = BooleanRule().validate("flag", value, {})
    assert admitted is (safe_bool(value) is not None)


@pytest.mark.asyncio
@pytest.mark.parametrize("value", FALSE_SPELLINGS)
async def test_a_validated_false_is_read_as_false(value) -> None:
    request = _json_request({"delivered": value})
    validated = Validation.make({"delivered": value}, {"delivered": "required|boolean"})
    assert validated.passes()

    assert await request.boolean("delivered") is False


@pytest.mark.asyncio
@pytest.mark.parametrize("value", TRUE_SPELLINGS)
async def test_a_validated_true_is_read_as_true(value) -> None:
    assert await _json_request({"delivered": value}).boolean("delivered") is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("query", "expected"),
    [(b"back=0", False), (b"back=false", False), (b"back=1", True), (b"back=yes", True)],
)
async def test_query_string_flags_are_read_by_value_not_presence(query, expected) -> None:
    assert await _query_request(query).boolean("back") is expected


@pytest.mark.asyncio
async def test_absent_or_blank_input_answers_the_default() -> None:
    assert await _json_request({}).boolean("flag") is False
    assert await _json_request({}).boolean("flag", True) is True
    assert await _json_request({"flag": None}).boolean("flag", True) is True
    assert await _query_request(b"flag=").boolean("flag", True) is True
