"""Edge inputs that used to escape three "never raises" support helpers.

* ``strip_header_crlf`` scrubbed CR/LF only; ``email.header`` also breaks a
  value on VT, FF and FS/GS/RS, so the send raised instead of going out.
* ``data_get`` raised ``TypeError`` when a path segment was a scalar.
* ``safe_int`` let ``OverflowError`` through for an infinite float.
"""

from __future__ import annotations

import json
from decimal import Decimal
from email.mime.multipart import MIMEMultipart

import pytest

from cara.support import data_get, safe_int, strip_header_crlf


@pytest.mark.parametrize(
    "separator", ["\r", "\n", "\x0b", "\x0c", "\x1c", "\x1d", "\x1e"]
)
def test_a_header_value_with_any_line_break_still_serialises(separator):
    message = MIMEMultipart("alternative")

    message["Subject"] = strip_header_crlf(
        f"Order update{separator}Bcc: evil@example.com"
    )
    wire = message.as_string()

    assert "Subject: Order update Bcc: evil@example.com" in wire
    assert "\nBcc:" not in wire


def test_header_scrub_keeps_ordinary_text_and_rejects_non_strings():
    assert strip_header_crlf("Günaydın — order #42") == "Günaydın — order #42"
    assert strip_header_crlf(None) == ""


@pytest.mark.parametrize("scalar", [5, 2.5, True, False])
def test_data_get_under_a_scalar_parent_is_the_default(scalar):
    assert data_get({"address": scalar}, "address.city", "fallback") == "fallback"


def test_data_get_still_reads_nested_and_wildcard_paths():
    payload = {"address": {"city": "Izmir"}, "items": [{"sku": "a"}, {"sku": "b"}]}

    assert data_get(payload, "address.city") == "Izmir"
    assert data_get(payload, "items.*.sku") == ["a", "b"]
    assert data_get(payload, "address.zip", "none") == "none"


@pytest.mark.parametrize(
    "value",
    [
        float("inf"),
        float("-inf"),
        float("nan"),
        Decimal("Infinity"),
        json.loads('{"quantity": 1e999}')["quantity"],
    ],
)
def test_safe_int_answers_none_for_a_non_finite_number(value):
    assert safe_int(value) is None


def test_safe_int_still_converts_numbers():
    assert safe_int("42") == 42
    assert safe_int(7.9) == 7
    assert safe_int(None) is None
    assert safe_int("abc") is None
