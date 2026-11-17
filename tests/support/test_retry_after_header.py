"""``cara.support.retry_after_seconds`` — the one outbound ``Retry-After`` reader."""

from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from cara.http.client.PendingRequest import _retry_after_seconds
from cara.support import retry_after_seconds

NOW = datetime(2026, 10, 21, 7, 28, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"Retry-After": "30"}, 30.0),
        ({"retry-after": "0"}, 0.0),
        ({"RETRY-AFTER": " 12 "}, 12.0),
        ({"Retry-After": "1.5"}, 1.5),
        ([("Retry-After", "7")], 7.0),
        (httpx.Headers({"retry-after": "9"}), 9.0),
    ],
)
def test_delay_seconds_are_read_whatever_the_header_case_or_container(headers, expected):
    assert retry_after_seconds(headers) == expected


def test_an_http_date_is_the_seconds_left_until_it():
    headers = {"Retry-After": "Wed, 21 Oct 2026 07:30:00 GMT"}
    assert retry_after_seconds(headers, now=NOW) == 120.0


def test_an_http_date_already_past_is_no_wait():
    headers = {"Retry-After": "Wed, 21 Oct 2026 07:00:00 GMT"}
    assert retry_after_seconds(headers, now=NOW) == 0.0


@pytest.mark.parametrize(
    "value", ["", "   ", "-5", "soon", "²", "１２０", "1e3", "inf", "nan"]
)
def test_an_unreadable_value_names_no_wait(value):
    assert retry_after_seconds({"Retry-After": value}) is None


@pytest.mark.parametrize("headers", [None, {}, {"X-Other": "5"}, 42, object()])
def test_no_header_names_no_wait(headers):
    assert retry_after_seconds(headers) is None


def test_the_cap_bounds_both_forms():
    assert retry_after_seconds({"Retry-After": "86400"}, cap=60) == 60.0
    far = {"Retry-After": "Fri, 23 Oct 2026 07:28:00 GMT"}
    assert retry_after_seconds(far, cap=3600, now=NOW) == 3600.0


def test_pending_request_reads_through_the_shared_parser():
    response = httpx.Response(429, headers={"retry-after": "600"})
    assert _retry_after_seconds(response) == 60.0
