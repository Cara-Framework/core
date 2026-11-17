"""``cara.support.parse_iso_moment`` — one strict parser for client datetimes."""

from __future__ import annotations

from datetime import UTC, datetime

import pendulum
import pytest

from cara.support import parse_iso_moment


def test_offset_moment_comes_back_aware_utc():
    parsed = parse_iso_moment("2026-05-05T10:00:00+02:00")
    assert parsed == pendulum.datetime(2026, 5, 5, 8, 0, 0, tz="UTC")
    assert parsed.timezone_name == "UTC"


def test_blank_and_none_pass_through_as_none():
    assert parse_iso_moment(None) is None
    assert parse_iso_moment("   ") is None


def test_datetime_instance_passes_through_naive_read_as_utc():
    assert parse_iso_moment(datetime(2026, 1, 1, 12)) == pendulum.datetime(2026, 1, 1, 12)
    aware = datetime(2026, 1, 1, 12, tzinfo=UTC)
    assert parse_iso_moment(aware).timezone_name == "UTC"


@pytest.mark.parametrize(
    "junk",
    [
        "P1D",  # duration: parses, but has no in_timezone
        "2026-01-01/2026-02-01",  # interval
        "May 5 2026",  # human spelling is refused, not guessed
        "not a date",
        12345,
        ["2026-05-05T10:00:00Z"],
    ],
)
def test_anything_but_a_single_instant_raises_value_error(junk):
    with pytest.raises(ValueError):
        parse_iso_moment(junk)


def test_bare_date_is_that_days_utc_midnight():
    assert parse_iso_moment("2026-05-05") == pendulum.datetime(2026, 5, 5)
