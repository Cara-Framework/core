"""``DateManager.parse``'s timezone argument has exactly ONE meaning.

The argument used to mean "localize" on the ``pendulum.parse`` path and
"convert, assuming the wall clock was UTC" everywhere else. Because the two
day-first formats are precisely the ones ``pendulum.parse`` rejects, they were
the only inputs that ever reached the second path — and west of Greenwich they
came back on the WRONG CALENDAR DAY: ``parse("01/12/2023", "America/New_York")``
answered 2023-11-30 19:00. ``is_today`` inherited the shift, so a date entered
as ``08/08/2026`` was not "today" on 2026-08-08.

The formats are read from ``SUPPORTED_FORMATS`` rather than restated here: a
newly supported format must obey the contract on the day it is added.
"""

from __future__ import annotations

import datetime as dt

import pendulum
import pytest

from cara.eloquent.utils.DateManager import DateManager

# One zone behind UTC and one ahead, so a "treat the wall clock as UTC" bug
# cannot cancel itself out.
ZONES = ["America/New_York", "Asia/Tokyo"]


@pytest.mark.parametrize("zone", ZONES)
def test_every_supported_format_localizes_to_the_same_instant(zone: str) -> None:
    """Midnight on 2023-12-01 is one instant, however it was spelled."""
    samples = {
        "%Y-%m-%d %H:%M:%S": "2023-12-01 00:00:00",
        "%Y-%m-%d %H:%M:%S.%f": "2023-12-01 00:00:00.000000",
        "%Y-%m-%d": "2023-12-01",
        "%Y/%m/%d": "2023/12/01",
        "%d/%m/%Y": "01/12/2023",
        "%d-%m-%Y": "01-12-2023",
        "%Y-%m-%dT%H:%M:%S": "2023-12-01T00:00:00",
        "%Y-%m-%dT%H:%M:%S.%f": "2023-12-01T00:00:00.000000",
    }
    # Every non-UTC-anchored format the class advertises must be covered here.
    uncovered = [
        fmt
        for fmt in DateManager.SUPPORTED_FORMATS
        if fmt not in samples and not fmt.endswith("Z")
    ]
    assert not uncovered, f"unsampled formats: {uncovered}"

    expected = pendulum.datetime(2023, 12, 1, tz=zone)
    for fmt, value in samples.items():
        parsed = DateManager.parse(value, zone)
        assert parsed == expected, f"{fmt} ({value}) parsed to {parsed}"
        assert parsed.day == 1, f"{fmt} ({value}) shifted the calendar day"


@pytest.mark.parametrize("zone", ZONES)
def test_naive_datetime_is_localized_not_converted(zone: str) -> None:
    parsed = DateManager.parse(dt.datetime(2023, 12, 1), zone)

    assert parsed == pendulum.datetime(2023, 12, 1, tz=zone)


@pytest.mark.parametrize("zone", ZONES)
def test_offset_bearing_input_is_converted_not_relabelled(zone: str) -> None:
    """An instant keeps its identity; only its spelling changes."""
    aware = dt.datetime(2023, 12, 1, tzinfo=dt.UTC)

    parsed = DateManager.parse(aware, zone)

    assert parsed == pendulum.datetime(2023, 12, 1, tz="UTC")
    assert parsed.timezone_name == zone


@pytest.mark.parametrize("zone", ZONES)
def test_utc_anchored_format_is_converted(zone: str) -> None:
    """A literal ``Z`` names an instant, so it converts rather than localizes."""
    parsed = DateManager.parse("2023-12-01T15:30:45Z", zone)

    assert parsed == pendulum.datetime(2023, 12, 1, 15, 30, 45, tz="UTC")


def test_is_today_accepts_a_day_first_date() -> None:
    today = pendulum.now("America/New_York")

    assert DateManager.is_today(today.format("DD/MM/YYYY"), "America/New_York")
