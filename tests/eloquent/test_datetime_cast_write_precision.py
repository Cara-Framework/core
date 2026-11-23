"""An ORM write keeps the instant's microseconds.

Columns are ``timestamptz(6)``, and the rest of the stack writes them at full
precision — ``NOW()`` in SQL and pendulum instances bound into raw statements.
``DateTimeCast.set`` wrote ``to_datetime_string()``, which drops the fraction,
so an ORM write landed up to a second BEFORE a raw write of the same instant
and ordering CHECKs across the two broke: a product's ``sync_task`` refused
essentially every push confirmation (``confirmed_at >= sent_at``).

The fraction is appended only when there is one, so an instant without
microseconds renders exactly as it always did.
"""

from __future__ import annotations

import pendulum

from cara.eloquent.casts.DateTimeCast import DateTimeCast
from cara.eloquent.casts.TimestampCast import TimestampCast


def test_set_keeps_the_microseconds_of_an_instant() -> None:
    instant = pendulum.datetime(2026, 9, 26, 12, 20, 3, 223979, tz="UTC")
    assert DateTimeCast().set(instant) == "2026-09-26 12:20:03.223979+00:00"


def test_set_renders_a_whole_second_exactly_as_before() -> None:
    instant = pendulum.datetime(2026, 9, 26, 12, 20, 3, tz="UTC")
    assert DateTimeCast().set(instant) == "2026-09-26 12:20:03+00:00"


def test_a_written_instant_never_sorts_before_the_same_raw_instant() -> None:
    instant = pendulum.datetime(2026, 9, 26, 12, 20, 3, 500000, tz="UTC")
    written = pendulum.parse(DateTimeCast().set(instant), tz="UTC")
    assert written == instant


def test_offset_instants_are_written_in_utc_with_their_fraction() -> None:
    instant = pendulum.datetime(2026, 9, 26, 15, 20, 3, 1, tz="Europe/Istanbul")
    assert DateTimeCast.sql_instant(instant) == "2026-09-26 12:20:03.000001+00:00"


def test_epoch_floats_keep_their_fraction() -> None:
    assert TimestampCast().set(1790000000.25).endswith(".250000+00:00")


def test_timestamp_cast_writes_an_aware_datetime_as_the_same_instant() -> None:
    instant = pendulum.datetime(2026, 9, 26, 15, 20, 3, 1, tz="Europe/Istanbul")
    assert TimestampCast().set(instant) == "2026-09-26 12:20:03.000001+00:00"
