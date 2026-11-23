"""Fluent ``.without_overlapping()`` holds its lock for the documented 24 hours.

The default was 1440 — Laravel's number, which is minutes there — so the
lock lapsed after 24 MINUTES and a longer run lost its overlap protection.
The dict-config path and the driver fallback already used 86400 seconds.
"""

from __future__ import annotations

from cara.scheduling.ScheduleBuilder import ScheduleBuilder


def _builder() -> ScheduleBuilder:
    return ScheduleBuilder(None, "nightly", lambda: None, {})


def test_the_default_lock_timeout_is_one_day_in_seconds() -> None:
    options = _builder().without_overlapping().options

    assert options["without_overlapping"] is True
    assert options["lock_timeout"] == 86400


def test_an_explicit_timeout_is_kept() -> None:
    assert _builder().without_overlapping(900).options["lock_timeout"] == 900
