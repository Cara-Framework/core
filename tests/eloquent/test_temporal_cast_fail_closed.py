"""Temporal casts never preserve malformed values as database-ready text."""

from __future__ import annotations

import datetime as dt

import pytest

from cara.eloquent.casts.DateCast import DateCast
from cara.eloquent.casts.DateTimeCast import DateTimeCast
from cara.eloquent.casts.TimeCast import TimeCast


@pytest.mark.parametrize("cast", [DateCast(), DateTimeCast(), TimeCast()])
def test_malformed_temporal_value_is_unknown_on_read_and_write(cast) -> None:
    assert cast.get("not-a-temporal-value") is None
    assert cast.set("not-a-temporal-value") is None


def test_valid_temporal_values_still_canonicalize() -> None:
    assert DateCast().set("2026-08-13T12:30:00Z") == "2026-08-13"
    assert TimeCast().set("12:30:45") == "12:30:45"
    assert DateTimeCast().set(dt.datetime(2026, 8, 13, 12, 30, tzinfo=dt.UTC)) == (
        "2026-08-13 12:30:00+00:00"
    )
