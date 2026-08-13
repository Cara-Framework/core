from __future__ import annotations

from datetime import UTC, date, datetime

from cara.support.DateSerialization import iso_datetime


def test_iso_datetime_normalizes_objects_and_raw_sql_strings() -> None:
    assert iso_datetime(datetime(2026, 8, 13, 1, 2, 3)) == ("2026-08-13T01:02:03+00:00")
    assert iso_datetime(datetime(2026, 8, 13, 1, 2, 3, tzinfo=UTC)) == (
        "2026-08-13T01:02:03+00:00"
    )
    assert iso_datetime("2026-08-13 01:02:03+00:00") == ("2026-08-13T01:02:03+00:00")
    assert iso_datetime("2026-08-13 01:02:03") == ("2026-08-13T01:02:03+00:00")
    assert iso_datetime(date(2026, 8, 13)) == "2026-08-13"


def test_iso_datetime_preserves_unknown_and_absent_values() -> None:
    assert iso_datetime(None) is None
    assert iso_datetime("  ") is None
    assert iso_datetime("not-a-date") == "not-a-date"
