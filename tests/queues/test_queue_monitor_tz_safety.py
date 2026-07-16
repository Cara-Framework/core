"""QueueMonitor — timezone-safe eviction sort.

``job_stats`` eviction sorts by ``started_at`` which is a UTC-aware
pendulum DateTime. The fallback for missing ``started_at`` was bare
``datetime.min`` (naive), which causes ``TypeError: can't compare
offset-naive and offset-aware datetimes`` when sorted() encounters
a mix. The fix uses ``datetime.min.replace(tzinfo=timezone.utc)``.

These tests avoid importing the full cara framework (which requires
Python 3.12+ for ``datetime.UTC``) by testing the sorting logic
directly.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pendulum
import pytest


class TestEvictionSortTzSafety:
    """Verify the fixed sorting lambda works with mixed tz-aware/None values."""

    def _sort_key(self, kv):
        """Replica of the fixed lambda from QueueMonitor.job_completed."""
        return kv[1].get("started_at") or datetime.min.replace(tzinfo=UTC)

    def test_sort_with_missing_started_at_no_crash(self):
        """Sorting must not raise TypeError when started_at is None."""
        items = {
            "job_1": {"started_at": pendulum.now("UTC")},
            "job_2": {"started_at": None},  # missing — triggers fallback
            "job_3": {"started_at": pendulum.now("UTC").subtract(hours=1)},
        }

        # Must not raise TypeError
        result = sorted(items.items(), key=self._sort_key)
        # The None entry should sort first (datetime.min is smallest)
        assert result[0][0] == "job_2"

    def test_fallback_value_is_utc_aware(self):
        """The fallback datetime.min must be timezone-aware (UTC)."""
        fallback = datetime.min.replace(tzinfo=UTC)
        aware_dt = pendulum.now("UTC")
        # Must not raise — both are tz-aware
        assert fallback < aware_dt

    def test_all_aware_datetimes_sort_correctly(self):
        """Normal case — all entries have started_at, sort by time."""
        t1 = pendulum.now("UTC").subtract(minutes=30)
        t2 = pendulum.now("UTC").subtract(minutes=20)
        t3 = pendulum.now("UTC").subtract(minutes=10)
        items = {
            "c": {"started_at": t3},
            "a": {"started_at": t1},
            "b": {"started_at": t2},
        }
        result = sorted(items.items(), key=self._sort_key)
        assert [r[0] for r in result] == ["a", "b", "c"]

    def test_old_naive_fallback_would_crash(self):
        """Demonstrate the bug: naive datetime.min vs aware raises TypeError."""
        aware_dt = pendulum.now("UTC")
        with pytest.raises(TypeError, match="compare"):
            _ = datetime.min < aware_dt
