## Add edge-case tests for Time helpers

The existing `tests/support/test_time.py` covers basic paths but misses boundary values. Add edge-case tests for `humanize_seconds` and `format_duration` covering large values, exact-boundary values, and seconds-only inputs.

### tests/support/test_time.py

Append these new test functions at the end of the file (do NOT remove or modify any existing tests):

```python
def test_humanize_seconds_exact_boundaries():
    from cara.support.Time import humanize_seconds
    # exactly 1 minute
    assert humanize_seconds(60) == "1 minute"
    # exactly 1 hour
    assert humanize_seconds(3600) == "1 hour"
    # exactly 1 day
    assert humanize_seconds(86400) == "1 day"
    # 1 second
    assert humanize_seconds(1) == "1 second"
    # 59 seconds
    assert humanize_seconds(59) == "59 seconds"


def test_humanize_seconds_large_value():
    from cara.support.Time import humanize_seconds
    # 7 days exactly
    assert humanize_seconds(604800) == "7 days"
    # 1 day 1 hour 1 minute 1 second
    assert humanize_seconds(90061) == "1 day 1 hour 1 minute 1 second"


def test_format_duration_exact_boundaries():
    from cara.support.Time import format_duration
    assert format_duration(60) == "1m"
    assert format_duration(3600) == "1h"
    assert format_duration(86400) == "1d"
    assert format_duration(1) == "1s"
    assert format_duration(59) == "59s"


def test_format_duration_large_value():
    from cara.support.Time import format_duration
    assert format_duration(604800) == "7d"
    assert format_duration(90061) == "1d 1h 1m 1s"
```
