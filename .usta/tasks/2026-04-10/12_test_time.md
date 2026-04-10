## Goal
Create `tests/support/test_time.py` covering `humanize_seconds` and `format_duration` with edge cases.

## Steps
1. Create `tests/support/test_time.py` with the following content.
2. Import `humanize_seconds` and `format_duration` from `cara.support.Time`.
3. Write test functions as specified below. Each assertion is given as exact Python.

```python
import pytest
from cara.support.Time import humanize_seconds, format_duration


def test_humanize_seconds_zero():
    assert humanize_seconds(0) == "0 seconds"


def test_humanize_seconds_singular_units():
    assert humanize_seconds(1) == "1 second"
    assert humanize_seconds(60) == "1 minute"
    assert humanize_seconds(3600) == "1 hour"
    assert humanize_seconds(86400) == "1 day"


def test_humanize_seconds_all_singular():
    # 86400 + 3600 + 60 + 1 = 90061
    assert humanize_seconds(90061) == "1 day 1 hour 1 minute 1 second"


def test_humanize_seconds_plural():
    assert humanize_seconds(172800) == "2 days"
    assert humanize_seconds(45) == "45 seconds"


def test_humanize_seconds_mixed():
    # 3600 + 60 + 1 = 3661
    assert humanize_seconds(3661) == "1 hour 1 minute 1 second"
    # 7200 + 60 + 1 = 7261
    assert humanize_seconds(7261) == "2 hours 1 minute 1 second"
    # 3600 + 120 + 3 = 3723
    assert humanize_seconds(3723) == "1 hour 2 minutes 3 seconds"


def test_humanize_seconds_omits_zero_components():
    # 86400 + 60 = 86460 -> skips hours and seconds
    assert humanize_seconds(86460) == "1 day 1 minute"
    # 3600 + 30 = 3630 -> skips minutes
    assert humanize_seconds(3630) == "1 hour 30 seconds"


def test_format_duration_zero():
    assert format_duration(0) == "0s"


def test_format_duration_single_unit():
    assert format_duration(1) == "1s"
    assert format_duration(45) == "45s"
    assert format_duration(60) == "1m"
    assert format_duration(3600) == "1h"
    assert format_duration(86400) == "1d"


def test_format_duration_combined():
    assert format_duration(61) == "1m 1s"
    # 3600 + 60 + 1 = 3661
    assert format_duration(3661) == "1h 1m 1s"
    # 86400 + 3600 + 60 + 1 = 90061
    assert format_duration(90061) == "1d 1h 1m 1s"
    assert format_duration(172800) == "2d"


def test_format_duration_omits_zero_components():
    # 86400 + 60 = 86460 -> skips h and s
    assert format_duration(86460) == "1d 1m"
    # 3600 + 30 = 3630 -> skips m
    assert format_duration(3630) == "1h 30s"
```

## Files
- `tests/support/test_time.py` (new)

## Reference Files
- `cara/support/Time.py`