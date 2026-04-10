## Add test coverage for parse_human_time

`cara/support/Time.py:parse_human_time` handles "now", "expired", and various duration strings but has zero test coverage. Add tests in a new file.

### tests/support/test_parse_human_time.py

```python
import pendulum
from cara.support.Time import parse_human_time


def test_parse_now():
    before = pendulum.now("GMT")
    result = parse_human_time("now")
    after = pendulum.now("GMT")
    assert before <= result <= after


def test_parse_expired():
    result = parse_human_time("expired")
    assert result.year == pendulum.now("GMT").year - 20


def test_parse_seconds():
    before = pendulum.now("GMT")
    result = parse_human_time("30 seconds")
    expected = before.add(seconds=30)
    # allow 1 second tolerance
    assert abs((result - expected).total_seconds()) < 1


def test_parse_minutes():
    before = pendulum.now("GMT")
    result = parse_human_time("5 minutes")
    expected = before.add(minutes=5)
    assert abs((result - expected).total_seconds()) < 1


def test_parse_hours():
    before = pendulum.now("GMT")
    result = parse_human_time("2 hours")
    expected = before.add(hours=2)
    assert abs((result - expected).total_seconds()) < 1


def test_parse_days():
    before = pendulum.now("GMT")
    result = parse_human_time("3 days")
    expected = before.add(days=3)
    assert abs((result - expected).total_seconds()) < 1


def test_parse_weeks():
    before = pendulum.now("GMT")
    result = parse_human_time("1 week")
    expected = before.add(weeks=1)
    assert abs((result - expected).total_seconds()) < 1


def test_parse_months():
    before = pendulum.now("GMT")
    result = parse_human_time("2 months")
    expected = before.add(months=2)
    # months can shift days, allow 2 second tolerance
    assert abs((result - expected).total_seconds()) < 2


def test_parse_years():
    before = pendulum.now("GMT")
    result = parse_human_time("1 year")
    expected = before.add(years=1)
    assert abs((result - expected).total_seconds()) < 2


def test_parse_singular_forms():
    # singular "second", "minute", "hour", "day" all work
    assert parse_human_time("1 second") is not None
    assert parse_human_time("1 minute") is not None
    assert parse_human_time("1 hour") is not None
    assert parse_human_time("1 day") is not None


def test_parse_unknown_unit_returns_none():
    result = parse_human_time("5 fortnights")
    assert result is None
```
