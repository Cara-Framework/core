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
    # 2 * 86400 = 172800
    assert humanize_seconds(172800) == "2 days"
    assert humanize_seconds(45) == "45 seconds"


def test_humanize_seconds_mixed_singular_plural():
    # 3600 + 60 + 1 = 3661
    assert humanize_seconds(3661) == "1 hour 1 minute 1 second"
    # 2*3600 + 60 + 1 = 7261
    assert humanize_seconds(7261) == "2 hours 1 minute 1 second"
    # 3600 + 2*60 + 3 = 3723
    assert humanize_seconds(3723) == "1 hour 2 minutes 3 seconds"


def test_humanize_seconds_skips_zero_components():
    # 86400 + 60 = 86460 (skips hours and seconds)
    assert humanize_seconds(86460) == "1 day 1 minute"
    # 3600 + 30 = 3630 (skips minutes)
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
    # 2 * 86400 = 172800
    assert format_duration(172800) == "2d"


def test_format_duration_skips_zero_components():
    # 86400 + 60 = 86460 (skips h and s)
    assert format_duration(86460) == "1d 1m"
    # 3600 + 30 = 3630 (skips m)
    assert format_duration(3630) == "1h 30s"


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
