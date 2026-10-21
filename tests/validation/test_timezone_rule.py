"""The ``timezone`` rule answers False for every key that is not a zone."""

import pytest

from cara.validation.rules import TimezoneRule


@pytest.mark.parametrize("value", ["UTC", "Europe/Istanbul", "America/New_York"])
def test_iana_names_pass(value):
    assert TimezoneRule().validate("timezone", value, {}) is True


@pytest.mark.parametrize(
    "value",
    [
        "Mars/Olympus",
        "UTC/",
        "/UTC",
        "a//b",
        "../x",
        " Europe/Istanbul ",
        "Europe/Istanbul\x00",
        # Longer than a filesystem name component: ZoneInfo raises OSError.
        "x" * 300,
        "",
        None,
        42,
    ],
)
def test_anything_else_fails_instead_of_raising(value):
    assert TimezoneRule().validate("timezone", value, {}) is False
