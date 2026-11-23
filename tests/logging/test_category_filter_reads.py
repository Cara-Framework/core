"""``CategoryFilter.should_log`` reads the categories section once, by flat key.

Products declare categories as flat keys (``"cara.http.requests": {...}``
under ``logging.categories``). The filter read
``logging.categories.<category>``, which the configuration layer splits on
dots, so a dotted category was never found: every category ran enabled at
DEBUG and every ``LOG_*`` switch was dead. A disabled category silences
chatter (below WARNING); warnings and errors always pass.
"""

from __future__ import annotations

import pytest

from cara.configuration import Configuration
from cara.logging import CategoryFilter


@pytest.fixture
def configuration():
    previous = Configuration._instance
    instance = Configuration.empty()
    instance.set(
        "logging.categories",
        {
            "database": {"enabled": False, "level": "DEBUG"},
            "cara.http.requests": {"enabled": False, "level": "INFO"},
            "cara.queue.jobs": {"enabled": True, "level": "WARNING"},
            "audit": {"enabled": True, "level": "WARNING"},
            "plain": {},
            "nulled": None,
            "odd": {"enabled": True, "level": None},
        },
    )
    yield instance
    Configuration._instance = previous


def test_one_configuration_read_per_call(configuration, monkeypatch):
    reads: list[str] = []
    get = Configuration.get

    def counting(self, path, default=None):
        reads.append(path)
        return get(self, path, default)

    monkeypatch.setattr(Configuration, "get", counting)

    assert CategoryFilter.should_log("ERROR", "cara.queue.jobs") is True

    assert reads == ["logging.categories"]


@pytest.mark.parametrize(
    ("level", "category", "expected"),
    [
        # Flat dotted key honoured: the switch is off, so info is dropped.
        ("INFO", "cara.http.requests", False),
        # Level threshold honoured on a dotted key.
        ("INFO", "cara.queue.jobs", False),
        ("WARNING", "cara.queue.jobs", True),
        ("INFO", "audit", False),
        ("WARNING", "audit", True),
        # Disabled drops chatter, never a warning or an error.
        ("DEBUG", "database", False),
        ("INFO", "database", False),
        ("WARNING", "database", True),
        ("ERROR", "database", True),
        ("CRITICAL", "cara.http.requests", True),
        ("INFO", "nulled", False),  # a category set to None reads as disabled
        ("WARNING", "nulled", True),
        ("ERROR", "nulled", True),
        # Unknown / settings-less categories keep the default: on at DEBUG.
        ("DEBUG", "plain", True),
        ("DEBUG", "unknown", True),
        ("DEBUG", "cara.unknown.thing", True),
        ("DEBUG", "odd", True),  # a null level means DEBUG
        ("INFO", None, True),
        ("INFO", "", True),
    ],
)
def test_the_answers(configuration, level, category, expected):
    assert CategoryFilter.should_log(level, category) is expected


def test_no_categories_section_allows_everything():
    previous = Configuration._instance
    Configuration.empty()
    try:
        assert CategoryFilter.should_log("DEBUG", "cara.http.requests") is True
    finally:
        Configuration._instance = previous
