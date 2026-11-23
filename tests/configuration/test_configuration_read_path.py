"""``Configuration.get`` walks plain string paths itself, and stays live.

Every read went through Dotty's path parser (~1 us: escape stamps, a fresh
closure and a ``pop(0)`` per level). The common read is now a dict walk
(0.94 -> 0.28 us for ``app.timezone``); nothing is memoised, so in-place
mutation of the store or of a section is visible on the next read.
"""

from __future__ import annotations

import importlib

import pytest

from cara.configuration import Configuration, config

configuration_module = importlib.import_module("cara.configuration.Configuration")


@pytest.fixture
def configuration():
    previous = Configuration._instance
    instance = Configuration.empty()
    instance.set(
        "app",
        {
            "timezone": "UTC",
            "debug": False,
            "nothing": None,
            "hosts": ["a.example", "b.example"],
            "limits": {"burst": 0},
            7: "an integer key",
        },
    )
    yield instance
    Configuration._instance = previous


@pytest.fixture
def dotty_walks(monkeypatch):
    """How many reads fell back to Dotty's path walk."""
    walks: list[str] = []
    walk = configuration_module._READ_PATH

    def counting(store, path):
        walks.append(path)
        return walk(store, path)

    monkeypatch.setattr(configuration_module, "_READ_PATH", counting)
    return walks


def test_a_plain_read_does_not_enter_dotty(configuration, dotty_walks):
    assert config("app.timezone") == "UTC"
    assert config("app.debug", True) is False
    assert config("app.limits.burst", 5) == 0
    assert config("app.limits") == {"burst": 0}

    assert dotty_walks == []


def test_none_is_answered_as_dotty_answers_it(configuration, dotty_walks):
    assert config("app.nothing", "default") is None
    # Dotty stops at a None parent and answers it, even with path left.
    assert config("app.nothing.deeper", "default") is None

    assert dotty_walks == []


def test_everything_else_is_still_dottys_answer(configuration, dotty_walks):
    assert config("app.missing", "default") == "default"
    assert config("missing.entirely") is None
    assert config("app.hosts.1") == "b.example"
    assert config("app.7") == "an integer key"
    assert config("app.timezone.zone", "default") == "default"

    assert len(dotty_walks) == 5


def test_an_escaped_dot_names_one_key(configuration):
    configuration.set("flat", {"a.b": "one key"})

    assert config("flat.a\\.b") == "one key"
    assert config("flat.a.b", "default") == "default"


def test_in_place_mutation_is_read_live(configuration, monkeypatch):
    assert config("app.timezone") == "UTC"

    # The three ways tests and providers change configuration:
    configuration.set("app.timezone", "Europe/Istanbul")
    assert config("app.timezone") == "Europe/Istanbul"

    monkeypatch.setitem(Configuration._instance._config, "app.timezone", "Asia/Tokyo")
    assert config("app.timezone") == "Asia/Tokyo"

    config("app")["timezone"] = "America/Lima"  # a section mutated in place
    assert config("app.timezone") == "America/Lima"

    del config("app")["timezone"]
    assert config("app.timezone", "gone") == "gone"
