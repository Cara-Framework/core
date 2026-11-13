"""Shared fixtures for the command tests."""

from __future__ import annotations

import pytest

from cara.configuration import Configuration


@pytest.fixture(autouse=True)
def _isolated_configuration(monkeypatch):
    """Install each test's own empty configuration authority.

    Commands read config (``database.*``, ``queue.*``) and in production
    only ever run inside a booted app, where ``ConfigurationProvider`` has
    registered one. These tests used to borrow whichever authority an EARLIER
    test happened to install, so running a module alone (or ``-k`` filtered)
    raised "Configuration is unavailable before ConfigurationProvider
    registration". ``monkeypatch`` restores the previous authority afterwards,
    so nothing leaks either way.
    """
    monkeypatch.setattr(Configuration, "_instance", None)
    return Configuration.empty()
