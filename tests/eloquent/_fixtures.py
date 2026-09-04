"""Shared doubles for the eloquent connection tests.

Leading underscore: a test-support module, not a test file itself (mirrors
``tests/architecture/_fixtures.py`` and ``tests/docs/_fixtures.py``).
"""

from __future__ import annotations

import sys
import types


def install_fake_psycopg2(monkeypatch, connect_factory) -> types.ModuleType:
    """Insert a minimal fake ``psycopg2`` module into ``sys.modules``.

    ``PostgresConnection.create_connection`` does ``import psycopg2`` at call
    time, so the connection-lifecycle tests can exercise it without the real
    driver or a live server. Returns the fake so a test can extend it.
    """
    fake = types.ModuleType("psycopg2")
    fake.connect = connect_factory
    fake.OperationalError = type("OperationalError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "psycopg2", fake)
    return fake
