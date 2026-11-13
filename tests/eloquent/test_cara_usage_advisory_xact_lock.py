"""``DB.advisory_xact_lock`` — the one way to take named transaction locks.

Before it, a dozen repositories hand-rolled
``SELECT pg_advisory_xact_lock(hashtext…(%s …))`` with two different hash
functions, and the per-key lockers paid one round trip per key. The helper
takes every key in ONE statement, in the database's text order.

The integration half needs an (empty is fine) scratch database::

    DB_DATABASE=synk_scratch_<label> CARA_RUN_INTEGRATION=1 pytest <this file>
"""

from __future__ import annotations

import os

import pytest

from cara.eloquent.DatabaseManager import DatabaseManager


class _Recording(DatabaseManager):
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def select(self, query, bindings=(), connection=None):
        self.calls.append((query, tuple(bindings)))
        return []


def test_no_keys_issue_no_statement():
    db = _Recording()
    db.advisory_xact_lock()
    assert db.calls == []


def test_one_key_is_one_64_bit_lock():
    db = _Recording()
    db.advisory_xact_lock("ai_credits:7")
    assert len(db.calls) == 1
    query, bindings = db.calls[0]
    assert "hashtextextended(%s, 0)" in query
    assert bindings == ("ai_credits:7",)


def test_many_keys_cost_one_round_trip_in_database_order():
    db = _Recording()
    keys = [f"trial-user-consumption:{n}" for n in range(50)]
    db.advisory_xact_lock(*keys, *keys[:5])
    assert len(db.calls) == 1, "N keys must not cost N round trips"
    query, bindings = db.calls[0]
    assert "DISTINCT" in query and "ORDER BY requested.key" in query
    assert bindings == (keys + keys[:5],)


_integration = pytest.mark.skipif(
    os.getenv("CARA_RUN_INTEGRATION") != "1"
    or not os.getenv("DB_DATABASE", "").startswith("synk_scratch_"),
    reason="set CARA_RUN_INTEGRATION=1 and DB_DATABASE named synk_scratch_*",
)


def _manager() -> DatabaseManager:
    return DatabaseManager(
        "default",
        {
            "default": {
                "driver": "postgres",
                "host": os.getenv("DB_HOST", "localhost"),
                "database": os.environ["DB_DATABASE"],
                "user": os.getenv("DB_USERNAME") or os.getenv("USER"),
                "password": os.getenv("DB_PASSWORD", ""),
                "port": int(os.getenv("DB_PORT", "5432")),
                "prefix": "",
            }
        },
    )


def _held(probe, keys: list[str]) -> int:
    """How many of ``keys`` another session holds right now."""
    with probe.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM pg_locks AS held "
            "JOIN unnest(%s::text[]) AS key "
            "ON held.locktype = 'advisory' AND held.pid <> pg_backend_pid() "
            "AND ((held.classid::bigint << 32) | held.objid::bigint) "
            "= hashtextextended(key, 0)",
            [keys],
        )
        return int(cursor.fetchone()[0])


@_integration
def test_every_key_is_held_until_the_transaction_ends():
    import psycopg2

    manager = _manager()
    probe = psycopg2.connect(dbname=os.environ["DB_DATABASE"])
    probe.autocommit = True
    keys = [f"cara-usage-lock:{n}" for n in range(25)]
    try:
        with manager.transaction():
            manager.advisory_xact_lock(*keys, keys[0])
            assert _held(probe, keys) == len(keys)
        assert _held(probe, keys) == 0
    finally:
        probe.close()
