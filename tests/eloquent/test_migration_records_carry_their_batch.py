"""``get_ran_migration_records`` carries each migration's batch (ORM audit).

``migrate:status`` needs the batch of every applied migration; without it in
the records it had to ask once per batch. The batch is now part of the same
single query, added beside ``migration`` / ``checksum`` (backward compatible).
"""

from __future__ import annotations

from cara.eloquent import DatabaseManager
from cara.eloquent.migrations import MigrationTracker
from cara.testing.FacadeSwap import swap


def test_records_carry_their_batch_in_one_query(tmp_path, monkeypatch):
    path = tmp_path / "migrations.sqlite"
    dm = DatabaseManager("app", {"app": {"driver": "sqlite", "database": str(path)}})
    with swap("DB", dm):
        tracker = MigrationTracker(dm)
        tracker.ensure_migrations_table()
        tracker.record_migration("2026_01_01_a", 1, "a" * 64)
        tracker.record_migration("2026_01_02_b", 1, "b" * 64)
        tracker.record_migration("2026_01_03_c", 2, "c" * 64)

        queries: list[str] = []
        original = dm.create_connection_instance

        def counting(*args, **kwargs):
            connection = original(*args, **kwargs)
            query = connection.query

            def recorded(sql, *a, **k):
                queries.append(sql)
                return query(sql, *a, **k)

            connection.query = recorded
            return connection

        monkeypatch.setattr(dm, "create_connection_instance", counting)
        records = tracker.get_ran_migration_records()

    assert records == [
        {"migration": "2026_01_01_a", "checksum": "a" * 64, "batch": 1},
        {"migration": "2026_01_02_b", "checksum": "b" * 64, "batch": 1},
        {"migration": "2026_01_03_c", "checksum": "c" * 64, "batch": 2},
    ]
    assert all(isinstance(record["batch"], int) for record in records)
    assert len(queries) == 1, queries
