from __future__ import annotations

import hmac

from cara.exceptions import InvalidArgumentException, ORMException
from cara.facades import Log


class MigrationExecutor:
    """Single Responsibility: Executes migration operations"""

    def __init__(self, db_manager, file_manager, tracker):
        self.db_manager = db_manager
        self.file_manager = file_manager
        self.tracker = tracker

    def run_pending_migrations(self):
        """Run all pending migrations"""
        with self.tracker.migration_lock():
            return self._run_pending_migrations_locked()

    def _run_pending_migrations_locked(self):
        self.tracker.ensure_migrations_table()

        # Get all migration files
        migration_files = self.file_manager.get_migration_files()
        migration_files.sort()  # Ensure proper order
        self._validate_applied_checksums(migration_files)

        # Get already run migrations
        ran_migrations = self.tracker.get_ran_migrations()

        # Find pending migrations
        pending_migrations = []
        for file_path in migration_files:
            migration_name = self.file_manager.get_migration_name_from_file(file_path)
            if migration_name not in ran_migrations:
                pending_migrations.append((migration_name, file_path))

        if not pending_migrations:
            Log.info("No pending migrations.")
            return

        # Get next batch number
        batch = self.tracker.get_last_batch_number() + 1

        # Run pending migrations. Each migration runs inside its own
        # transaction so a half-applied DDL (constraint fails after
        # column added) rolls back cleanly — Postgres supports
        # transactional DDL. The migration is also bound to its
        # ``record_migration`` write so the tracker only marks the
        # migration ran when the DDL actually committed; without this
        # tying, a crash between ``up()`` and ``record_migration``
        # leaves the schema half-changed AND unrecorded, so the next
        # run re-applies and crashes again.
        #
        # Migrations that need to run OUTSIDE a transaction (e.g.
        # ``CREATE INDEX CONCURRENTLY`` on Postgres) opt out by setting
        # the class attribute ``transactional = False``.
        for migration_name, file_path in pending_migrations:
            Log.info("Running migration: %s", migration_name)
            transactional = self._migration_is_transactional(file_path)
            if transactional:
                with self.db_manager.transaction():
                    self._run_migration(file_path, "up")
                    self.tracker.record_migration(
                        migration_name,
                        batch,
                        self.file_manager.checksum(file_path),
                    )
            else:
                self._run_migration(file_path, "up")
                self.tracker.record_migration(
                    migration_name,
                    batch,
                    self.file_manager.checksum(file_path),
                )
            Log.info("Migrated: %s", migration_name)

    def rollback_last_batch(self):
        """Rollback the last batch of migrations"""
        with self.tracker.migration_lock():
            return self._rollback_last_batch_locked()

    def _rollback_last_batch_locked(self):
        self.tracker.ensure_migrations_table()
        migration_files = self.file_manager.get_migration_files()
        migration_files.sort()
        self._validate_applied_checksums(migration_files)
        last_batch = self.tracker.get_last_batch_number()
        if last_batch == 0:
            Log.info("Nothing to rollback.")
            return

        # Get migrations from last batch
        migrations = self.tracker.get_migrations_by_batch(last_batch)

        if not migrations:
            Log.info("No migrations to rollback.")
            return

        # Rollback migrations in reverse order
        file_map = {}
        for file_path in migration_files:
            name = self.file_manager.get_migration_name_from_file(file_path)
            file_map[name] = file_path

        for migration_name in migrations:
            if migration_name in file_map:
                Log.info("Rolling back: %s", migration_name)
                transactional = self._migration_is_transactional(file_map[migration_name])
                if transactional:
                    with self.db_manager.transaction():
                        self._run_migration(file_map[migration_name], "down")
                        self.tracker.remove_migration(migration_name)
                else:
                    self._run_migration(file_map[migration_name], "down")
                    self.tracker.remove_migration(migration_name)
                Log.info("Rolled back: %s", migration_name)

    def _run_migration(self, file_path, direction):
        """Run a single migration in specified direction"""
        try:
            migration_class = self.file_manager.load_migration_class(file_path)
            migration_instance = migration_class()

            if direction == "up":
                migration_instance.up()
            elif direction == "down":
                migration_instance.down()
            else:
                raise InvalidArgumentException(
                    f"Invalid migration direction: {direction}"
                )

        except Exception as e:
            Log.error(
                "Error running migration %s: %s",
                file_path,
                e,
                exc_info=True,
            )
            raise

    def _migration_is_transactional(self, file_path) -> bool:
        """Probe a migration for an opt-out flag.

        ``CREATE INDEX CONCURRENTLY`` and a handful of other Postgres
        operations cannot run inside a transaction. Migrations declare
        ``transactional = False`` (class-level attribute) to opt out;
        everything else runs wrapped.
        """
        try:
            migration_class = self.file_manager.load_migration_class(file_path)
            return bool(getattr(migration_class, "transactional", True))
        except Exception:
            # If we can't load the class, the per-migration runner
            # below will raise the real error — assume transactional
            # so we don't accidentally lose tx safety on a migration
            # we *could* have run safely wrapped.
            return True

    def get_migration_status(self):
        """Get status of all migrations"""
        self.tracker.ensure_migrations_table()

        migration_files = self.file_manager.get_migration_files()
        migration_files.sort()
        self._validate_applied_checksums(migration_files)

        ran_migrations = self.tracker.get_ran_migrations()

        status = []
        for file_path in migration_files:
            migration_name = self.file_manager.get_migration_name_from_file(file_path)
            is_ran = migration_name in ran_migrations
            status.append(
                {"name": migration_name, "status": "Ran" if is_ran else "Pending"}
            )

        return status

    def _validate_applied_checksums(self, migration_files) -> None:
        """Refuse altered/deleted applied migrations; adopt legacy NULL hashes once."""
        records = self.tracker.get_ran_migration_records()
        # Lightweight test doubles and third-party trackers written before the
        # checksum API may return a mock/non-sequence. Real Cara trackers
        # always return a list of dicts.
        if not isinstance(records, (list, tuple)):
            return

        file_map = {
            self.file_manager.get_migration_name_from_file(path): path
            for path in migration_files
        }
        for record in records:
            if not isinstance(record, dict):
                continue
            name = record.get("migration")
            if not name:
                continue
            file_path = file_map.get(name)
            if file_path is None:
                raise ORMException(
                    f"Applied migration '{name}' is missing from disk. Restore the "
                    "file or rebuild the development database after an overwrite."
                )
            actual = self.file_manager.checksum(file_path)
            expected = record.get("checksum")
            if not expected:
                self.tracker.set_migration_checksum(name, actual)
                continue
            if not hmac.compare_digest(str(expected), actual):
                raise ORMException(
                    f"Applied migration '{name}' was modified after execution. "
                    "Create a new migration; for a model-first development "
                    "overwrite, rebuild the database before migrating."
                )
