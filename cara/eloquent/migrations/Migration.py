from __future__ import annotations

from cara.exceptions import ConnectionNotRegisteredException
from cara.facades import DB, Log
from cara.support import paths

from ..models.MigrationModel import MigrationModel
from ..schema import Schema
from .MigrationExecutor import MigrationExecutor
from .MigrationFileManager import MigrationFileManager
from .MigrationTracker import MigrationTracker


class Migration:
    """
    Single Responsibility: Coordinates migration operations using composition
    Open/Closed: Extensible through component injection
    Dependency Inversion: Depends on abstractions (DB facade, components)
    """

    # PostgreSQL and SQLite DDL is transactional; atomic schema + tracker
    # writes are therefore the safe default. Operations that explicitly
    # forbid a transaction (for example CREATE INDEX CONCURRENTLY) opt out.
    transactional = True

    def __init__(
        self,
        connection=None,
        dry=False,
        command_class=None,
        migration_directory=None,
        schema=None,
    ):
        """Initialize Migration with composition pattern - follows SOLID principles"""
        self.command_class = command_class
        self.schema_name = schema
        self.dry = dry

        # Resolve connection - DB facade handles all connection logic
        if connection:
            self.connection = connection
        else:
            self.connection = DB.get_default_connection()

        if not self.connection:
            raise ConnectionNotRegisteredException("No connection specified")

        # Initialize schema - delegates to Schema class
        # Use connection name for schema initialization
        self.schema = Schema(dry=dry, schema=schema).on(self.connection)

        # Use paths() helper instead of hardcoded path
        if migration_directory is None:
            migration_directory = paths("migrations")

        # Composition: Delegate responsibilities to specialized components
        self.file_manager = MigrationFileManager(migration_directory)
        self.tracker = MigrationTracker(DB)
        self.executor = MigrationExecutor(DB, self.file_manager, self.tracker)

        # Initialize migration model for tracking
        self.migration_model = MigrationModel()

    def create_table_if_not_exists(self):
        """Create migrations table if it doesn't exist - delegates to tracker"""
        self.tracker.ensure_migrations_table()

    def get_unran_migrations(self):
        """Get unran migrations - delegates to file manager and tracker"""
        all_files = self.file_manager.get_migration_files()
        ran_migrations = self.tracker.get_ran_migrations()

        unran = []
        for file_path in sorted(all_files):
            migration_name = self.file_manager.get_migration_name_from_file(file_path)
            if migration_name not in ran_migrations:
                unran.append(file_path)

        return unran

    def get_rollback_migrations(self):
        """Get migrations for rollback - delegates to tracker"""
        last_batch = self.tracker.get_last_batch_number()
        if last_batch == 0:
            return []
        return self.tracker.get_migrations_by_batch(last_batch)

    def get_all_migrations(self, reverse=False):
        """Get all migrations - delegates to tracker"""
        migrations = self.tracker.get_ran_migrations()
        return list(reversed(migrations)) if reverse else migrations

    def get_last_batch_number(self):
        """Get last batch number - delegates to tracker"""
        return self.tracker.get_last_batch_number()

    def delete_migration(self, migration_name):
        """Delete migration record - delegates to tracker"""
        return self.tracker.remove_migration(migration_name)

    def locate(self, file_name):
        """Locate migration class - delegates to file manager"""
        return self.file_manager.load_migration_class(file_name)

    def get_ran_migrations(self):
        """Get ran migrations - delegates to tracker"""
        return self.tracker.get_ran_migrations()

    def migrate(self, migration="all", output=False):
        """Run migrations - delegates to executor"""
        if output and self.command_class:
            self.command_class.info("Running migrations...")

        if migration == "all":
            self.executor.run_pending_migrations()
        else:
            with self.tracker.migration_lock():
                self._migrate_specific_locked(migration)

        if output and self.command_class:
            self.command_class.info("Migrations completed.")

    def rollback(self, migration="all", output=False):
        """Rollback migrations - delegates to executor"""
        if output and self.command_class:
            self.command_class.info("Rolling back migrations...")

        if migration == "all":
            self.executor.rollback_last_batch()
        else:
            with self.tracker.migration_lock():
                self._rollback_specific_locked(migration)

        if output and self.command_class:
            self.command_class.info("Rollback completed.")

    def reset(self, migration="all"):
        """Reset all migrations"""
        if migration == "all":
            for migration_name in self.get_all_migrations(reverse=True):
                self.rollback(migration_name)
        else:
            self.rollback(migration)

    def _migrate_specific_locked(self, migration: str) -> None:
        self.tracker.ensure_migrations_table()
        files = sorted(self.file_manager.get_migration_files())
        self.executor._validate_applied_checksums(files)
        if migration in set(self.tracker.get_ran_migrations()):
            Log.info("Migration %s already ran; skipping.", migration)
            return
        file_map = {
            self.file_manager.get_migration_name_from_file(path): path for path in files
        }
        file_path = file_map.get(migration)
        if file_path is None:
            raise FileNotFoundError(f"Migration not found: {migration}")
        batch = self.tracker.get_last_batch_number() + 1
        Log.info("Running migration: %s", migration)
        checksum = self.file_manager.checksum(file_path)
        if self.executor._migration_is_transactional(file_path):
            with DB.transaction():
                self.executor._run_migration(file_path, "up")
                self.tracker.record_migration(migration, batch, checksum)
        else:
            self.executor._run_migration(file_path, "up")
            self.tracker.record_migration(migration, batch, checksum)
        Log.info("Migrated: %s", migration)

    def _rollback_specific_locked(self, migration: str) -> None:
        self.tracker.ensure_migrations_table()
        files = sorted(self.file_manager.get_migration_files())
        self.executor._validate_applied_checksums(files)
        if migration not in set(self.tracker.get_ran_migrations()):
            Log.info(
                "Migration %s was not in the tracker; nothing to rollback.", migration
            )
            return
        file_map = {
            self.file_manager.get_migration_name_from_file(path): path for path in files
        }
        file_path = file_map.get(migration)
        if file_path is None:
            raise FileNotFoundError(f"Migration not found: {migration}")
        Log.info("Rolling back: %s", migration)
        if self.executor._migration_is_transactional(file_path):
            with DB.transaction():
                self.executor._run_migration(file_path, "down")
                self.tracker.remove_migration(migration)
        else:
            self.executor._run_migration(file_path, "down")
            self.tracker.remove_migration(migration)
        Log.info("Rolled back: %s", migration)

    def refresh(self, migration="all"):
        """Refresh migrations (reset + migrate)"""
        self.reset(migration)
        self.migrate(migration)

    def drop_all_tables(self, ignore_fk=False):
        """Drop all tables.

        We don't borrow our own connection here — every operation we
        invoke (``disable_foreign_key_constraints``, ``get_all_tables``,
        ``drop_table_if_exists``) goes through ``self.schema``, which
        borrows and releases its own connection per statement. The
        previous implementation grabbed a slot it never used and never
        returned, draining the pool on multi-table resets.
        """
        if ignore_fk:
            self.schema.disable_foreign_key_constraints()

        tables = self.schema.get_all_tables()

        for table_name in tables:
            self.schema.drop_table_if_exists(table_name)

        if ignore_fk:
            self.schema.enable_foreign_key_constraints()

    def fresh(self, ignore_fk=False, migration="all"):
        """Fresh migration (drop all + migrate)"""
        self.drop_all_tables(ignore_fk)
        self.migrate(migration)

    def delete_migrations(self, migrations=None):
        """Delete migration records - delegates to tracker"""
        if migrations:
            for migration in migrations:
                self.tracker.remove_migration(migration)

    def delete_last_batch(self):
        """Delete last batch - delegates to tracker"""
        last_batch = self.tracker.get_last_batch_number()
        if last_batch > 0:
            migrations = self.tracker.get_migrations_by_batch(last_batch)
            for migration in migrations:
                self.tracker.remove_migration(migration)
