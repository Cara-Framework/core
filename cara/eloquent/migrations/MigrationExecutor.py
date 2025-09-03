from cara.facades import Log


class MigrationExecutor:
    """Single Responsibility: Executes migration operations"""

    def __init__(self, db_manager, file_manager, tracker):
        self.db_manager = db_manager
        self.file_manager = file_manager
        self.tracker = tracker

    def run_pending_migrations(self):
        """Run all pending migrations"""
        self.tracker.ensure_migrations_table()

        # Get all migration files
        migration_files = self.file_manager.get_migration_files()
        migration_files.sort()  # Ensure proper order

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

        # Run pending migrations
        for migration_name, file_path in pending_migrations:
            Log.info(f"Running migration: {migration_name}")
            self._run_migration(file_path, "up")
            self.tracker.record_migration(migration_name, batch)
            Log.info(f"Migrated: {migration_name}")

    def rollback_last_batch(self):
        """Rollback the last batch of migrations"""
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
        migration_files = self.file_manager.get_migration_files()
        file_map = {}
        for file_path in migration_files:
            name = self.file_manager.get_migration_name_from_file(file_path)
            file_map[name] = file_path

        for migration_name in migrations:
            if migration_name in file_map:
                Log.info(f"Rolling back: {migration_name}")
                self._run_migration(file_map[migration_name], "down")
                self.tracker.remove_migration(migration_name)
                Log.info(f"Rolled back: {migration_name}")

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
                raise ValueError(f"Invalid migration direction: {direction}")

        except Exception as e:
            Log.debug(f"Error running migration {file_path}: {str(e)}")
            import traceback

            Log.debug(f"Full traceback: {traceback.format_exc()}")
            raise

    def get_migration_status(self):
        """Get status of all migrations"""
        self.tracker.ensure_migrations_table()

        migration_files = self.file_manager.get_migration_files()
        migration_files.sort()

        ran_migrations = self.tracker.get_ran_migrations()

        status = []
        for file_path in migration_files:
            migration_name = self.file_manager.get_migration_name_from_file(file_path)
            is_ran = migration_name in ran_migrations
            status.append(
                {"name": migration_name, "status": "Ran" if is_ran else "Pending"}
            )

        return status
