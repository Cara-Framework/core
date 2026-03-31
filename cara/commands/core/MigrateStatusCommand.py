from cara.commands import CommandBase
from cara.decorators import command
from cara.eloquent.migrations import Migration
from cara.support import paths


@command(
    name="migrate:status",
    help="Show the status of each migration",
    options={
        "--c|connection=default": "The connection you want to check migrations on",
        "--d|directory=database/migrations": "The location of the migration directory",
        "--schema=?": "Sets the schema to be checked",
    },
)
class MigrateStatusCommand(CommandBase):
    def handle(self):
        """Show migration status with enhanced UX."""
        self.info("Checking migration status...")

        try:
            # Get migration manager
            migration_manager = self._build_migration()

            # Ensure migrations table exists
            migration_manager.create_table_if_not_exists()

            # Get all migration files and ran migrations
            all_files = migration_manager.file_manager.get_migration_files()
            ran_migrations = migration_manager.get_ran_migrations()

            if not all_files:
                self.warning("No migration files found.")
                return

            # Show migration status table
            self._show_migration_status(all_files, ran_migrations, migration_manager)

        except Exception as e:
            self.error(f"× Failed to check migration status: {str(e)}")

    def _build_migration(self) -> Migration:
        """Build migration instance with proper configuration."""
        connection = self.option("connection") or "default"
        directory = self.option("directory") or paths("migrations")
        schema = self.option("schema")

        self._show_configuration(connection, directory, schema)

        return Migration(
            command_class=self,
            connection=connection,
            migration_directory=directory,
            schema=schema,
            dry=False,
        )

    def _show_configuration(self, connection: str, directory: str, schema: str = None):
        """Display migration configuration."""
        self.info("Configuration:")
        self.info(f"   Connection: {connection}")
        self.info(f"   Directory: {directory}")
        if schema:
            self.info(f"   Schema: {schema}")

    def _show_migration_status(
        self, all_files: list, ran_migrations: list, migration_manager: Migration
    ):
        """Display migration status in a table format."""
        self.info("\nMigration Status:")
        self.info("=" * 80)

        # Table header
        self.info(f"{'Status':<10} {'Migration':<50} {'Batch':<10}")
        self.info("-" * 80)

        # Sort files by name to show chronologically
        sorted_files = sorted(all_files)

        ran_count = 0
        pending_count = 0

        for file_path in sorted_files:
            migration_name = migration_manager.file_manager.get_migration_name_from_file(
                file_path
            )

            # Check if migration has been run
            if migration_name in ran_migrations:
                status = "✓ Ran"
                batch = self._get_migration_batch(migration_name, migration_manager)
                ran_count += 1
            else:
                status = "✗ Pending"
                batch = "-"
                pending_count += 1

            # Truncate migration name if too long
            display_name = (
                migration_name[:50] if len(migration_name) > 50 else migration_name
            )

            self.info(f"{status:<10} {display_name:<50} {batch:<10}")

        # Summary
        self.info("-" * 80)
        self.info(f"Total migrations: {len(sorted_files)}")
        self.info(f"Ran: {ran_count} | Pending: {pending_count}")

        if pending_count > 0:
            self.warning(f"\n⚠ {pending_count} migration(s) need to be run.")
            self.info("Run 'python craft migrate' to execute pending migrations.")
        else:
            self.success("\n✓ All migrations are up to date!")

    def _get_migration_batch(
        self, migration_name: str, migration_manager: Migration
    ) -> str:
        """Get the batch number for a migration."""
        try:
            # Get migration details from tracker
            batch_info = migration_manager.tracker.get_migration_details(migration_name)
            if batch_info and "batch" in batch_info:
                return str(batch_info["batch"])
            return "1"  # Default batch if not found
        except:
            return "1"  # Default batch on error
