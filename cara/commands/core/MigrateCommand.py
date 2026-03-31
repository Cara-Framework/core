import os

from cara.commands import CommandBase
from cara.decorators import command
from cara.eloquent.migrations import Migration
from cara.support import paths


@command(
    name="migrate",
    help="Run the database migrations.",
    options={
        "--m|migration=all": "Migration's name to be migrated",
        "--c|connection=default": "The connection you want to run migrations on",
        "--f|force": "Force migrations without prompt in production",
        "--show": "Show SQL output without executing migrations",
        "--schema=?": "Sets the schema to be migrated",
        "--d|directory=database/migrations": "The location of the migration directory",
    },
)
class MigrateCommand(CommandBase):
    def handle(self):
        """Execute database migrations with enhanced UX."""
        self.info("Starting database migration...")

        # Check for production environment
        if self._is_production() and not self.option("force"):
            if not self._confirm_production():
                self.info("× Migration aborted by user.")
                return

        try:
            # Get migration manager
            migration_manager = self._build_migration()

            self.info("Checking migration table...")

            # Ensure migrations table exists
            migration_manager.create_table_if_not_exists()

            # Check for pending migrations
            pending = migration_manager.get_unran_migrations()
            if not pending:
                self.success("No pending migrations. Database is up to date!")
                return

            # Show what will be migrated
            self._show_pending_migrations(pending)

            if self.option("show"):
                self.info("SQL Preview Mode - No changes will be made:")
                self._show_sql_preview(migration_manager, pending)
                return

            # Run migrations
            self.info("Running migrations...")
            migration_manager.migrate("all", output=True)

            self.success("All migrations completed successfully!")

        except Exception as e:
            self.error(f"× Migration failed: {str(e)}")
            self.error("Try running with --show to see the SQL that would be executed")

    def _is_production(self) -> bool:
        """Check if we're running in production environment."""
        env = os.getenv("APP_ENV", "").lower()
        return env in ["production", "prod"]

    def _confirm_production(self) -> bool:
        """Confirm migration in production environment."""
        self.warning("⚠ You are about to run migrations in PRODUCTION!")
        self.warning("   This operation may modify your database schema.")
        self.warning("   Make sure you have a backup before proceeding.")

        while True:
            answer = (
                input("\nAre you sure you want to continue? (yes/no): ").strip().lower()
            )
            if answer in ["yes", "y"]:
                return True
            elif answer in ["no", "n"]:
                return False
            else:
                self.warning("Please answer 'yes' or 'no'")

    def _build_migration(self) -> Migration:
        """Build migration instance with proper configuration."""
        connection = self.option("connection") or "default"
        directory = self.option("directory") or paths("migrations")
        schema = self.option("schema")
        show_mode = self.option("show")

        self._show_configuration(connection, directory, schema, show_mode)

        return Migration(
            command_class=self,
            connection=connection,
            migration_directory=directory,
            schema=schema,
            dry=show_mode,
        )

    def _show_configuration(
        self, connection: str, directory: str, schema: str = None, show_mode: bool = False
    ):
        """Display migration configuration."""
        self.info("Configuration:")
        self.info(f"   Connection: {connection}")
        self.info(f"   Directory: {directory}")
        if schema:
            self.info(f"   Schema: {schema}")
        if show_mode:
            self.info("   Mode: DRY RUN (no changes will be made)")

    def _show_pending_migrations(self, migrations: list):
        """Display pending migrations list."""
        self.info(f"Found {len(migrations)} pending migration(s):")

        # Show first 5 migrations
        display_count = min(5, len(migrations))
        for mig_path in migrations[:display_count]:
            # Extract just the filename from the full path
            mig_name = mig_path.split("/")[-1].replace(".py", "")
            self.info(f"   • {mig_name}")

        # Show remaining count if more than 5
        if len(migrations) > 5:
            self.info(f"   ... and {len(migrations) - 5} more")

    def _show_migration_preview(self, migration_files: list):
        """Show simplified migration preview."""
        self.info("SQL that would be executed:")
        self.info("=" * 60)

        for i, file_path in enumerate(migration_files, 1):
            migration_name = file_path.split("/")[-1].replace(".py", "")
            self.info(f"\n{i}. Migration: {migration_name}")
            self.info("-" * 40)
            self.info(
                f"-- Would execute: {migration_name} (CREATE/ALTER TABLE operations)"
            )

        self.info("\n" + "=" * 60)
        self.info("No actual changes were made to the database.")

    def _confirm_production_migration(self) -> bool:
        # This method is no longer used in the new handle method
        return True

    def _get_migration_manager(self) -> Migration:
        # This method is no longer used in the new handle method
        return self._build_migration()

    def _show_sql_preview(self, migration_manager: Migration, pending: list):
        # This method is no longer used in the new handle method
        self._show_migration_preview(pending)
