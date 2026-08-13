from __future__ import annotations

from cara.commands.CommandBase import CommandBase
from cara.commands.OptionalDependencyError import missing_optional
from cara.decorators import command
from cara.support import paths


@command(
    name="migrate:rollback",
    help="Rollback database migrations.",
    options=[
        {
            "name": "-m|--migration",
            "help": "Migration name to rollback; 'all' means latest batch",
            "type": str,
            "default": "all",
            "is_flag": False,
        },
        {
            "name": "-c|--connection",
            "help": "The connection key from config to run migrations on",
            "type": str,
            "default": "",
            "is_flag": False,
        },
        {
            "name": "-f|--force",
            "help": "Force rollback without prompt in production",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "-s|--show",
            "help": "Shows the output of SQL for rollback operations",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--schema",
            "help": "Sets the schema to be used",
            "type": str,
            "default": None,
            "is_flag": False,
        },
        {
            "name": "-d|--directory",
            "help": "The location of the migration directory",
            "type": str,
            "default": None,
            "is_flag": False,
        },
        {
            "name": "--step",
            "help": "Number of migration batches to rollback",
            "type": int,
            "default": 1,
            "is_flag": False,
        },
    ],
)
class MigrateRollbackCommand(CommandBase):
    def handle(self):
        """
        Roll back migrations with improved safety and user experience.
        """
        global Migration
        try:
            from cara.eloquent.migrations.Migration import (
                Migration,  # local: heavy optional dep
            )
        except ImportError as exc:
            raise missing_optional("db", exc) from exc

        self.info("🔄 Starting migration rollback...")

        # Production safety check
        if (
            self._is_production()
            and not self.option("force")
            and not self._confirm_production()
        ):
            self.info("❌ Rollback aborted by user.")
            return

        # Build migration instance
        migrations_dir = self.option("directory") or paths("migrations")
        conn_name = self.option("connection") or "default"
        schema = self.option("schema")

        self.info("🔧 Configuration:")
        self.info(f"   Connection: {conn_name}")
        self.info(f"   Directory: {migrations_dir}")
        if schema:
            self.info(f"   Schema: {schema}")

        mig = Migration(
            connection=conn_name,
            command_class=self,
            migration_directory=migrations_dir,
            schema=schema,
        )

        # Ensure migration table exists
        self.info("📋 Checking migration table...")
        mig.create_table_if_not_exists()

        # Get available rollback migrations
        available = mig.get_rollback_migrations()
        if not available:
            self.info("✅ Nothing to rollback. No migrations have been run.")
            return

        # Show what will be rolled back
        migration_name = self.option("migration")
        step = int(self.option("step") or 1)

        if migration_name and migration_name != "all":
            self.info(f"📦 Rolling back specific migration: {migration_name}")
        else:
            self.info(f"📦 Rolling back last {step} batch(es) of migrations:")
            for mig_info in available[:5]:  # Show first 5
                self.info(f"   • {mig_info}")
            if len(available) > 5:
                self.info(f"   ... and {len(available) - 5} more")

        # Show SQL only mode
        if self.option("show"):
            self.info("🔍 SQL Preview Mode - No changes will be made:")
            mig.rollback(migration=migration_name, output=True)
            return

        # Perform rollback
        self.info("⚡ Performing rollback...")
        try:
            mig.rollback(migration=migration_name, output=False)
            self.info("✅ Rollback completed successfully!")
        except Exception as e:
            self.error(f"❌ Rollback failed: {str(e)}")
            self.error("💡 Try running with --show to see the SQL that would be executed")
            raise

    def _confirm_production(self) -> bool:
        """Confirm rollback in production environment."""
        self.warning("⚠️  You are about to ROLLBACK migrations in PRODUCTION!")
        self.warning("   This operation will UNDO database schema changes.")
        self.warning("   This may result in DATA LOSS if tables/columns are dropped.")
        self.warning("   Make sure you have a backup before proceeding.")
        return self._confirm_yes_no("Are you absolutely sure you want to continue?")
