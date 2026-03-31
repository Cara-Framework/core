import os

from cara.commands import CommandBase
from cara.decorators import command
from cara.eloquent.migrations.Migration import Migration
from cara.support import paths


@command(
    name="migrate:rollback",
    help="Rollback database migrations.",
    options={
        "--m|migration=all": "Migration name to rollback; 'all' means latest batch",
        "--c|connection=": "The connection key from config to run migrations on",
        "--f|force": "Force rollback without prompt in production",
        "--s|show": "Shows the output of SQL for rollback operations",
        "--schema=?": "Sets the schema to be used",
        "--d|directory=database/migrations": "The location of the migration directory",
        "--step=1": "Number of migration batches to rollback",
    },
)
class MigrateRollbackCommand(CommandBase):
    def handle(self):
        """
        Roll back migrations with improved safety and user experience.
        """
        self.info("🔄 Starting migration rollback...")

        # Production safety check
        if self._is_production() and not self.option("force"):
            if not self._confirm_production():
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

    def _is_production(self) -> bool:
        """Check if we're running in production environment."""
        env = os.getenv("APP_ENV", "").lower()
        return env in ["production", "prod"]

    def _confirm_production(self) -> bool:
        """Confirm rollback in production environment."""
        self.warning("⚠️  You are about to ROLLBACK migrations in PRODUCTION!")
        self.warning("   This operation will UNDO database schema changes.")
        self.warning("   This may result in DATA LOSS if tables/columns are dropped.")
        self.warning("   Make sure you have a backup before proceeding.")

        while True:
            answer = (
                input("\n🤔 Are you absolutely sure you want to continue? (yes/no): ")
                .strip()
                .lower()
            )
            if answer in ["yes", "y"]:
                return True
            elif answer in ["no", "n"]:
                return False
            else:
                self.warning("Please answer 'yes' or 'no'")
