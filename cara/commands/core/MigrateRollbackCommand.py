from __future__ import annotations

from cara.commands.CommandBase import CommandBase
from cara.commands.OptionalDependencyError import missing_optional
from cara.decorators import command
from cara.support import paths

from .MigrateCommand import MigrateCommand


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
    # The dry-run SQL collector is ``migrate --show``'s: one owner for "build
    # the migration against a dry Schema, guard the DB facade, record every
    # statement", so the two previews cannot drift apart.
    _collect_migration_sql = MigrateCommand._collect_migration_sql
    _install_db_facade_guard = MigrateCommand._install_db_facade_guard
    _install_sql_recorder = staticmethod(MigrateCommand._install_sql_recorder)

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
        specific = bool(migration_name and migration_name != "all")
        step = int(self.option("step") or 1)
        if step < 1:
            self.error("❌ --step must be a positive number of batches.")
            return 1

        if specific:
            targets = [migration_name]
            self.info(f"📦 Rolling back specific migration: {migration_name}")
        else:
            targets = available if step == 1 else self._batch_targets(mig, step)
            self.info(f"📦 Rolling back last {step} batch(es) of migrations:")
            for mig_info in targets[:5]:  # Show first 5
                self.info(f"   • {mig_info}")
            if len(targets) > 5:
                self.info(f"   ... and {len(targets) - 5} more")

        # Show SQL only mode. This used to call ``mig.rollback(...)`` — the
        # REAL rollback — right under "No changes will be made": the executor
        # builds each migration with a live Schema, so ``--show`` ran every
        # ``down()`` and deleted the ledger rows. The preview now compiles
        # ``down()`` against a dry Schema and never reaches the executor.
        if self.option("show"):
            self.info("🔍 SQL Preview Mode - No changes will be made:")
            self._show_sql_preview(
                mig,
                targets,
                connection=conn_name,
                directory=migrations_dir,
                schema=schema,
            )
            return

        # Perform rollback
        self.info("⚡ Performing rollback...")
        try:
            if specific:
                mig.rollback(migration=migration_name, output=False)
            else:
                # ``rollback("all")`` undoes exactly the newest batch, so
                # ``--step N`` is N of them. ``--step`` used to be parsed,
                # echoed in the banner above and then dropped: every value
                # rolled back one batch.
                for _ in range(step):
                    if not mig.get_rollback_migrations():
                        break
                    mig.rollback(migration="all", output=False)
            self.info("✅ Rollback completed successfully!")
        except Exception as e:
            self.error(f"❌ Rollback failed: {str(e)}")
            self.error("💡 Try running with --show to see the SQL that would be executed")
            raise

    @staticmethod
    def _batch_targets(mig: Migration, step: int) -> list[str]:
        """Migrations held by the newest ``step`` batches, in rollback order.

        Batch numbers can have gaps (a rolled-back batch leaves one), so this
        walks down from the newest and counts only batches that hold rows.
        """
        targets: list[str] = []
        batch = mig.get_last_batch_number()
        taken = 0
        while batch > 0 and taken < step:
            names = mig.tracker.get_migrations_by_batch(batch)
            if names:
                targets.extend(names)
                taken += 1
            batch -= 1
        return targets

    def _show_sql_preview(
        self,
        mig: Migration,
        targets: list[str],
        connection: str,
        directory: str,
        schema: str | None,
    ) -> None:
        """Print the SQL each target's ``down()`` WOULD run; execute nothing."""
        file_map = {
            mig.file_manager.get_migration_name_from_file(path): path
            for path in mig.file_manager.get_migration_files()
        }

        self.info("SQL that would be executed:")
        self.info("=" * 60)

        for index, name in enumerate(targets, 1):
            self.info(f"\n{index}. Migration: {name}")
            self.info("-" * 40)

            file_path = file_map.get(name)
            if file_path is None:
                self.warning("   Migration file is missing from disk.")
                continue

            statements = self._collect_migration_sql(
                mig,
                file_path,
                connection=connection,
                directory=directory,
                schema=schema,
                direction="down",
            )
            if statements:
                for sql in statements:
                    self.line(f"{sql.rstrip(';')};")
            else:
                self.info("-- (no SQL statements compiled for this migration)")

        self.info("\n" + "=" * 60)
        self.info("No actual changes were made to the database.")

    def _confirm_production(self) -> bool:
        """Confirm rollback in production environment."""
        self.warning("⚠️  You are about to ROLLBACK migrations in PRODUCTION!")
        self.warning("   This operation will UNDO database schema changes.")
        self.warning("   This may result in DATA LOSS if tables/columns are dropped.")
        self.warning("   Make sure you have a backup before proceeding.")
        return self._confirm_yes_no("Are you absolutely sure you want to continue?")
