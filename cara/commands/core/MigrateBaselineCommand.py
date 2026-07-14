from __future__ import annotations

from pathlib import Path

from cara.commands import CommandBase, missing_optional
from cara.decorators import command
from cara.support import paths


@command(
    name="migrate:baseline",
    help="Adopt a verified live schema after intentionally squashing migrations.",
    options={
        "--c|connection=default": "The connection to baseline",
        "--schema=?": "The database schema to introspect",
        "--d|directory=?": "The migration directory",
        "--force": "Acknowledge replacement of migration history",
    },
)
class MigrateBaselineCommand(CommandBase):
    """Reconcile history only after live schema and preserved work are proven safe."""

    def handle(self):
        if not self.option("force"):
            self.error("migrate:baseline requires --force; it replaces migration history.")
            return 2

        try:
            from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand
            from cara.eloquent.migrations import Migration, ModelDiscoverer
            from cara.eloquent.migrations.ModelMigrationComparator import (
                migration_table_actions,
            )
            from cara.facades import DB
        except ImportError as exc:
            raise missing_optional("db", exc) from exc

        connection = self.option("connection") or "default"
        directory = self.option("directory") or paths("migrations")
        schema = self.option("schema")
        migration = Migration(
            command_class=self,
            connection=connection,
            migration_directory=directory,
            schema=schema,
        )
        files = sorted(migration.file_manager.get_migration_files())
        if not files:
            self.error("No migration files found; refusing to create an empty baseline.")
            return 1

        tracker = migration.tracker
        with tracker.migration_lock():
            tracker.ensure_migrations_table()
            ran = set(tracker.get_ran_migrations())

            # Explicit bridge migrations are the only DDL allowed before the
            # drift gate. They must be idempotent because a failed baseline is
            # safe to retry.
            bridges: set[str] = set()
            for file_path in files:
                migration_class = migration.file_manager.load_migration_class(file_path)
                name = migration.file_manager.get_migration_name_from_file(file_path)
                if not getattr(migration_class, "baseline_bridge", False):
                    continue
                bridges.add(name)
                if name in ran:
                    continue
                if migration.executor._migration_is_transactional(file_path):
                    with DB.transaction():
                        migration.executor._run_migration(file_path, "up")
                else:
                    migration.executor._run_migration(file_path, "up")

            check = SchemaCheckCommand(self.application)
            check.console = self.console
            check.set_parsed_options(
                {
                    "connection": connection,
                    "schema": schema,
                    "allow_unavailable": False,
                }
            )
            result = check.handle()
            if result not in (None, 0):
                self.error("Schema drift remains; migration history was not changed.")
                return 1

            models = ModelDiscoverer().discover_models()
            model_tables = {model["table"] for model in models if model.get("table")}
            preserved: set[str] = set()
            for file_path in files:
                content = Path(file_path).read_text(encoding="utf-8")
                model_owned = any(
                    any(migration_table_actions(content, table))
                    for table in model_tables
                )
                if not model_owned:
                    preserved.add(
                        migration.file_manager.get_migration_name_from_file(file_path)
                    )

            missing_preserved = sorted(preserved - ran - bridges)
            if missing_preserved:
                self.error(
                    "Refusing to hide pending data/framework migrations: "
                    + ", ".join(missing_preserved)
                )
                return 1

            records = [
                (
                    migration.file_manager.get_migration_name_from_file(file_path),
                    migration.file_manager.checksum(file_path),
                )
                for file_path in files
            ]
            tracker.replace_migration_history(records)

        self.success(f"Baselined {len(files)} migration(s) against the verified schema.")
        return 0
