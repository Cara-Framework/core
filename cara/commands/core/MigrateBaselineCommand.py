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
            self.error(
                "migrate:baseline requires --force; it replaces migration history."
            )
            return 2

        try:
            from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand
            from cara.eloquent.migrations import Migration, ModelDiscoverer
            from cara.eloquent.migrations.ModelMigrationComparator import (
                migration_table_actions,
            )
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

            # No DDL runs here. There are no bridge migrations any more —
            # every file in the directory is generated from the models, so the
            # live schema either already equals the models (and the drift gate
            # below proves it) or the baseline is refused. A database that IS
            # missing schema must be migrated or rebuilt, not baselined.
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

            # Every file is a generated creator for a model table; the drift
            # gate just proved the live schema equals those models. Verify the
            # directory shape holds (a stray file here means migrations:check
            # is red and adopting it into history would launder it), then
            # rewrite the ledger.
            models = ModelDiscoverer().discover_models()
            model_tables = {model["table"] for model in models if model.get("table")}
            strays = [
                migration.file_manager.get_migration_name_from_file(file_path)
                for file_path in files
                if not any(
                    any(migration_table_actions(Path(file_path).read_text("utf-8"), t))
                    for t in model_tables
                )
            ]
            if strays:
                self.error(
                    "Refusing to baseline non-generated migration(s): "
                    + ", ".join(sorted(strays))
                    + " — run migrations:check and regenerate first."
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
