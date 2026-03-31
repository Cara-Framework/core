"""
MakeMigrationCommand: Auto-generates migrations from models using stubs.
Orchestrates model discovery, schema comparison, and migration generation.
"""

from cara.commands import CommandBase
from cara.decorators import command
from cara.eloquent.migrations.MigrationGenerator import MigrationGenerator
from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer
from cara.eloquent.migrations.ModelMigrationComparator import ModelMigrationComparator


@command(
    name="make:migration",
    help="Auto-generate migrations from models using Laravel 11+ ordering system (no timestamps).",
    options={
        "--overwrite": "Update existing migration file instead of creating new one",
        "--style=blueprint": "Migration style: 'blueprint' (default) or 'sql'",
        "--dry_run": "Show what would be generated without creating files",
    },
)
class MakeMigrationCommand(CommandBase):
    def __init__(self, application):
        super().__init__(application)
        self.discoverer = ModelDiscoverer()
        self.comparator = ModelMigrationComparator()
        self.generator = MigrationGenerator()

    def handle(self):
        """Generate migrations from model Field.* definitions."""
        self.info("Auto-generating migrations from models...")

        # Check for overwrite mode
        overwrite_mode = self.option("overwrite", False)
        if overwrite_mode:
            return self._handle_overwrite_mode()

        # Discover models
        models = self.discoverer.discover_models()
        if not models:
            self.info("No models found in app/models directory")
            return

        # Sort models by dependency order (FK dependencies first)
        ordered_models = self.discoverer.resolve_dependency_order(models)

        created_count = 0
        updated_count = 0

        for model_info in ordered_models:
            result = self._process_model(model_info)
            if result == "created":
                created_count += 1
            elif result == "updated":
                updated_count += 1

        # Summary message
        if self.option("dry_run"):
            if created_count or updated_count:
                self.success(
                    f"Would create {created_count} new migration(s) and {updated_count} updated migration(s)"
                )
            else:
                self.success("All models have migrations")
        else:
            if created_count or updated_count:
                self.success(
                    f"Created {created_count} new migration(s) and {updated_count} updated migration(s)"
                )
            else:
                self.success("All models have migrations")

    def _handle_overwrite_mode(self):
        """Handle --overwrite mode: recreate all migrations from scratch."""
        self.info("Overwrite mode: Recreating all migrations from scratch...")

        # Discover models
        models = self.discoverer.discover_models()
        if not models:
            self.info("No models found in app/models directory")
            return

        # Sort models by dependency order (FK dependencies first)
        ordered_models = self.discoverer.resolve_dependency_order(models)

        # Clear existing migrations (handles dry_run internally)
        self._clear_existing_migrations(ordered_models)

        # Reset migration counter for fresh numbering
        self.generator.reset_counter()

        created_count = 0

        # Create fresh CREATE TABLE migrations for each model with proper ordering
        for index, model_info in enumerate(ordered_models):
            result = self._create_fresh_migration(model_info, dependency_order=index)
            if result == "created":
                created_count += 1

        # Summary message
        if self.option("dry_run"):
            self.success(
                f"Would recreate {created_count} migration(s) with dependency-based ordering"
            )
        else:
            self.success(
                f"Recreated {created_count} migration(s) with dependency-based ordering"
            )

    def _clear_existing_migrations(self, models):
        """Clear existing migration files for the given models."""
        from pathlib import Path

        from cara.support import paths

        # Use paths() helper instead of hardcoded path construction
        migrations_dir = Path(paths("migrations"))
        if not migrations_dir.exists():
            return

        for model_info in models:
            table_name = model_info["table"]
            # Find all migration files for this table
            patterns = [
                f"*create_{table_name}_table.py",
                f"*update_{table_name}_table.py",
            ]

            for pattern in patterns:
                files = list(migrations_dir.glob(pattern))
                for file_path in files:
                    try:
                        dry_run_option = self.option("dry_run")
                        if dry_run_option:
                            self.info(f"Would remove: {file_path.name}")
                        else:
                            file_path.unlink()
                            self.info(f"Removed: {file_path.name}")
                    except Exception:
                        pass  # Skip files that can't be deleted

    def _create_fresh_migration(self, model_info, dependency_order=0):
        """Create a fresh CREATE TABLE migration for a model."""
        style = self.option("style", "blueprint")

        try:
            # Generate CREATE migration content
            content = self.generator.generate_create_migration(model_info, style)
            if not content:
                return "skipped"

            if self.option("dry_run"):
                self.info(
                    f"Would create fresh migration for {model_info['name']} -> {model_info['table']} (order: {dependency_order})"
                )
                self.info("Create migration content:")
                self.info(content)
                self.info("=" * 50)
                return "created"

            # Create migration file with dependency-based timestamp
            migration_name = f"create_{model_info['table']}_table"
            filepath = self.generator.create_migration_file(
                migration_name, content, dependency_order=dependency_order
            )
            self.info(f"Created fresh migration (order {dependency_order}): \n{filepath}")
            return "created"
        except ValueError as e:
            # Handle missing fields method error
            self.error(str(e))
            return "error"

    def _process_model(self, model_info: dict) -> str:
        """Process a single model migration with database comparison."""
        table_name = model_info["table"]

        try:
            # Compare model with migration files
            diff = self.comparator.compare_model_with_migrations(model_info)

            if diff:
                # Check if this is a table creation or update
                table_exists = self.comparator.table_exists_in_migrations(table_name)

                if table_exists:
                    # Table exists, create update migration
                    self.info(f"Differences found for {model_info['name']}:")
                    for change in diff:
                        self.info(f"   • {change}")

                    if not self.option("dry_run"):
                        content = self.generator.generate_update_migration(
                            model_info, diff, self.option("style", "blueprint")
                        )
                        name = f"update_{table_name}_table"
                        filepath = self.generator.create_migration_file(name, content)
                        self.info(f"Created migration: \n{filepath}")
                    else:
                        content = self.generator.generate_update_migration(
                            model_info, diff, self.option("style", "blueprint")
                        )
                        self.info(f"Update migration content for {model_info['name']}:")
                        self.info(content)
                        self.info("=" * 50)
                    return "created"
                else:
                    # Table doesn't exist, create table migration
                    self.info(
                        f"Creating migration for {model_info['name']} -> {table_name}"
                    )
                    if not self.option("dry_run"):
                        content = self.generator.generate_create_migration(
                            model_info, self.option("style", "blueprint")
                        )
                        name = f"create_{table_name}_table"
                        filepath = self.generator.create_migration_file(name, content)
                        self.info(f"Created migration: \n{filepath}")
                    else:
                        content = self.generator.generate_create_migration(
                            model_info, self.option("style", "blueprint")
                        )
                        self.info(f"Create migration content for {model_info['name']}:")
                        self.info(content)
                        self.info("=" * 50)
                    return "created"
            else:
                self.info(f"{model_info['name']} is up to date with migrations")
                return "unchanged"
        except ValueError as e:
            # Handle missing fields method error
            self.error(str(e))
            return "error"
