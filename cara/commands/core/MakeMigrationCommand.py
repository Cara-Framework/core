"""
MakeMigrationCommand: Auto-generates migrations from models using stubs.
Orchestrates model discovery, schema comparison, and migration generation.

``--overwrite`` enforces ONE FILE PER TABLE: after regenerating, the migrations
directory contains exactly the model-generated set and nothing else. It used to
delete only the files it recognised as its own, so hand-written ``add_*`` /
``backfill_*`` / ``fix_*`` migrations accumulated forever (synk: 123 generated +
40 hand-written) and the directory stopped being a function of the models.

The escape hatch is the module-level ``MODEL_LESS = True`` marker: a migration
that creates a schema object NO model can own (a materialized view, an extension,
a data backfill) declares it and ``--overwrite`` preserves the file. Unmarked
files are deleted. Both lists are printed — the sweep is never silent.
"""

from __future__ import annotations

import ast
import contextlib
import os
import shutil
import tempfile
from pathlib import Path

from cara.commands import CommandBase, missing_optional
from cara.decorators import command

# Module-level marker that exempts a migration from the ``--overwrite`` purge.
# Constraint: it must be a literal ``MODEL_LESS = True`` at module scope — the
# file is parsed, never imported, so a computed or nested value is not honoured.
MODEL_LESS_MARKER = "MODEL_LESS"


@command(
    name="make:migration",
    help=(
        "Auto-generate migrations from models using Laravel 11+ ordering system "
        "(no timestamps). With --overwrite the migrations directory becomes "
        "EXACTLY one generated file per model table; every other .py file is "
        "deleted unless it declares a module-level 'MODEL_LESS = True' marker "
        "(for materialized views, extensions, data backfills and other objects "
        "no model owns). Preserved and deleted files are both printed."
    ),
    options={
        "--overwrite": (
            "Recreate all migrations from scratch: regenerate one file per table "
            "and DELETE every other migration except those marked MODEL_LESS = True"
        ),
        "--force": "Skip the hand-edit confirmation prompt when --overwrite clobbers files",
        "--style=blueprint": "Migration style (blueprint is the only supported SSOT)",
        "--dry_run": "Show what would be generated without creating files",
    },
)
class MakeMigrationCommand(CommandBase):
    def __init__(self, application):
        super().__init__(application)
        # Lazy DB import (optional 'db' extra: eloquent → psycopg2/faker). Runs
        # at command INSTANTIATION (only when make:migration is actually
        # invoked), so the module imports cleanly on a DB-less service.
        try:
            from cara.eloquent.migrations.MigrationGenerator import (
                MigrationGenerator,
            )
            from cara.eloquent.migrations.ModelDiscoverer import ModelDiscoverer
            from cara.eloquent.migrations.ModelMigrationComparator import (
                ModelMigrationComparator,
            )
        except ImportError as exc:
            raise missing_optional("db", exc) from exc
        self.discoverer = ModelDiscoverer()
        self.comparator = ModelMigrationComparator()
        self.generator = MigrationGenerator()

    def handle(self):
        """Generate migrations from model Field.* definitions."""
        with self.generator.generation_lock():
            return self._handle_locked()

    def _handle_locked(self):
        """Generate while holding the cross-process generation lock."""
        self.info("Auto-generating migrations from models...")

        if self.option("style", "blueprint") != "blueprint":
            self.error(
                "Only --style=blueprint is supported. Raw SQL cannot be "
                "round-tripped safely by the model comparator."
            )
            return 2

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
        unchanged_count = 0
        error_count = 0

        for model_info in ordered_models:
            result = self._process_model(model_info)
            if result == "created":
                created_count += 1
            elif result == "updated":
                updated_count += 1
            elif result == "error":
                error_count += 1
            else:  # "unchanged"
                unchanged_count += 1

        self._print_summary(
            created_count,
            updated_count,
            unchanged_count,
            error_count,
            dry_run=bool(self.option("dry_run")),
        )

    def _print_summary(
        self,
        created: int,
        updated: int,
        unchanged: int,
        error: int,
        dry_run: bool,
    ):
        """Print a single, actionable N created / N updated / N unchanged line."""
        verb = "Would create" if dry_run else "Created"
        # Always show the full tally so the run is self-describing (even a
        # no-op states that every model is already covered).
        self.success(f"{verb} {created} new, {updated} updated, {unchanged} unchanged")
        if error:
            # Errors were already printed per-model via self.error(); surface
            # the count so a partially-failed sweep isn't mistaken for success.
            self.warning(f"{error} model(s) could not be processed (see errors above)")

    def _handle_overwrite_mode(self):
        """Handle --overwrite mode: recreate all migrations from scratch.

        ``--overwrite`` DELETES every migration file that is not marked
        ``MODEL_LESS = True`` and regenerates exactly one per model table.
        Before unlinking anything we surface which files will be destroyed and
        which are preserved, and if any doomed file looks hand-edited (contains
        markers the generator never authors) we require an interactive confirm
        (or ``--force``) so the sweep can't silently wipe handwritten SQL.
        """
        self.info("Overwrite mode: Recreating all migrations from scratch...")

        # Discover models
        models = self.discoverer.discover_models()
        if not models:
            self.info("No models found in app/models directory")
            return

        # Sort models by dependency order (FK dependencies first)
        ordered_models = self.discoverer.resolve_dependency_order(models)

        # Render and compile the complete replacement before touching disk. A
        # bad model or stub cannot leave a half-erased migration set.
        try:
            prepared = self._prepare_overwrite(ordered_models)
        except Exception as exc:
            self.error(f"Overwrite preflight failed; no files changed: {exc}")
            return 1

        # Partition ONCE, before any disk write: after the replace the directory
        # holds the freshly generated files, so re-partitioning would count them
        # as deletions. This snapshot drives both the safety gate and the summary.
        doomed, preserved = self._partition_migrations()

        # Safety gate: refuse to silently clobber hand-edited migrations.
        # Returns False (abort) only when the user declines the confirm.
        if not self._confirm_clobber(doomed, preserved):
            self.warning("Aborted: no files were changed.")
            return

        # Reset migration counter for fresh numbering.
        # NOTE: the regenerated filenames are NNNN_01_01_NNNNNN_<name>.py. The
        # ``01_01`` middle segment is vestigial Laravel date cruft, but every
        # consumer (MigrationExecutor.run_pending_migrations / get_migration_status,
        # Migration.get_unran_migrations, the comparator's glob) orders purely by
        # LEXICOGRAPHIC sort on the whole filename string and never splits those
        # segments out — so they're load-bearing only as constant padding that
        # keeps the sort monotonic. Changing the shape is high-risk (the tracker
        # keys migrations by full filename, so a rename would make already-applied
        # migrations look pending) for zero functional gain, so it is deliberately
        # left intact. Do not "simplify" it.
        if self.option("dry_run"):
            for model_info, index, content in prepared:
                self.info(
                    f"Would create fresh migration for {model_info['name']} -> "
                    f"{model_info['table']} (order: {index})"
                )
                self.info(content)
            created_count = len(prepared)
        else:
            created_count = self._replace_model_migrations_atomically(doomed, prepared)

        # Summary: state the resulting CONTRACT, not just the count, so a run
        # that quietly removed 40 hand-written migrations says so.
        removed, preserved_files = len(doomed), len(preserved)
        verb = "Would recreate" if self.option("dry_run") else "Recreated"
        self.success(
            f"{verb} {created_count} migration(s) with dependency-based ordering "
            f"— one file per table"
        )
        if removed:
            self.warning(
                f"{removed} non-generated migration(s) "
                f"{'would be' if self.option('dry_run') else 'were'} deleted "
                f"(listed above). Mark a file '{MODEL_LESS_MARKER} = True' to keep it."
            )
        if preserved_files:
            self.info(
                f"{preserved_files} {MODEL_LESS_MARKER} file(s) preserved untouched."
            )

    def _prepare_overwrite(self, ordered_models):
        """Render and syntax-check the complete replacement set in memory."""
        style = self.option("style", "blueprint")
        prepared = []
        for index, model_info in enumerate(ordered_models):
            if not model_info.get("has_fields_method", False):
                continue
            content = self.generator.generate_create_migration(model_info, style)
            if not content:
                continue
            compile(content, f"<migration:{model_info['table']}>", "exec")
            prepared.append((model_info, index, content))
        return prepared

    def _replace_model_migrations_atomically(self, targets, prepared) -> int:
        """Move the doomed migrations aside; restore all of them on failure."""
        migrations_dir = self._migrations_dir() or self.generator.migrations_dir
        migrations_dir.mkdir(parents=True, exist_ok=True)
        backup_dir = Path(
            tempfile.mkdtemp(prefix=".cara-overwrite-", dir=str(migrations_dir))
        )
        moved: list[tuple[Path, Path]] = []
        generated: list[Path] = []
        counter_file = self.generator.counter_file
        previous_counter = counter_file.read_bytes() if counter_file.exists() else None

        try:
            for source in targets:
                backup = backup_dir / source.name
                os.replace(source, backup)
                moved.append((source, backup))

            self.generator.reset_counter()
            for model_info, dependency_order, content in prepared:
                generated.append(
                    self.generator.create_migration_file(
                        f"create_{model_info['table']}_table",
                        content,
                        dependency_order=dependency_order,
                    )
                )
            self.generator.finalize_counter()
        except BaseException:
            for path in generated:
                with contextlib.suppress(OSError):
                    path.unlink(missing_ok=True)
            for original, backup in reversed(moved):
                if backup.exists():
                    os.replace(backup, original)
            if previous_counter is None:
                counter_file.unlink(missing_ok=True)
            else:
                from cara.eloquent.migrations.MigrationGenerator import _atomic_write

                _atomic_write(counter_file, previous_counter.decode("utf-8"))
            self.generator.cancel_fresh_counter_batch()
            raise
        finally:
            shutil.rmtree(backup_dir, ignore_errors=True)

        return len(generated)

    def _migrations_dir(self):
        """Resolve the migrations directory via the paths() helper, or None."""
        from pathlib import Path

        from cara.support import paths

        migrations_dir = Path(paths("migrations"))
        return migrations_dir if migrations_dir.exists() else None

    def _partition_migrations(self):
        """Split the migrations directory into (deleted, preserved) file lists.

        ``--overwrite`` regenerates one file per model table, so ANY other .py
        file left behind breaks the one-file-per-table contract: it either
        duplicates a generated CREATE or applies an increment that the fresh
        CREATE already contains. Selecting by "does this file touch a model
        table" (the previous rule) let every hand-written ``add_*``/``backfill_*``
        migration survive forever.

        The single exemption is a module-level ``MODEL_LESS = True``, for schema
        objects no model can own. ``__init__.py`` is package plumbing, never a
        migration, and is left alone without needing the marker.
        """
        migrations_dir = self._migrations_dir()
        if migrations_dir is None:
            return [], []

        deleted: list[Path] = []
        preserved: list[Path] = []
        for file_path in sorted(migrations_dir.glob("*.py")):
            if file_path.name == "__init__.py":
                continue
            try:
                content = file_path.read_text(encoding="utf-8")
            except OSError as exc:
                raise RuntimeError(
                    f"Cannot safely inspect migration '{file_path.name}': {exc}"
                ) from exc
            if self._declares_model_less(content, file_path):
                preserved.append(file_path)
            else:
                deleted.append(file_path)
        return deleted, preserved

    @staticmethod
    def _declares_model_less(content: str, file_path) -> bool:
        """Whether the file carries a module-level ``MODEL_LESS = True``.

        Parsed, never imported — a migration must not execute to be classified,
        and importing one could open a database connection. A file that does not
        parse is NOT treated as marked: an unparseable migration is broken, and
        defaulting to "preserve" would let it dodge the sweep forever.
        """
        try:
            tree = ast.parse(content, filename=str(file_path))
        except SyntaxError:
            return False

        for node in tree.body:
            targets = []
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, ast.AnnAssign):
                targets = [node.target]
            else:
                continue
            if node.value is None:
                continue
            if not (
                isinstance(node.value, ast.Constant) and node.value.value is True
            ):
                continue
            for target in targets:
                if isinstance(target, ast.Name) and target.id == MODEL_LESS_MARKER:
                    return True
        return False


    # Comment fragments the generator DOES emit (inline annotations on the
    # drop/alter lines). Everything else after a ``#`` is a human comment.
    _GENERATED_COMMENT_MARKERS = ("DESTRUCTIVE", "altered:")

    def _looks_hand_edited(self, file_path) -> bool:
        """Heuristically detect whether a migration file was hand-edited.

        Conservative: only flags content the generator provably never authors
        — a code comment (``#``, whole-line OR inline) that isn't one of the
        generator's own ``# DESTRUCTIVE`` / ``# altered:`` annotations, or
        control-flow / escape-hatch logic the stub path never writes (``def``
        other than up/down, ``if``/``for``/``while``/``try``, ``DB.connection``,
        ``cursor``/``execute``/``raw``). The generator's ``table.*`` lines carry
        string literals but never a ``#``, so a ``#`` outside the module/method
        docstrings is a reliable human-edit signal. If the file can't be read,
        treat it as hand-edited so we err on the side of asking first.
        """
        try:
            text = file_path.read_text(encoding="utf-8")
        except OSError:
            return True

        suspicious_tokens = (
            "if ",
            "for ",
            "while ",
            "try:",
            "except",
            "DB.connection",
            ".cursor(",
            ".execute(",
            "raw(",
            "lambda",
            "import os",
        )
        in_docstring = False
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            # Skip docstring bodies — the generator's stub docstrings are the
            # only place a ``#`` could legitimately appear inside prose, and a
            # one-line ``"""..."""`` opens and closes on the same line.
            triple = line.count('"""')
            if in_docstring:
                if triple:
                    in_docstring = False
                continue
            if triple == 1:
                in_docstring = True
                continue
            if triple >= 2:
                # opens and closes on one line → not inside a docstring after
                continue

            # Any ``#`` (whole-line or inline) that isn't a generated annotation.
            if "#" in line:
                comment = line[line.index("#") :]
                if not any(m in comment for m in self._GENERATED_COMMENT_MARKERS):
                    return True
            # A def for something other than up()/down().
            if line.startswith("def ") and not (
                line.startswith("def up(") or line.startswith("def down(")
            ):
                return True
            if any(tok in line for tok in suspicious_tokens):
                return True
        return False

    def _confirm_clobber(self, targets, preserved) -> bool:
        """Preview + gate the destructive unlink. Returns True to proceed.

        Always prints WHICH files --overwrite deletes and which it PRESERVES,
        so the one-file-per-table sweep is auditable before it runs. If any
        deleted file looks hand-edited, requires an interactive confirm unless
        ``--force`` (or ``--dry_run``, which never touches disk). Returns False
        only when the user explicitly declines — the caller then aborts.
        """
        if preserved:
            self.info(
                f"--overwrite will PRESERVE {len(preserved)} file(s) marked "
                f"{MODEL_LESS_MARKER} = True:"
            )
            for file_path in preserved:
                self.info(f"   • {file_path.name}")

        if not targets:
            return True

        self.warning(f"--overwrite will DELETE and regenerate {len(targets)} file(s):")
        edited = []
        for file_path in targets:
            hand_edited = self._looks_hand_edited(file_path)
            marker = "  (hand-edited?)" if hand_edited else ""
            self.info(f"   • {file_path.name}{marker}")
            if hand_edited:
                edited.append(file_path)

        # Dry-run never writes; --force is the documented escape hatch.
        if self.option("dry_run") or self.option("force"):
            return True

        if edited:
            self.warning(
                f"{len(edited)} file(s) appear hand-edited — overwriting will "
                f"discard those changes."
            )
            return self.confirm(
                "Overwrite hand-edited migration(s) anyway?", default=False
            )
        return True

    def _create_fresh_migration(self, model_info, dependency_order=0):
        """Create a fresh CREATE TABLE migration for a model."""
        style = self.option("style", "blueprint")

        # Skip VIEW-only models (no fields property, backed by a SQL VIEW).
        if not model_info.get("has_fields_method", False):
            self.info(f"{model_info['name']} is up to date with migrations")
            return "skipped"

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

        # Skip VIEW-only models (no fields property).
        if not model_info.get("has_fields_method", False):
            self.info(f"{model_info['name']} is up to date with migrations")
            return "unchanged"

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

                    # Intent-revealing name from the change set (add_x_to_y /
                    # drop_x_from_y / rename_x_to_y_on_y / change_x_on_y), not
                    # a generic update_<table>_table.
                    from cara.eloquent.migrations.ModelMigrationComparator import (
                        summarize_change_name,
                    )

                    name, _ = summarize_change_name(table_name, diff)
                    content = self.generator.generate_update_migration(
                        model_info, diff, self.option("style", "blueprint")
                    )
                    if not self.option("dry_run"):
                        filepath = self.generator.create_migration_file(name, content)
                        self.info(f"Created migration: \n{filepath}")
                    else:
                        self.info(
                            f"Would create '{name}' (update) for {model_info['name']}:"
                        )
                        self.info(content)
                        self.info("=" * 50)
                    return "updated"
                else:
                    # Table doesn't exist, create table migration
                    self.info(
                        f"Creating migration for {model_info['name']} -> {table_name}"
                    )
                    name = f"create_{table_name}_table"
                    content = self.generator.generate_create_migration(
                        model_info, self.option("style", "blueprint")
                    )
                    if not self.option("dry_run"):
                        filepath = self.generator.create_migration_file(name, content)
                        self.info(f"Created migration: \n{filepath}")
                    else:
                        self.info(f"Would create '{name}' for {model_info['name']}:")
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
