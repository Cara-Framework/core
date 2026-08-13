"""
MakeMigrationCommand: the migrations directory as a function of the models.

The command has exactly two modes, and only one of them writes:

* **Bare (report).** Compares every model against the generated directory
  with the structured differ and PRINTS the typed changes — added / removed /
  altered / renamed columns, missing create files, orphaned files whose model
  is gone. Exit 1 when anything drifts, 0 when the directory is exactly the
  models. It never creates a file: the bare mode used to emit incremental
  ``add_x_to_y`` migrations from this same diff, which is precisely the file
  class ``migrations:check`` bans — one command manufacturing what the other
  rejects. The diff intelligence survives as the report; the emission is gone.

* **``--overwrite`` (write).** Regenerates the whole directory: ONE FILE PER
  TABLE, with no exceptions: after regenerating, the directory contains
  exactly the model-generated set and nothing else. It used to delete only
  the files it recognised as its own, so hand-written ``add_*`` / ``alter_*``
  / ``backfill_*`` / ``fix_*`` migrations accumulated forever (one product
  reached 123 generated + 40 hand-written) and the directory stopped being a
  function of the models.

Both modes REFUSE in production. Regeneration renumbers files an applied
ledger already references, and a report against production models answers a
question the deploy pipeline should be asking of the repository instead. The
production schema path is the evolve workflow (DOCTRINE §migrations): planned,
ordered, append-only operations against the deployed database — built at
cutover, refused here so nobody reaches for the dev tool out of habit.

There are no escape markers. ``MODEL_LESS = True`` used to exempt a file from
the purge for "objects no model can own", and ``MODEL_TRANSITION`` preserved
applied rename chains. Both hatches turned back into dumping grounds: one
product accumulated 23 marked files, six of which redeclared columns their
models ALREADY owned — two sources of truth for eleven columns, one of them
non-idempotent, so the documented regenerate workflow itself would have broken
a from-scratch install. The framework now declares its own tables as models
(``cara.models``), triggers and named constraints live in a model's
``__indexes__`` DDL entries, and one-time data rewrites are simply not
migrations — this directory rebuilds the WORLD from the models, and anything
history-dependent has no home in it. A marked file found today is a hard
error in ``migrations:check`` and is deleted by the sweep like any other
stray.
"""

from __future__ import annotations

import contextlib
import os
import re
import shutil
import tempfile
from pathlib import Path

from cara.commands.CommandBase import CommandBase
from cara.commands.OptionalDependencyError import missing_optional
from cara.configuration import config
from cara.decorators import command
from cara.eloquent.migrations.MigrationGenerator import _atomic_write
from cara.support import paths


@command(
    name="make:migration",
    help=(
        "Bare: REPORT model↔directory drift (typed column diffs, missing and "
        "orphaned files) and exit 1 on drift — writes nothing. With "
        "--overwrite: regenerate the directory as EXACTLY one generated file "
        "per model table; every other .py file is deleted — there are no "
        "exemption markers. Framework tables are models too (cara.models), "
        "triggers and named constraints belong in a model's __indexes__ "
        "entries, and data rewrites are not migrations. Deleted files are "
        "printed. 'migrations:check' audits the same contract statically; "
        "'schema:verify' proves it against a scratch database. Both modes "
        "refuse in production — the deployed schema evolves through the "
        "evolve workflow, never by regeneration."
    ),
    options=[
        {
            "name": "--overwrite",
            "help": "Recreate all migrations from scratch: regenerate one file per table and DELETE every other migration",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--force",
            "help": "Skip the hand-edit confirmation prompt when --overwrite clobbers files",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
        {
            "name": "--style",
            "help": "Migration style (blueprint is the only supported SSOT)",
            "type": str,
            "default": "blueprint",
            "is_flag": False,
        },
        {
            "name": "--dry_run",
            "help": "With --overwrite: show what would be generated without creating files",
            "type": bool,
            "default": False,
            "is_flag": True,
        },
    ],
)
class MakeMigrationCommand(CommandBase):
    def __init__(self, application):
        super().__init__(application)
        # Lazy DB import (optional 'db' extra: eloquent → psycopg2/faker). Runs
        # at command INSTANTIATION (only when make:migration is actually
        # invoked), so the module imports cleanly on a DB-less service.
        try:
            from cara.eloquent.migrations.MigrationGenerator import (
                MigrationGenerator,  # local: heavy optional dep
            )
            from cara.eloquent.migrations.ModelDiscoverer import (
                ModelDiscoverer,  # local: heavy optional dep
            )
            from cara.eloquent.migrations.ModelMigrationComparator import (
                ModelMigrationComparator,  # local: heavy optional dep
            )
        except ImportError as exc:
            raise missing_optional("db", exc) from exc
        self.discoverer = ModelDiscoverer()
        self.comparator = ModelMigrationComparator()
        self.generator = MigrationGenerator()

    def handle(self):
        """Report drift (bare) or regenerate the directory (--overwrite)."""
        if self._refuse_in_production():
            return 2
        with self.generator.generation_lock():
            return self._handle_locked()

    def _refuse_in_production(self) -> bool:
        """Neither mode may run against a production environment.

        Same guard idiom as ``dev:reset``: the environment name is the only
        thing consulted, and no flag can talk past it. Regeneration renumbers
        files the applied ledger references by filename, so on a deployed
        database every already-applied migration would look pending again.
        """
        if (config("app.env", "") or "").lower() in ("production", "prod"):
            self.error(
                "Refusing to run make:migration in production. The migrations "
                "directory is regenerated from models in development only; the "
                "deployed schema evolves through the evolve workflow "
                "(DOCTRINE §migrations). If this is not production, fix APP_ENV."
            )
            return True
        return False

    def _handle_locked(self):
        """Run the selected mode while holding the cross-process lock."""
        if self.option("style", "blueprint") != "blueprint":
            self.error(
                "Only --style=blueprint is supported. Raw SQL cannot be "
                "round-tripped safely by the model comparator."
            )
            return 2

        if self.option("overwrite", False):
            self.info("Auto-generating migrations from models...")
            return self._handle_overwrite_mode()

        return self._handle_report_mode()

    def _handle_report_mode(self):
        """Print typed model↔directory drift; write nothing; exit 1 on drift.

        The report answers one question — is the directory still a function
        of the models? — from both directions: a model whose generated file is
        missing or stale (typed column diffs from the structured comparator),
        and a generated file whose model no longer exists. The remedy is
        always the same single command, so the report names it and stops;
        deciding WHEN to regenerate stays with the operator, which is what
        makes this safe to wire into CI and pre-commit.
        """
        self.info("Comparing models against the generated migrations directory...")

        models = self.discoverer.discover_models()
        if not models:
            self.info("No models found in app/models directory")
            return

        ordered_models = self.discoverer.resolve_dependency_order(models)

        drifted = 0
        in_sync = 0
        for model_info in ordered_models:
            if not model_info.get("has_fields_method", False):
                in_sync += 1
                continue
            table_name = model_info["table"]
            diff = self.comparator.compare_model_with_migrations(model_info)
            if not diff:
                in_sync += 1
                continue
            drifted += 1
            if self.comparator.table_exists_in_migrations(table_name):
                from cara.eloquent.migrations.ModelMigrationComparator import (
                    summarize_change_name,  # local: heavy optional dep
                )

                label, _ = summarize_change_name(table_name, diff)
                self.warning(f"{model_info['name']} → {table_name}  ({label})")
                for change in diff:
                    self.info(f"   • {change}")
            else:
                self.warning(
                    f"{model_info['name']} → {table_name}  "
                    f"(no generated create_{table_name}_table file)"
                )

        orphans = self._orphaned_files({m["table"] for m in ordered_models})
        for orphan in orphans:
            self.warning(f"{orphan}  (no model owns this file)")

        total = drifted + len(orphans)
        self.info("")
        if total:
            self.warning(
                f"Drift: {drifted} model(s) out of sync, {len(orphans)} orphaned "
                f"file(s), {in_sync} in sync."
            )
            self.warning(
                "Regenerate with 'python craft make:migration --overwrite' "
                "(one file per table; nothing else survives)."
            )
            return 1
        self.success(
            f"All {in_sync} model(s) in sync — the directory is exactly the models."
        )
        return 0

    def _orphaned_files(self, model_tables: set[str]) -> list[str]:
        """Generated create-files whose table no model declares any more.

        Only generated names are considered: a non-generated file is a
        different violation with a dedicated report in ``migrations:check``,
        and double-reporting it here would suggest two remedies for one file.
        """
        migrations_dir = self._migrations_dir()
        if migrations_dir is None:
            return []

        generated = re.compile(r"^\d{4}_\d{2}_\d{2}_\d{6}_create_(.+)_table\.py$")
        orphans = []
        for path in sorted(migrations_dir.glob("*.py")):
            match = generated.match(path.name)
            if match and match.group(1) not in model_tables:
                orphans.append(path.name)
        return orphans

    def _handle_overwrite_mode(self):
        """Handle --overwrite mode: recreate all migrations from scratch.

        ``--overwrite`` DELETES every migration file and regenerates exactly
        one per model table. Before unlinking anything we surface which files
        will be destroyed, and if any doomed file looks hand-edited (contains
        markers the generator never authors) we require an interactive confirm
        (or ``--force``) so the sweep can't silently wipe handwritten SQL —
        the confirm is the moment to move that SQL into a model's
        ``__indexes__`` entries, which is its only durable home.
        """
        self.info("Overwrite mode: Recreating all migrations from scratch...")

        # Discover models
        models = self.discoverer.discover_models()
        if not models:
            self.info("No models found in app/models directory")
            return

        # Sort models by dependency order (FK dependencies first)
        ordered_models = self.discoverer.resolve_dependency_order(models)

        try:
            doomed = self._partition_migrations()
        except RuntimeError as exc:
            self.error(f"Overwrite preflight failed; no files changed: {exc}")
            return 1

        # Render and compile the complete replacement before touching disk. A
        # bad model or stub cannot leave a half-erased migration set.
        try:
            prepared = self._prepare_overwrite(ordered_models)
        except Exception as exc:
            self.error(f"Overwrite preflight failed; no files changed: {exc}")
            return 1

        # Safety gate: refuse to silently clobber hand-edited migrations.
        # Returns False (abort) only when the user declines the confirm.
        if not self._confirm_clobber(doomed):
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
        removed = len(doomed)
        verb = "Would recreate" if self.option("dry_run") else "Recreated"
        self.success(
            f"{verb} {created_count} migration(s) with dependency-based ordering "
            f"— one file per table, no exemptions"
        )
        if removed:
            self.warning(
                f"{removed} non-generated migration(s) "
                f"{'would be' if self.option('dry_run') else 'were'} deleted "
                f"(listed above). Schema they carried belongs in a model "
                f"(columns as fields, named DDL in __indexes__); data rewrites "
                f"are not migrations."
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
                _atomic_write(counter_file, previous_counter.decode("utf-8"))
            self.generator.cancel_fresh_counter_batch()
            raise
        finally:
            shutil.rmtree(backup_dir, ignore_errors=True)

        return len(generated)

    def _migrations_dir(self):
        """Resolve the migrations directory via the paths() helper, or None."""

        migrations_dir = Path(paths("migrations"))
        return migrations_dir if migrations_dir.exists() else None

    def _partition_migrations(self):
        """Return every migration file the overwrite will delete.

        ``--overwrite`` regenerates one file per model table, so ANY other .py
        file left behind breaks the one-file-per-table contract: it either
        duplicates a generated CREATE or applies an increment that the fresh
        CREATE already contains. There is nothing to preserve — the exemption
        markers this method used to honour are gone, because every kind of
        content they sheltered now has a model-side home. ``__init__.py`` is
        package plumbing.
        """
        migrations_dir = self._migrations_dir()
        if migrations_dir is None:
            return []

        doomed = [
            path
            for path in sorted(migrations_dir.glob("*.py"))
            if path.name != "__init__.py"
        ]
        for file_path in doomed:
            try:
                file_path.read_text(encoding="utf-8")
            except OSError as exc:
                raise RuntimeError(
                    f"Cannot safely inspect migration '{file_path.name}': {exc}"
                ) from exc
        return doomed

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

    def _confirm_clobber(self, targets) -> bool:
        """Preview + gate the destructive unlink. Returns True to proceed.

        Always prints WHICH files --overwrite deletes, so the
        one-file-per-table sweep is auditable before it runs. If any deleted
        file looks hand-edited, requires an interactive confirm unless
        ``--force`` (or ``--dry_run``, which never touches disk). Returns False
        only when the user explicitly declines — the caller then aborts. The
        confirm is deliberately the LAST stop for handwritten SQL: whatever it
        expresses must move into a model (columns as fields, named DDL as
        ``__indexes__`` entries) or it is gone.
        """
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
