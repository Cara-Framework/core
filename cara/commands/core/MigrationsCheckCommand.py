"""MigrationsCheckCommand: enforce the migration convention that ``make:migration``
only *implements*.

``make:migration --overwrite`` makes the migrations directory a function of the
models. Nothing verified that it STAYED one. Between two regenerations anybody
can drop an ``add_x_to_y.py`` in, or hand-edit an index into a generated file —
and the next from-scratch install is the first thing to notice.
``migrations:check`` is the CI gate for the convention:

  1. ONE FILE PER TABLE — every model table has exactly one generated
     ``create_<table>_table.py``; zero and two are both defects.
  2. GENERATED FILES ONLY — every file must be a generated create-table file.
     There are NO exemption markers: ``MODEL_LESS``, ``MODEL_TRANSITION`` and
     ``DROPPED_INDEXES`` are banned outright, and a file carrying one is a
     violation in itself. The escape hatch existed for content that now has a
     model-side home — framework tables are declared in ``cara.models``,
     triggers and named constraints live in a model's ``__indexes__`` DDL
     entries, and one-time data rewrites are not migrations at all. The hatch
     was removed after it accumulated 23 files in one product, six of which
     redeclared columns their models already owned: two sources of truth,
     one of them non-idempotent, so the documented regenerate workflow itself
     would have broken the next from-scratch install.
  3. UTC EVERYWHERE — a naive ``TIMESTAMP`` in migration SQL re-creates the
     mixed-awareness cast that makes index expressions non-IMMUTABLE, which is
     what made a from-scratch migrate die in both products. Generated files
     carry model-authored ``__indexes__`` SQL, so they are scanned too.
  4. NO DUPLICATES — two files creating the same table.
  5. INDEXES BELONG TO MODELS — an index that exists only inside a migration
     file is silently DROPPED by the next regenerate-from-models. No
     historical-immutability exemption: history is regenerated, not preserved.
  6. LITERAL DEFAULTS — generated migrations must be replayable without model
     imports; schema defaults therefore cannot contain class/constant lookups.

(From-scratch installability itself is not statically checkable; it is the
acceptance test these rules exist to protect: an empty database migrated from
zero must equal the models' schema, byte for byte.)

Every file is parsed with ``ast``, never imported — importing a migration can
open a database connection, and a gate must not need one.

``--fix`` takes the ``make:migration --overwrite --force`` path and re-audits,
printing which files it added and removed. A naive timestamp in model-authored
SQL is reported but never auto-fixed (the fix belongs in the model). A
hand-added index, a duplicated CREATE TABLE and an unparseable file BLOCK the
fix entirely — regenerating would DESTROY the thing the check just found.

Exit code is 0 only when the directory is clean, so CI can gate on it.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

from cara.commands import CommandBase, missing_optional
from cara.commands.core.MakeMigrationCommand import MakeMigrationCommand
from cara.decorators import command

# Filenames the generator authors: ``NNNN_01_01_NNNNNN_create_<table>_table.py``.
# The middle segments are vestigial Laravel date padding that keeps the
# lexicographic ordering monotonic (see MakeMigrationCommand) — matched loosely
# on purpose, only the table name is load-bearing here.
_GENERATED_NAME_RE = re.compile(
    r"^\d+_\d+_\d+_\d+_create_(?P<table>[a-z0-9_]+?)_table\.py$"
)

# ``CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] <name> ON <table>`` —
# both halves captured, because rule 5 is about WHICH table's index escaped its
# model. SchemaCheckCommand's sibling regex captures only the name.
_CREATE_INDEX_ON_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<name>\w+)\"?\s+ON\s+"
    r"(?:ONLY\s+)?\"?(?:\w+\"?\.\"?)?(?P<table>\w+)\"?",
    re.IGNORECASE,
)

# ``CREATE TABLE [IF NOT EXISTS] <table>`` in raw SQL — the duplicate-table
# check has to see past filenames, since raw SQL can create a table a
# generated file already owns.
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"\"?(?:\w+\"?\.\"?)?(?P<table>\w+)\"?",
    re.IGNORECASE,
)

# The retired escape hatches. Their NAMES stay known so a file that still
# declares one is reported as exactly that, with a remedy, instead of as a
# generic parse curiosity. Any assignment to one of these at module scope is a
# violation regardless of the assigned value.
_BANNED_MARKERS = ("MODEL_LESS", "MODEL_TRANSITION", "DROPPED_INDEXES")

# A naive TIMESTAMP. ``CURRENT_TIMESTAMP`` / ``LOCALTIMESTAMP`` / ``to_timestamp``
# are excluded by the lookbehind (they are preceded by a word character),
# ``TIMESTAMPTZ`` and ``TIMESTAMP WITH TIME ZONE`` by the lookaheads. What is
# left — bare ``TIMESTAMP`` and ``TIMESTAMP WITHOUT TIME ZONE`` — is exactly the
# naive declaration rule 3 forbids.
_NAIVE_TIMESTAMP_RE = re.compile(
    r"(?<![A-Za-z0-9_])TIMESTAMP(?!TZ)(?!\s+WITH\s+TIME\s+ZONE)(?![A-Za-z0-9_])",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Violation:
    """One convention breach, with the file it lives in and its one-line remedy.

    ``blocks_fix`` marks a violation that ``--fix`` must not run THROUGH:
    regenerating would erase the evidence (a hand-added index) rather than
    repair it. ``human_only`` marks one regeneration simply cannot address.
    """

    rule: str
    path: str
    message: str
    remedy: str
    human_only: bool = False
    blocks_fix: bool = False


@dataclass(frozen=True)
class MigrationFile:
    """A parsed migration file: classification + the SQL text it contains."""

    path: Path
    generated_table: str | None
    banned_markers: tuple[str, ...]
    docstring: str | None
    sql_constants: tuple[tuple[int, str], ...]
    syntax_error: str | None


def _string_constants(tree: ast.AST) -> list[tuple[int, str]]:
    """Every ``str`` literal in the module EXCEPT docstrings, with line numbers.

    SQL lives in string literals; prose lives in docstrings. Scanning the raw
    file text instead would flag a docstring that merely mentions ``TIMESTAMP``
    — a false positive in a CI gate is how a CI gate gets ignored.
    """
    docstring_nodes: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(
            node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
        ):
            continue
        body = getattr(node, "body", None) or []
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            docstring_nodes.add(id(body[0].value))

    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstring_nodes
        ):
            found.append((node.lineno, node.value))
    return found


def _non_literal_defaults(path: Path) -> list[tuple[int, str]]:
    """Return generated ``.default(...)`` calls that need runtime name lookup.

    Migration generation copies the model's schema expression into a standalone
    module. A model constant such as ``Product.SOURCE_MANUAL`` is not imported
    there and only fails when a clean database executes the migration. Requiring
    a literal keeps historical migrations self-contained and deterministic.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "default"
            and node.args
        ):
            continue
        try:
            ast.literal_eval(node.args[0])
        except ValueError, TypeError:
            found.append((node.lineno, ast.unparse(node.args[0])))
    return found


def _banned_markers(tree: ast.Module) -> tuple[str, ...]:
    """Module-scope assignments to any retired escape-hatch name."""
    found: list[str] = []
    for node in tree.body:
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        for target in targets:
            if isinstance(target, ast.Name) and target.id in _BANNED_MARKERS:
                found.append(target.id)
    return tuple(dict.fromkeys(found))


def parse_migration_file(path: Path) -> MigrationFile:
    """Classify one migration file by PARSING it. Never imports it."""
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        return MigrationFile(path, None, (), None, (), f"unreadable: {exc}")

    try:
        tree = ast.parse(content, filename=str(path))
    except SyntaxError as exc:
        return MigrationFile(path, None, (), None, (), f"does not parse: {exc}")

    match = _GENERATED_NAME_RE.match(path.name)
    return MigrationFile(
        path=path,
        generated_table=match.group("table") if match else None,
        banned_markers=_banned_markers(tree),
        docstring=ast.get_docstring(tree),
        sql_constants=tuple(_string_constants(tree)),
        syntax_error=None,
    )


def audit_migrations(
    migrations_dir: Path, model_indexes: dict[str, set[str]]
) -> list[Violation]:
    """Audit a migrations directory against the convention. Pure — no DB, no imports.

    ``model_indexes`` maps every model table to the set of index names its model
    declares (``__indexes__`` raw SQL + ``field.index([...])``); its KEYS are the
    authoritative table list.
    """
    files = [
        parse_migration_file(path)
        for path in sorted(migrations_dir.glob("*.py"))
        if path.name != "__init__.py"
    ]

    violations: list[Violation] = []
    # table -> set of files that create it, from BOTH routes: a generated
    # filename and a raw CREATE TABLE. Sets, because a hand-edited generated
    # file could otherwise be counted twice and reported as its own duplicate.
    creators: dict[str, set[str]] = {}
    generated_files: set[str] = set()

    for entry in files:
        name = entry.path.name
        if entry.syntax_error:
            violations.append(
                Violation(
                    rule="unparseable",
                    path=name,
                    message=f"migration {entry.syntax_error}",
                    remedy=(
                        "fix or delete the file by hand; a file that does not "
                        "parse cannot be classified, so --fix refuses to run"
                    ),
                    human_only=True,
                    blocks_fix=True,
                )
            )
            continue

        for marker in entry.banned_markers:
            violations.append(
                Violation(
                    rule="banned-marker",
                    path=name,
                    message=(
                        f"declares {marker}, a retired escape hatch — the "
                        "migrations directory carries generated files only"
                    ),
                    remedy=(
                        "move the schema into a model (columns as fields, named "
                        "DDL in __indexes__; framework tables live in "
                        "cara.models) and regenerate; data rewrites are not "
                        "migrations"
                    ),
                )
            )

        if entry.generated_table and not entry.banned_markers:
            creators.setdefault(entry.generated_table, set()).add(name)
            generated_files.add(name)
            for line, expression in _non_literal_defaults(entry.path):
                violations.append(
                    Violation(
                        rule="non-literal-default",
                        path=name,
                        message=(
                            f"line {line} default {expression!r} requires a "
                            "runtime name lookup"
                        ),
                        remedy=(
                            "declare the default as a literal in the model and "
                            "regenerate migrations"
                        ),
                        human_only=True,
                        blocks_fix=True,
                    )
                )
        elif not entry.banned_markers:
            violations.append(
                Violation(
                    rule="incremental-migration",
                    path=name,
                    message="not a generated create_<table>_table.py",
                    remedy=(
                        "put the change in the model and regenerate "
                        "(craft make:migration --overwrite --force)"
                    ),
                )
            )

        for lineno, text in entry.sql_constants:
            # A bare identifier cannot be a type declaration — a product
            # legitimately has a COLUMN named "timestamp", and the generated
            # ``table.datetime("timestamp")`` line must not read as naive SQL.
            # Naive TIMESTAMP as a TYPE only ever appears inside a longer
            # statement, which always carries whitespace.
            if " " not in text:
                continue
            if _NAIVE_TIMESTAMP_RE.search(text):
                violations.append(
                    Violation(
                        rule="naive-timestamp",
                        path=f"{name}:{lineno}",
                        message=(
                            "naive TIMESTAMP in migration SQL — mixing naive and "
                            "aware values needs a non-IMMUTABLE cast, so index "
                            "expressions over it cannot build"
                        ),
                        remedy=(
                            "declare the column TIMESTAMPTZ (UTC everywhere); "
                            "the SQL comes from a model's __indexes__, so fix "
                            "it there and regenerate"
                        ),
                        human_only=True,
                    )
                )
                # One report per file is enough to send a human in; listing
                # every column would bury the other rules in identical lines.
                break

        for table in _created_tables(entry):
            creators.setdefault(table, set()).add(name)

    for entry in files:
        if entry.syntax_error:
            continue
        violations.extend(_audit_indexes(entry, model_indexes))

    violations.extend(_audit_table_coverage(creators, generated_files, model_indexes))
    return violations


def _audit_indexes(
    entry: MigrationFile,
    model_indexes: dict[str, set[str]],
) -> list[Violation]:
    """Rule 5: an index on a MODEL table must be declared by that model.

    An index that exists only inside a migration file survives exactly until the
    next regenerate-from-models, which rebuilds the file from the model and
    drops it — silently, because nothing compares the two. No historical
    exemptions: history is regenerated, not preserved.
    """
    violations: list[Violation] = []
    seen: set[str] = set()

    for _, text in entry.sql_constants:
        for match in _CREATE_INDEX_ON_RE.finditer(text):
            index_name, table = match.group("name"), match.group("table")
            if table not in model_indexes or index_name in seen:
                continue
            if index_name in model_indexes[table]:
                continue
            seen.add(index_name)
            violations.append(
                Violation(
                    rule="undeclared-index",
                    path=entry.path.name,
                    message=(
                        f"index '{index_name}' on model table '{table}' is not "
                        "declared by the model — regeneration will drop it"
                    ),
                    remedy=(
                        f"move it into {table}'s model __indexes__ (or field.index) "
                        "and regenerate"
                    ),
                    human_only=True,
                    blocks_fix=True,
                )
            )
    return violations


def _created_tables(entry: MigrationFile) -> set[str]:
    """Table names the file CREATEs in raw SQL.

    Generated files build tables through the Blueprint DSL, not raw
    ``CREATE TABLE``, so this reports what raw SQL creates — the only way a
    duplicate can slip past the generator's naming.
    """
    tables: set[str] = set()
    for _, text in entry.sql_constants:
        for match in _CREATE_TABLE_RE.finditer(text):
            tables.add(match.group("table").lower())
    return tables


def _audit_table_coverage(
    creators: dict[str, set[str]],
    generated_files: set[str],
    model_indexes: dict[str, set[str]],
) -> list[Violation]:
    """Rules 1 and 4: exactly one creating file per model table, no orphans."""
    violations: list[Violation] = []

    for table in sorted(model_indexes):
        files = creators.get(table, set())
        if not files:
            violations.append(
                Violation(
                    rule="missing-migration",
                    path=f"create_{table}_table.py",
                    message=f"model table '{table}' has no create migration",
                    remedy="regenerate (craft make:migration --overwrite --force)",
                )
            )

    for table, files in sorted(creators.items()):
        if len(files) > 1:
            violations.append(
                Violation(
                    rule="duplicate-table",
                    path=", ".join(sorted(files)),
                    message=(f"table '{table}' is created by {len(files)} files"),
                    remedy=(
                        "delete the redundant file; one table is created by "
                        "exactly one migration"
                    ),
                    # A duplicate involving a hand-written file is a human call:
                    # regeneration would delete one side without being asked.
                    human_only=True,
                    blocks_fix=True,
                )
            )
        elif table not in model_indexes and files <= generated_files:
            violations.append(
                Violation(
                    rule="orphan-migration",
                    path=next(iter(files)),
                    message=(
                        f"creates table '{table}', which no model declares "
                        "(model deleted or renamed?)"
                    ),
                    remedy="regenerate to drop it, or declare the owning model",
                )
            )

    return violations


@command(
    name="migrations:check",
    help=(
        "Audit the migrations directory against the migration convention: one "
        "generated file per model table and nothing else — no incremental "
        "files, no escape markers (MODEL_LESS and friends are banned), "
        "TIMESTAMPTZ everywhere, no duplicate or orphan table creations, and "
        "no index living only in a migration. "
        "Exits non-zero on any violation so CI can gate on it."
    ),
    options={
        "--fix": (
            "Regenerate the directory from the models (the make:migration "
            "--overwrite --force path) and re-audit. Naive timestamps are "
            "reported, never auto-fixed (fix the model); a hand-added index "
            "or duplicate table blocks the fix."
        ),
    },
)
class MigrationsCheckCommand(CommandBase):
    def handle(self):
        """Audit the migrations directory; with --fix, regenerate and re-audit."""
        migrations_dir = self._migrations_dir()
        if migrations_dir is None:
            self.error(
                "No migrations directory found (paths('migrations')). Nothing to check."
            )
            return 2

        try:
            model_indexes = self._model_indexes()
        except ImportError as exc:
            raise missing_optional("db", exc) from exc

        if not model_indexes:
            self.warning("No models with declared fields found; nothing to check.")
            return 0

        self.info(f"Auditing {migrations_dir} against the migration convention...")
        violations = audit_migrations(migrations_dir, model_indexes)

        if self.option("fix"):
            violations = self._run_fix(migrations_dir, model_indexes, violations)

        return self._report(violations, len(model_indexes))

    # --- fix ---------------------------------------------------------------

    def _run_fix(
        self,
        migrations_dir: Path,
        model_indexes: dict[str, set[str]],
        violations: list[Violation],
    ) -> list[Violation]:
        """Regenerate from models, then re-audit. Returns the surviving violations."""
        blocking = [v for v in violations if v.blocks_fix]
        if blocking:
            self.error(
                f"--fix refused: {len(blocking)} violation(s) would be DESTROYED "
                "rather than repaired by regenerating (listed below). Resolve "
                "them by hand first."
            )
            return violations

        fixable = [v for v in violations if not v.human_only]
        if not fixable:
            self.info("Nothing for --fix to regenerate.")
            return violations

        before = {path.name for path in migrations_dir.glob("*.py")}

        maker = MakeMigrationCommand(self.application)
        maker.set_parsed_options({"overwrite": True, "force": True, "style": "blueprint"})
        result = maker.handle()
        if result:
            self.error("Regeneration failed; the directory was left unchanged.")
            return violations

        after = {path.name for path in migrations_dir.glob("*.py")}
        self._report_changes(before, after)

        return audit_migrations(migrations_dir, model_indexes)

    def _report_changes(self, before: set[str], after: set[str]):
        """State what --fix actually changed on disk — never a silent sweep."""
        for name in sorted(before - after):
            self.warning(f"   - removed {name}")
        for name in sorted(after - before):
            self.success(f"   + added {name}")
        if before == after:
            self.info("   (no files added or removed)")

    # --- output ------------------------------------------------------------

    def _report(self, violations: list[Violation], table_count: int) -> int:
        """Print grouped violations and return the process exit code."""
        self.info("\n" + "=" * 60)
        if not violations:
            self.success(
                f"Migration convention clean — {table_count} model table(s), "
                "one generated file each."
            )
            return 0

        by_rule: dict[str, list[Violation]] = {}
        for violation in violations:
            by_rule.setdefault(violation.rule, []).append(violation)

        for rule in sorted(by_rule):
            self.warning(f"\n{rule} ({len(by_rule[rule])}):")
            for violation in by_rule[rule]:
                self.info(f"   × {violation.path}: {violation.message}")
                self.info(f"     → {violation.remedy}")

        human_only = sum(1 for v in violations if v.human_only)
        self.warning(
            f"\n⚠ {len(violations)} convention violation(s) across "
            f"{table_count} model table(s)."
        )
        if human_only:
            self.warning(
                f"{human_only} of them need a HUMAN decision and are never auto-fixed."
            )
        if any(not v.human_only for v in violations) and not self.option("fix"):
            self.info("Run 'python craft migrations:check --fix' to regenerate.")
        return 1

    # --- inputs ------------------------------------------------------------

    def _migrations_dir(self) -> Path | None:
        """Resolve the migrations directory via the paths() helper, or None."""
        from cara.support import paths

        migrations_dir = Path(paths("migrations"))
        return migrations_dir if migrations_dir.exists() else None

    def _model_indexes(self) -> dict[str, set[str]]:
        """``{model table: declared index names}`` — the authoritative table list.

        Lazy DB import: ``cara.eloquent`` pulls the optional 'db' extra, so a
        DB-less service still imports this module. Discovery reads model
        SOURCE, not the database — the audit itself never connects.
        """
        from cara.commands.core.SchemaCheckCommand import SchemaCheckCommand
        from cara.eloquent.migrations import ModelDiscoverer

        indexes: dict[str, set[str]] = {}
        for model in ModelDiscoverer().discover_models():
            table = model.get("table")
            if not table or not model.get("has_fields_method"):
                continue
            indexes[table] = SchemaCheckCommand._declared_indexes(model)
        return indexes
