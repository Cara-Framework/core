"""MigrationsCheckCommand: enforce the migration convention that ``make:migration``
only *implements*.

``make:migration --overwrite`` makes the migrations directory a function of the
models. Nothing verified that it STAYED one. Between two regenerations anybody
can drop an ``add_x_to_y.py`` in, hand-edit an index into a generated file, or
write a MODEL_LESS migration with a naive ``TIMESTAMP`` column — and the next
from-scratch install is the first thing to notice. ``migrations:check`` is the
CI gate for the convention:

  1. ONE FILE PER TABLE — every model table has exactly one generated
     ``create_<table>_table.py``; zero and two are both defects.
  2. NO INCREMENTAL MIGRATIONS — an unmarked file that is not a generated
     create-table file (``add_*`` / ``alter_*`` / ``backfill_*`` / ``fix_*``)
     is a violation. Change the MODEL, ALTER the dev database by hand, regenerate.
  3. MODEL_LESS ESCAPE HATCH — a file marked ``MODEL_LESS = True`` must SAY in
     its docstring why no model can own the object. An unexplained marker is how
     the hatch turns back into a dumping ground.
  4. UTC EVERYWHERE — a naive ``TIMESTAMP`` in hand-written SQL re-creates the
     mixed-awareness cast that makes index expressions non-IMMUTABLE, which is
     what made a from-scratch migrate die in both products.
  5. NO DUPLICATES — two files creating the same table.
  6. APPLIED MODEL TRANSITIONS — an immutable generated creator may be paired
     with one later, explained ``MODEL_TRANSITION = ("old", "new")`` migration
     only when its SQL proves the rename and ``new`` is the exact current model.
  7. INDEXES BELONG TO MODELS — an index that exists only inside a migration
     file is silently DROPPED by the next regenerate-from-models.

(Rule 6, from-scratch installability, is not statically checkable; it is the
acceptance test these rules exist to protect.)

Every file is parsed with ``ast``, never imported — importing a migration can
open a database connection, and a gate must not need one.

``--fix`` takes the ``make:migration --overwrite --force`` path (honouring
MODEL_LESS) and re-audits, printing which files it added and removed. Two
classes of violation are deliberately NOT auto-fixed because they need a human
decision: a naive timestamp inside a hand-written MODEL_LESS file, and a marked
file with no explanation. Both are reported and keep the exit code non-zero.
A third class BLOCKS the fix entirely rather than being carried out — an index
declared only in a migration, a duplicated CREATE TABLE, an unparseable file —
because regenerating would DESTROY the thing the check just found.

Exit code is 0 only when the directory is clean, so CI can gate on it.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

from cara.commands import CommandBase, missing_optional
from cara.commands.core.MakeMigrationCommand import (
    MODEL_LESS_MARKER,
    MODEL_TRANSITION_MARKER,
    MakeMigrationCommand,
)
from cara.decorators import command

# Filenames the generator authors: ``NNNN_01_01_NNNNNN_create_<table>_table.py``.
# The middle segments are vestigial Laravel date padding that keeps the
# lexicographic ordering monotonic (see MakeMigrationCommand) — matched loosely
# on purpose, only the table name is load-bearing here.
_GENERATED_NAME_RE = re.compile(
    r"^\d+_\d+_\d+_\d+_create_(?P<table>[a-z0-9_]+?)_table\.py$"
)

# ``CREATE [UNIQUE] INDEX [CONCURRENTLY] [IF NOT EXISTS] <name> ON <table>`` —
# both halves captured, because rule 7 is about WHICH table's index escaped its
# model. SchemaCheckCommand's sibling regex captures only the name.
_CREATE_INDEX_ON_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?"
    r"(?:IF\s+NOT\s+EXISTS\s+)?\"?(?P<name>\w+)\"?\s+ON\s+"
    r"(?:ONLY\s+)?\"?(?:\w+\"?\.\"?)?(?P<table>\w+)\"?",
    re.IGNORECASE,
)

# ``CREATE TABLE [IF NOT EXISTS] <table>`` in raw SQL — the duplicate-table
# check has to see past filenames, since a MODEL_LESS file can create a table a
# generated file already owns.
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"\"?(?:\w+\"?\.\"?)?(?P<table>\w+)\"?",
    re.IGNORECASE,
)

_ALTER_TABLE_RENAME_RE = re.compile(
    r"ALTER\s+TABLE\s+\"?(?P<old>[a-z0-9_]+)\"?\s+"
    r"RENAME\s+TO\s+\"?(?P<new>[a-z0-9_]+)\"?",
    re.IGNORECASE,
)

# A naive TIMESTAMP. ``CURRENT_TIMESTAMP`` / ``LOCALTIMESTAMP`` / ``to_timestamp``
# are excluded by the lookbehind (they are preceded by a word character),
# ``TIMESTAMPTZ`` and ``TIMESTAMP WITH TIME ZONE`` by the lookaheads. What is
# left — bare ``TIMESTAMP`` and ``TIMESTAMP WITHOUT TIME ZONE`` — is exactly the
# naive declaration rule 4 forbids.
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
    model_less: bool
    generated_table: str | None
    model_transition: tuple[str, str] | None
    transition_error: str | None
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


def _model_transition(
    tree: ast.Module,
) -> tuple[tuple[str, str] | None, str | None]:
    """Read one explicit old-table -> model-table transition marker."""

    marker: ast.AST | None = None
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == MODEL_TRANSITION_MARKER
            for target in node.targets
        ):
            marker = node.value
            break
    if marker is None:
        return None, None
    try:
        value = ast.literal_eval(marker)
    except ValueError, TypeError:
        return None, f"{MODEL_TRANSITION_MARKER} must be a literal pair"
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(
            not isinstance(table, str)
            or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", table) is None
            for table in value
        )
        or value[0] == value[1]
    ):
        return None, (f"{MODEL_TRANSITION_MARKER} must be an (old_table, new_table) pair")
    return (value[0], value[1]), None


def parse_migration_file(path: Path) -> MigrationFile:
    """Classify one migration file by PARSING it. Never imports it."""
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        return MigrationFile(
            path,
            False,
            None,
            None,
            None,
            None,
            (),
            f"unreadable: {exc}",
        )

    try:
        tree = ast.parse(content, filename=str(path))
    except SyntaxError as exc:
        return MigrationFile(
            path,
            False,
            None,
            None,
            None,
            None,
            (),
            f"does not parse: {exc}",
        )

    model_less = MakeMigrationCommand._declares_model_less(content, path)
    match = _GENERATED_NAME_RE.match(path.name)
    # A MODEL_LESS file may legitimately be named create_<table>_table.py (the
    # framework-owned failed_job table is), so the marker is decided FIRST —
    # otherwise the escape hatch would be reported as an orphan every run.
    generated_table = match.group("table") if match and not model_less else None
    model_transition, transition_error = _model_transition(tree)

    return MigrationFile(
        path=path,
        model_less=model_less,
        generated_table=generated_table,
        model_transition=model_transition,
        transition_error=transition_error,
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
    transitions: list[MigrationFile] = []

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

        if entry.transition_error:
            violations.append(
                Violation(
                    rule="invalid-model-transition",
                    path=name,
                    message=entry.transition_error,
                    remedy=(
                        f"declare {MODEL_TRANSITION_MARKER} = ('old_table', 'new_table')"
                    ),
                    human_only=True,
                    blocks_fix=True,
                )
            )
        elif entry.model_less and entry.model_transition:
            violations.append(
                Violation(
                    rule="invalid-model-transition",
                    path=name,
                    message=(
                        f"{MODEL_TRANSITION_MARKER} cannot be combined with "
                        f"{MODEL_LESS_MARKER}"
                    ),
                    remedy="remove the conflicting marker",
                    human_only=True,
                    blocks_fix=True,
                )
            )
        elif entry.model_less:
            violations.extend(_audit_model_less(entry))
        elif entry.generated_table:
            if entry.model_transition:
                violations.append(
                    Violation(
                        rule="invalid-model-transition",
                        path=name,
                        message=(
                            f"{MODEL_TRANSITION_MARKER} belongs in a forward "
                            "non-create migration"
                        ),
                        remedy="move the marker to the forward rename migration",
                        human_only=True,
                        blocks_fix=True,
                    )
                )
            else:
                creators.setdefault(entry.generated_table, set()).add(name)
                generated_files.add(name)
        elif entry.model_transition:
            transitions.append(entry)
        else:
            violations.append(
                Violation(
                    rule="incremental-migration",
                    path=name,
                    message=(
                        "not a generated create_<table>_table.py and not marked "
                        f"{MODEL_LESS_MARKER} = True"
                    ),
                    remedy=(
                        "put the change in the model and regenerate "
                        "(craft make:migration --overwrite --force)"
                    ),
                )
            )

        violations.extend(_audit_indexes(entry, model_indexes))
        for table in _created_tables(entry):
            creators.setdefault(table, set()).add(name)

    transition_violations, valid_transitions = _audit_model_transitions(
        transitions,
        creators,
        generated_files,
        model_indexes,
    )
    violations.extend(transition_violations)
    retired_tables: set[str] = set()
    for _chain, tables, root_creator in valid_transitions:
        retired_tables.update(tables[:-1])
        creators.setdefault(tables[-1], set()).add(root_creator)

    violations.extend(_audit_table_coverage(creators, generated_files, model_indexes))
    violations = [
        violation
        for violation in violations
        if not (
            violation.rule == "orphan-migration"
            and any(
                violation.path in creators.get(table, set()) for table in retired_tables
            )
        )
    ]
    return violations


def _audit_model_transitions(
    entries: list[MigrationFile],
    creators: dict[str, set[str]],
    generated_files: set[str],
    model_indexes: dict[str, set[str]],
) -> tuple[
    list[Violation],
    list[tuple[list[MigrationFile], list[str], str]],
]:
    """Validate applied immutable create -> forward model rename chains."""

    violations: list[Violation] = []
    conflicted: set[str] = set()
    sources: dict[str, list[MigrationFile]] = {}
    targets: dict[str, list[MigrationFile]] = {}
    for entry in entries:
        old_table, new_table = entry.model_transition or ("", "")
        sources.setdefault(old_table, []).append(entry)
        targets.setdefault(new_table, []).append(entry)

    for label, grouped in (("source", sources), ("target", targets)):
        for table, grouped_entries in grouped.items():
            if len(grouped_entries) == 1:
                continue
            for entry in grouped_entries:
                conflicted.add(entry.path.name)
                violations.append(
                    Violation(
                        rule="duplicate-model-transition",
                        path=entry.path.name,
                        message=(
                            f"model-transition {label} table {table!r} is "
                            "declared more than once"
                        ),
                        remedy="keep exactly one forward transition per table",
                        human_only=True,
                        blocks_fix=True,
                    )
                )

    by_source = {
        source: grouped_entries[0]
        for source, grouped_entries in sources.items()
        if len(grouped_entries) == 1 and grouped_entries[0].path.name not in conflicted
    }
    by_target = {
        target: grouped_entries[0]
        for target, grouped_entries in targets.items()
        if len(grouped_entries) == 1 and grouped_entries[0].path.name not in conflicted
    }

    chains: list[tuple[list[MigrationFile], list[str]]] = []
    visited: set[str] = set()
    roots = sorted(set(by_source) - set(by_target))
    for root in roots:
        chain: list[MigrationFile] = []
        tables = [root]
        source = root
        while source in by_source:
            entry = by_source[source]
            name = entry.path.name
            if name in visited:
                break
            visited.add(name)
            chain.append(entry)
            _old_table, new_table = entry.model_transition or ("", "")
            tables.append(new_table)
            source = new_table
        chains.append((chain, tables))

    cyclic = [
        entry
        for entry in entries
        if entry.path.name not in conflicted and entry.path.name not in visited
    ]
    for entry in cyclic:
        conflicted.add(entry.path.name)
        violations.append(
            Violation(
                rule="cyclic-model-transition",
                path=entry.path.name,
                message="MODEL_TRANSITION graph contains a rename cycle",
                remedy="break the cycle; applied table history must form a DAG",
                human_only=True,
                blocks_fix=True,
            )
        )

    valid: list[tuple[list[MigrationFile], list[str], str]] = []
    for chain, tables in chains:
        if not chain:
            continue
        chain_errors: list[tuple[str, str, str]] = []
        root_table, terminal_table = tables[0], tables[-1]
        root_creators = creators.get(root_table, set())
        if len(root_creators) != 1 or not root_creators <= generated_files:
            chain_errors.append(
                (
                    chain[0].path.name,
                    f"chain root {root_table!r} has no single generated creator",
                    "point the root at exactly one applied generated create migration",
                )
            )
        unexpected_creators = {
            table: creators.get(table, set())
            for table in tables[1:]
            if creators.get(table)
        }
        if unexpected_creators:
            chain_errors.append(
                (
                    chain[-1].path.name,
                    "only the chain root may have a generated creator",
                    "remove regenerated creators for transition intermediates/terminal",
                )
            )
        stale_models = sorted(set(tables[:-1]) & set(model_indexes))
        if stale_models:
            chain_errors.append(
                (
                    chain[0].path.name,
                    "non-terminal transition table(s) are still model-owned: "
                    + ", ".join(stale_models),
                    "only the terminal table may be the current model",
                )
            )
        if terminal_table not in model_indexes:
            chain_errors.append(
                (
                    chain[-1].path.name,
                    f"terminal table {terminal_table!r} has no model",
                    "point the chain terminal at the exact current model table",
                )
            )

        previous_name = next(iter(root_creators)) if len(root_creators) == 1 else ""
        for entry in chain:
            name = entry.path.name
            old_table, new_table = entry.model_transition or ("", "")
            if not (entry.docstring or "").strip():
                chain_errors.append(
                    (
                        name,
                        "missing explanation",
                        "add a module docstring explaining why immutable history "
                        "requires this forward transition",
                    )
                )
            rename_pairs = {
                (match.group("old").lower(), match.group("new").lower())
                for _, text in entry.sql_constants
                for match in _ALTER_TABLE_RENAME_RE.finditer(text)
            }
            if rename_pairs != {(old_table, new_table)}:
                chain_errors.append(
                    (
                        name,
                        "SQL does not prove the declared one-to-one table rename",
                        f"include exactly ALTER TABLE {old_table} RENAME TO {new_table}",
                    )
                )
            if _created_tables(entry):
                chain_errors.append(
                    (
                        name,
                        "transition migration creates a table",
                        "rename the applied chain; do not create a second table",
                    )
                )
            if previous_name and name <= previous_name:
                chain_errors.append(
                    (
                        name,
                        "transition does not sort after its predecessor",
                        "give every chain edge a later monotonic sequence number",
                    )
                )
            previous_name = name

        if chain_errors:
            for path, message, remedy in chain_errors:
                violations.append(
                    Violation(
                        rule="invalid-model-transition",
                        path=path,
                        message=message,
                        remedy=remedy,
                        human_only=True,
                        blocks_fix=True,
                    )
                )
            continue
        valid.append((chain, tables, next(iter(root_creators))))
    return violations, valid


def _audit_model_less(entry: MigrationFile) -> list[Violation]:
    """Rules 3 and 4 for a marked file: it must explain itself, and stay UTC."""
    violations: list[Violation] = []
    name = entry.path.name

    if not (entry.docstring or "").strip():
        violations.append(
            Violation(
                rule="unexplained-model-less",
                path=name,
                message=(f"marked {MODEL_LESS_MARKER} = True but carries no docstring"),
                remedy=(
                    "add a docstring stating WHY no model can own this object "
                    "(materialized view, framework-owned table, extension)"
                ),
                human_only=True,
            )
        )

    for lineno, text in entry.sql_constants:
        if _NAIVE_TIMESTAMP_RE.search(text):
            violations.append(
                Violation(
                    rule="naive-timestamp",
                    path=f"{name}:{lineno}",
                    message=(
                        "naive TIMESTAMP in hand-written SQL — mixing naive and "
                        "aware values needs a non-IMMUTABLE cast, so index "
                        "expressions over it cannot build"
                    ),
                    remedy="declare the column TIMESTAMPTZ (UTC everywhere)",
                    human_only=True,
                )
            )
            # One report per file is enough to send a human in; listing every
            # column would bury the other rules in a wall of identical lines.
            break

    return violations


def _audit_indexes(
    entry: MigrationFile, model_indexes: dict[str, set[str]]
) -> list[Violation]:
    """Rule 7: an index on a MODEL table must be declared by that model.

    An index that exists only inside a migration file survives exactly until the
    next regenerate-from-models, which rebuilds the file from the model and
    drops it — silently, because nothing compares the two. Indexes on tables no
    model owns (a MODEL_LESS materialized view) are out of scope by definition.
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
    ``CREATE TABLE``, so this reports what a hand-written or MODEL_LESS file
    creates — the only way a duplicate can slip past the generator's naming.
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
    """Rules 1 and 5: exactly one creating file per model table, no orphans."""
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
            # Only a GENERATED file can be orphaned: a MODEL_LESS file creating
            # a table no model declares is the escape hatch working as intended.
            violations.append(
                Violation(
                    rule="orphan-migration",
                    path=next(iter(files)),
                    message=(
                        f"creates table '{table}', which no model declares "
                        "(model deleted or renamed?)"
                    ),
                    remedy=(
                        "regenerate to drop it, or mark the file "
                        f"{MODEL_LESS_MARKER} = True if no model can own the table"
                    ),
                )
            )

    return violations


@command(
    name="migrations:check",
    help=(
        "Audit the migrations directory against the migration convention: one "
        "generated chain per model table, no undeclared incremental files, "
        "every MODEL_LESS or MODEL_TRANSITION file proven and explained, "
        "TIMESTAMPTZ everywhere, no duplicate or orphan table creations, and "
        "no index living only in a migration. "
        "Exits non-zero on any violation so CI can gate on it."
    ),
    options={
        "--fix": (
            "Regenerate the directory from the models (the make:migration "
            "--overwrite --force path, MODEL_LESS and validated "
            "MODEL_TRANSITION chains preserved) and re-audit. Naive timestamps "
            "and unexplained markers are never "
            "auto-fixed; a hand-added index or duplicate table blocks the fix."
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
