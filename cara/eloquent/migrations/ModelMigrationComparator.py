"""ModelMigrationComparator — structured schema-snapshot differ.

The OLD comparator diffed by column NAME only and emitted English strings
("Added field: x"), so a column's TYPE / length / nullability / default / unique
CHANGE was silently lost, a RENAME became drop+add (data loss), and a removed
column's down() could only ever be re-created as ``table.string`` (lossy
rollback).

This version parses BOTH sides — the model (from ``model_info``) and the
existing migration files — into structured ``Column`` snapshots, then emits a
list of TYPED :class:`FieldDiff` changes:

* ``added``   — a column in the model, absent from the migration.
* ``removed`` — a column in the migration, absent from the model. Carries the
                column's REAL parsed definition so ``down()`` can recreate it
                losslessly (the right type/length/nullable/default), not a bare
                varchar.
* ``altered`` — a column on BOTH sides whose type/length/precision/scale/
                nullable/unique differs. Carries old + new so the generator can
                ``change_column`` and reverse it.
* ``renamed`` — exactly one removed + one added column with an identical parsed
                definition → a rename, NOT a destructive drop+add.

The generator consumes :class:`FieldDiff` objects (``kind`` discriminator); the
legacy string protocol is gone. ``table_exists_in_migrations`` is preserved.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from cara.support import paths

from .FieldDiff import FieldDiff
from .MigrationColumn import _COMPARED_ATTRS, MigrationColumn

_logger = logging.getLogger("cara.migrations.comparator")

# Framework-managed columns / helper field-types. The model's ``Schema.build``
# emits these via ``big_increments()`` / ``timestamps()`` / ``soft_deletes()``,
# so ModelDiscoverer records pseudo-fields named ``id`` / ``timestamps`` /
# ``soft_deletes``; the migration parser expands ``timestamps()`` into
# created_at + updated_at (soft_deletes → deleted_at). Excluding these on BOTH
# sides keeps the diff to real, operator-defined columns (without it every table
# looked like it was "missing" id/timestamps and got a spurious drop_column).
_FRAMEWORK_FIELDS = frozenset(
    {"id", "created_at", "updated_at", "deleted_at", "timestamps", "soft_deletes"}
)
_FRAMEWORK_TYPES = frozenset(
    {"increments", "big_increments", "timestamps", "soft_deletes"}
)


def _method_body_text(content: str, which: str) -> str:
    if which == "up":
        match = re.search(
            r"def up\(self\):(.*?)(?:\n    def down\(self\):|$)",
            content,
            re.DOTALL,
        )
    else:
        match = re.search(r"def down\(self\):(.*?)$", content, re.DOTALL)
    return match.group(1) if match else ""


def migration_table_actions(content: str, table_name: str) -> tuple[bool, bool]:
    """Return whether ``up()`` creates and/or alters ``table_name``.

    File names are intentionally ignored. Generated intent names include
    ``add_*_to``, ``drop_*_from``, ``change_*_on`` and ``rename_*_on``; using
    only create/update globs made those migrations invisible on the next run.
    """
    up = _method_body_text(content, "up")
    quoted = rf"[\"']{re.escape(table_name)}[\"']"
    creates = bool(
        re.search(
            rf"\bself\.schema\.create(?:_table_if_not_exists)?\(\s*{quoted}\s*\)",
            up,
        )
    )
    alters = bool(re.search(rf"\bself\.schema\.table\(\s*{quoted}\s*\)", up))
    return creates, alters


def summarize_change_name(table: str, diffs: list[FieldDiff]) -> tuple[str, str]:
    """Derive an INTENT-revealing migration file name + class name from the
    change set (Laravel/Django convention) instead of a generic
    ``update_<table>_table``:

      * one added column     → add_<col>_to_<table>_table / Add<Col>To<Table>
      * one dropped column    → drop_<col>_from_<table>_table / Drop<Col>From<Table>
      * one renamed column    → rename_<old>_to_<new>_on_<table>_table
      * one altered column    → change_<col>_on_<table>_table
      * mixed / many          → update_<table>_table (the generic fallback)
    """

    def camel(s: str) -> str:
        return "".join(p.capitalize() for p in s.split("_"))

    tbl = camel(table)
    if len(diffs) == 1:
        d = diffs[0]
        if d.kind == "added":
            return f"add_{d.name}_to_{table}_table", f"Add{camel(d.name)}To{tbl}"
        if d.kind == "removed":
            return f"drop_{d.name}_from_{table}_table", f"Drop{camel(d.name)}From{tbl}"
        if d.kind == "renamed":
            return (
                f"rename_{d.old_name}_to_{d.name}_on_{table}_table",
                f"Rename{camel(d.old_name)}To{camel(d.name)}On{tbl}",
            )
        if d.kind == "altered":
            return f"change_{d.name}_on_{table}_table", f"Change{camel(d.name)}On{tbl}"
    # A pure batch of adds reads well as add_columns_to_<table>.
    if diffs and all(d.kind == "added" for d in diffs):
        return f"add_columns_to_{table}_table", f"AddColumnsTo{tbl}"
    return f"update_{table}_table", f"Update{tbl}Table"


class ModelMigrationComparator:
    """Diff a model's declared schema against its existing migration files."""

    def __init__(self):
        self.migrations_dir = Path(paths("migrations"))

    # ── Public API ──────────────────────────────────────────────────────

    def compare_model_with_migrations(self, model_info: dict) -> list[FieldDiff]:
        """Return the TYPED schema changes between the model and its migrations.

        Empty list ⇒ no change. When the table does not yet exist in any
        migration, every model column is an ``added`` diff (the caller emits a
        CREATE migration, not an ALTER — it keys on table existence separately).
        """
        table_name = model_info["table"]
        model_cols = self._model_columns(model_info)
        migration_cols, table_exists = self._migration_columns(table_name)

        if not table_exists:
            return [FieldDiff("added", name, column=c) for name, c in model_cols.items()]

        return self._diff(model_cols, migration_cols)

    def table_exists_in_migrations(self, table_name: str) -> bool:
        _, exists = self._migration_columns(table_name)
        return exists

    # ── The diff ────────────────────────────────────────────────────────

    def _diff(
        self,
        model_cols: dict[str, MigrationColumn],
        migration_cols: dict[str, MigrationColumn],
    ) -> list[FieldDiff]:
        # Framework columns (id/created_at/updated_at/deleted_at) are excluded
        # on BOTH sides: the model side records them as pseudo-fields
        # (timestamps/soft_deletes) while the migration parser expands
        # timestamps() → created_at+updated_at, so without this filter every
        # timestamped table shows a spurious created_at/updated_at drop.
        added_names = [
            n
            for n in model_cols
            if n not in migration_cols and n not in _FRAMEWORK_FIELDS
        ]
        removed_names = [
            n
            for n in migration_cols
            if n not in model_cols and n not in _FRAMEWORK_FIELDS
        ]

        diffs: list[FieldDiff] = []

        # RENAME heuristic: exactly one added + one removed whose parsed
        # definitions are identical (same type/length/nullable/unique/…) is far
        # more likely a rename than an unrelated drop+add — emit a lossless
        # RENAME instead of a destructive drop + a fresh add.
        if (
            len(added_names) == 1
            and len(removed_names) == 1
            and model_cols[added_names[0]].signature()
            == migration_cols[removed_names[0]].signature()
        ):
            old = migration_cols[removed_names[0]]
            new = model_cols[added_names[0]]
            diffs.append(
                FieldDiff("renamed", new.name, column=new, old=old, old_name=old.name)
            )
            added_names, removed_names = [], []

        for name in added_names:
            diffs.append(FieldDiff("added", name, column=model_cols[name]))

        # Removed carries the migration's REAL definition for a lossless down().
        for name in removed_names:
            diffs.append(FieldDiff("removed", name, column=migration_cols[name]))

        # ALTER: same name on both sides, but a compared attribute differs.
        for name in model_cols:
            if name in migration_cols:
                new, old = model_cols[name], migration_cols[name]
                changed = [
                    a for a in _COMPARED_ATTRS if getattr(new, a) != getattr(old, a)
                ]
                if new.default_signature() != old.default_signature():
                    changed.append("default")
                if changed:
                    diffs.append(
                        FieldDiff(
                            "altered", name, column=new, old=old, changed_attrs=changed
                        )
                    )
        return diffs

    # ── Model side ──────────────────────────────────────────────────────

    def _model_columns(self, model_info: dict) -> dict[str, MigrationColumn]:
        cols: dict[str, MigrationColumn] = {}
        for name, info in model_info.get("fields", {}).items():
            ftype = info.get("type", "string")
            if name in _FRAMEWORK_FIELDS or ftype in _FRAMEWORK_TYPES:
                continue
            params = info.get("params", {}) or {}
            cols[name] = MigrationColumn(
                name=name,
                type=ftype,
                length=params.get("length"),
                precision=params.get("precision"),
                scale=params.get("scale"),
                nullable=bool(params.get("nullable", False)),
                unique=bool(params.get("unique", False)),
                index=bool(params.get("index", False)),
                default=params.get("default"),
                has_default="default" in params,
            )

        # Standalone one-column declarations live outside field params:
        # ``field.index("email")`` / ``field.unique(["sid"])`` become
        # ``table.index(["email"])`` / ``table.unique(["sid"])`` in the
        # generated migration. Project them back onto the column snapshot so
        # overwrite followed by dry-run is genuinely idempotent.
        for declaration in model_info.get("composite_indexes", []):
            names = declaration["columns"]
            if len(names) == 1 and names[0] in cols:
                cols[names[0]].index = True
        for declaration in model_info.get("composite_uniques", []):
            names = declaration["columns"]
            if len(names) == 1 and names[0] in cols:
                cols[names[0]].unique = True
        return cols

    # ── Migration side ──────────────────────────────────────────────────

    def _migration_columns(
        self, table_name: str
    ) -> tuple[dict[str, MigrationColumn], bool]:
        cols: dict[str, MigrationColumn] = {}
        exists = False
        if not self.migrations_dir.exists():
            return cols, exists

        # Parse every migration in chronological order and select by the
        # actual schema operation in up(), not by a fragile filename glob.
        for mf in sorted(self.migrations_dir.glob("*.py"), key=lambda p: p.name):
            try:
                content = mf.read_text(encoding="utf-8")
            except OSError:
                _logger.warning("unreadable migration file: %s", mf)
                continue
            creates, alters = migration_table_actions(content, table_name)
            if creates:
                exists = True
                self._apply_create(content, cols)
            if alters:
                self._apply_update(content, cols)
        return cols, exists

    def _apply_create(self, content: str, cols: dict[str, MigrationColumn]) -> None:
        up = self._method_body(content, "up")
        for line in self._blueprint_column_lines(up):
            col = self._parse_column_line(line)
            if col and col.name not in _FRAMEWORK_FIELDS:
                cols[col.name] = col
        self._apply_standalone_indexes(up, cols)
        if "table.timestamps()" in content:
            cols.setdefault("created_at", MigrationColumn("created_at", "datetime"))
            cols.setdefault("updated_at", MigrationColumn("updated_at", "datetime"))

    def _apply_update(self, content: str, cols: dict[str, MigrationColumn]) -> None:
        up = self._method_body(content, "up")
        # Adds (and the modern ``change_column`` / ``rename_column`` ALTERs).
        for line in self._blueprint_column_lines(up):
            col = self._parse_column_line(line)
            if col and col.name not in _FRAMEWORK_FIELDS:
                cols[col.name] = col
        self._apply_standalone_indexes(up, cols)
        for old, new in re.findall(
            r'table\.(?:rename_column|rename)\(\s*["\'](\w+)["\']\s*,\s*["\'](\w+)["\']',
            up,
        ):
            if old in cols:
                renamed = cols.pop(old)
                renamed.name = new
                cols[new] = renamed
        for dropped in re.findall(r'table\.drop_column\(\s*["\'](\w+)["\']', up):
            cols.pop(dropped, None)

    # ── Line parsing ────────────────────────────────────────────────────

    @staticmethod
    def _method_body(content: str, which: str) -> str:
        return _method_body_text(content, which)

    @staticmethod
    def _apply_standalone_indexes(body: str, cols: dict[str, MigrationColumn]) -> None:
        """Apply scalar/list-of-one table.index/unique declarations.

        Only the COLUMN argument decides which column the flag lands on — a
        ``name="..."`` kwarg is the index's identifier, not a column. The old
        string-scrape swept the kwarg's value into the name list, so every
        NAMED single-column index looked multi-column and was skipped, and the
        model side (which does project named single-column declarations)
        reported a phantom ``index`` diff on every regenerate.
        """
        for method, raw in re.findall(r"table\.(index|unique)\(([^\n)]*)\)", body):
            columns_part = re.sub(r"name\s*=\s*[\"'][^\"']*[\"']", "", raw)
            names = re.findall(r"[\"'](\w+)[\"']", columns_part)
            if len(names) != 1 or names[0] not in cols:
                continue
            if method == "index":
                cols[names[0]].index = True
            else:
                cols[names[0]].unique = True

    @staticmethod
    def _blueprint_column_lines(body: str) -> list[str]:
        """The ``table.<type>("name", ...)...`` column lines — NOT drop_column /
        rename_column / index / unique / primary / foreign (those are handled
        separately).

        ``primary`` matters: a natural-key table declares its key as a real
        column plus a standalone ``table.primary("job_id")``. Parsed as a
        column line, that second statement REPLACED the real ``string(64)``
        snapshot with a phantom ``primary``-typed column, and every compare
        reported a type/length diff the regenerate could never satisfy.
        """
        out = []
        for raw in body.split("\n"):
            line = raw.strip()
            if not line.startswith("table."):
                continue
            method = re.match(r"table\.(\w+)\(", line)
            if not method:
                continue
            if method.group(1) in (
                "drop_column",
                "rename_column",
                "rename",
                "index",
                "unique",
                "primary",
                "foreign",
                "drop_index",
                "drop_unique",
                "drop_primary",
                "drop_foreign",
            ):
                continue
            out.append(line)
        return out

    @staticmethod
    def _parse_column_line(line: str) -> MigrationColumn | None:
        """Parse ``table.string("x", 50).nullable().default("y").unique()`` into a
        structured :class:`MigrationColumn` (keeps the verbatim line for lossless down)."""
        head = re.match(r'table\.(\w+)\(\s*["\'](\w+)["\']\s*(.*)', line)
        if not head:
            return None
        method, name, rest = head.group(1), head.group(2), head.group(3)
        col = MigrationColumn(name=name, type=method, raw_line=line)

        # Positional size args before the first ``)`` of the type call.
        size_args = re.match(r",?\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", rest)
        if size_args:
            a, b = size_args.group(1), size_args.group(2)
            if method in ("decimal", "double", "float") and b is not None:
                col.precision, col.scale = int(a), int(b)
            else:
                col.length = int(a)

        col.nullable = ".nullable()" in line
        col.unique = ".unique()" in line
        col.index = ".index(" in line or ".index()" in line
        dflt = re.search(r"\.default\(([^)]*)\)", line)
        if dflt:
            col.has_default = True
            col.default = dflt.group(1).strip()
        return col
