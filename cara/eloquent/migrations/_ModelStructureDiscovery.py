"""Model class structure and schema metadata discovery."""

from __future__ import annotations

import ast
import re
from pathlib import Path

_UNRESOLVED = object()


def _model_discovery_parse_model_file(self, file_path: Path) -> dict | None:
    """Parse model file and extract Field.* structure."""
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    tree = ast.parse(content)

    # Module-level literal constants, so an ``__indexes__`` entry written as
    # an f-string (``f"WHERE status IN ({_STATUS_SQL})"``) resolves to real
    # SQL instead of being skipped. The model file is parsed, never
    # imported, so nothing else can supply these values.
    self._module_constants = {
        target.id: node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
        for target in node.targets
        if isinstance(target, ast.Name)
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and self._is_model_class(node):
            return self._extract_model_structure(node, file_path.stem, str(file_path))

    return None


def _model_discovery_is_model_class(self, class_node: ast.ClassDef) -> bool:
    """Check if class extends Model."""
    for base in class_node.bases:
        if isinstance(base, ast.Name) and base.id == "Model":
            return True
    return False


def _model_discovery_extract_model_structure(
    self, class_node: ast.ClassDef, filename: str, file_path: str | None = None
) -> dict:
    """Extract model structure from AST looking for table attribute and fields() method."""
    model_info = {
        "name": class_node.name,
        "filename": filename,
        "file": file_path,
        "table": None,
        "fields": {},
        "uses_soft_deletes": False,
        "has_fields_method": False,
        # Standalone constraint calls inside ``fields()`` —
        # ``field.unique([...])`` and ``field.index([...])`` —
        # are collected here and emitted as ``table.unique([...])``
        # / ``table.index([...])`` in the generated migration.
        # Without this collection the composite indexes were
        # silently dropped and Postgres rejected later
        # ``ON CONFLICT (col_a, col_b)`` upserts (``no unique or
        # exclusion constraint matching the ON CONFLICT
        # specification``).
        "composite_uniques": [],
        "composite_indexes": [],
        # First-class table CHECK constraints declared as
        # ``field.check("expression", name="...")``. Blueprint already
        # supports this verb; discovery must carry it into generated create
        # migrations instead of forcing models through raw ``__indexes__``
        # ALTER TABLE SQL.
        "checks": [],
        # Multi-column foreign keys declared as
        # ``field.foreign(["a", "b"]).references(["x", "y"]).on("t")``.
        # The local columns are a list (so there is no single ``fields``
        # entry to hang them off, the way scalar FKs do), so they are
        # collected here and emitted as
        # ``table.foreign(["a", "b"]).references(["x", "y"]).on("t")``.
        # Each entry mirrors the scalar ``foreign_key`` shape but with
        # column LISTS: {"columns": [...], "references": [...],
        # "on": str, "on_delete": str|None, "on_update": str|None}.
        "composite_foreign_keys": [],
        # SQL VIEWs that depend on this table. Defined via
        # ``__views__`` on the model class, each entry is a dict
        # with ``name`` (view name) and ``sql`` (CREATE OR REPLACE
        # VIEW ...). The migration generator appends these as
        # DB.statement() calls after the CREATE TABLE block.
        "views": [],
        # Raw-SQL schema objects the Blueprint can't express —
        # partial/expression/GIN indexes, partial UNIQUE indexes (the
        # ON CONFLICT targets), CHECK constraints and GENERATED columns.
        # Defined via ``__indexes__`` on the model class, each entry a
        # dict with ``name``, ``up`` (forward SQL) and ``down`` (rollback).
        # The generator appends ``up`` after CREATE TABLE and ``down`` into
        # down(), keeping the MODEL the single source of truth so
        # ``make:migration --overwrite`` regenerates them.
        "indexes": [],
        # Column renames, DECLARED: ``__renamed_from__ = {"new": "old"}``.
        # A diff cannot tell a rename from a drop plus an add — they are
        # the same two facts — so every tool that guesses emits DROP+ADD
        # and throws the column's data away. Evolve-mode planning reads
        # this map and emits ``RENAME COLUMN`` instead; undeclared, it
        # refuses to guess. Regenerate mode ignores it: a regenerated
        # creator only ever states the CURRENT name.
        "renamed_from": {},
    }

    # Check if model uses MakesSoftDeletes
    for base in class_node.bases:
        if isinstance(base, ast.Name) and base.id == "MakesSoftDeletes":
            model_info["uses_soft_deletes"] = True

    # Class-level literal constants (``STATUS_PENDING = "pending"``, …) so a
    # ``field.string(...).default(self.STATUS_PENDING)`` schema default
    # resolves to its LITERAL value in the generated migration. The
    # migration is a standalone class with no such attribute, so emitting
    # ``self.STATUS_PENDING`` verbatim raised AttributeError on up().
    self._class_constants = {
        target.id: node.value.value
        for node in class_node.body
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    self._class_name = class_node.name

    for node in class_node.body:
        if isinstance(node, ast.Assign):
            self._parse_model_attribute(node, model_info)
        elif isinstance(node, ast.FunctionDef) and node.name == "fields":
            model_info["has_fields_method"] = True
            self._parse_fields_method(node, model_info)

    # Set default table name if not specified
    if not model_info["table"]:
        model_info["table"] = self._snake_case(class_node.name)

    # Note: Runtime field processing removed to avoid database connection issues
    # AST parsing provides sufficient field information for migration generation

    return model_info


def _model_discovery_parse_model_attribute(
    self, assign_node: ast.Assign, model_info: dict
):
    """Parse model class attributes looking for __columns__ dict, __table__, and other special attributes."""
    for target in assign_node.targets:
        if isinstance(target, ast.Name):
            # Parse __table__ attribute
            if target.id == "__table__" and isinstance(assign_node.value, ast.Constant):
                model_info["table"] = assign_node.value.value

            # Parse __columns__ = {...} dict (old syntax)
            elif target.id == "__columns__" and isinstance(assign_node.value, ast.Dict):
                self._parse_fields_dict(assign_node.value, model_info)

            # Parse __fillable__ = [...] (future feature)
            elif target.id == "__fillable__" and isinstance(assign_node.value, ast.List):
                fillable = []
                for element in assign_node.value.elts:
                    if isinstance(element, ast.Constant):
                        fillable.append(element.value)
                model_info["fillable"] = fillable

            # Parse __guarded__ = [...] (future feature)
            elif target.id == "__guarded__" and isinstance(assign_node.value, ast.List):
                guarded = []
                for element in assign_node.value.elts:
                    if isinstance(element, ast.Constant):
                        guarded.append(element.value)
                model_info["guarded"] = guarded

            # Parse __primary_key__ = "id" (future feature)
            elif target.id == "__primary_key__" and isinstance(
                assign_node.value, ast.Constant
            ):
                model_info["primary_key"] = assign_node.value.value

            # Parse __connection__ = "database_name" (future feature)
            elif target.id == "__connection__" and isinstance(
                assign_node.value, ast.Constant
            ):
                model_info["connection"] = assign_node.value.value

            # Parse __timestamps__ = False (future feature)
            elif target.id == "__timestamps__" and isinstance(
                assign_node.value, ast.Constant
            ):
                model_info["timestamps"] = assign_node.value.value

            # Parse __views__ = [{"name": "...", "sql": "..."}]
            elif target.id == "__views__" and isinstance(assign_node.value, ast.List):
                model_info["views"] = self._parse_views_attribute(assign_node.value)

            # Parse __indexes__ = [{"name": "...", "up": "...", "down": "..."}]
            elif target.id == "__indexes__" and isinstance(assign_node.value, ast.List):
                model_info["indexes"] = self._parse_indexes_attribute(assign_node.value)

            # Parse __renamed_from__ = {"new_name": "old_name"}
            elif target.id == "__renamed_from__" and isinstance(
                assign_node.value, ast.Dict
            ):
                model_info["renamed_from"] = self._parse_renamed_from_attribute(
                    assign_node.value
                )


def _model_discovery_parse_fields_dict(self, dict_node: ast.Dict, model_info: dict):
    """Parse __columns__ = {...} dictionary and extract Field.* definitions."""
    for key, value in zip(dict_node.keys, dict_node.values, strict=False):
        if isinstance(key, ast.Constant) and isinstance(value, ast.Call):
            field_name = key.value
            field_definition = self._extract_field_definition(value)
            if field_definition:
                model_info["fields"][field_name] = field_definition


def _model_discovery_parse_fields_method(
    self, method_node: ast.FunctionDef, model_info: dict
):
    """Parse fields() method that returns Schema.build(lambda field: (...)) or dict with up/down."""
    for stmt in method_node.body:
        if isinstance(stmt, ast.Return):
            # Check if it returns a dict with 'up' and 'down' keys (raw SQL)
            if isinstance(stmt.value, ast.Dict):
                self._parse_raw_sql_fields(stmt.value, model_info)
            # Check if it's Schema.build(lambda field: (...))
            elif (
                isinstance(stmt.value, ast.Call)
                and isinstance(stmt.value.func, ast.Attribute)
                and isinstance(stmt.value.func.value, ast.Name)
                and stmt.value.func.value.id == "Schema"
                and stmt.value.func.attr == "build"
                and stmt.value.args
                and isinstance(stmt.value.args[0], ast.Lambda)
            ):
                self._parse_lambda_fields(stmt.value.args[0], model_info)


def _model_discovery_snake_case(self, camel_str: str) -> str:
    """Convert CamelCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", camel_str)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _model_discovery_parse_views_attribute(self, list_node: ast.List) -> list:
    """Parse ``__views__ = [{"name": "...", "sql": "..."}]`` from AST."""
    views = []
    for elt in list_node.elts:
        if not isinstance(elt, ast.Dict):
            continue
        view_entry: dict[str, str] = {}
        for key, value in zip(elt.keys, elt.values, strict=False):
            if isinstance(key, ast.Constant) and isinstance(value, ast.Constant):
                view_entry[key.value] = value.value
        if view_entry.get("name") and view_entry.get("sql"):
            views.append(view_entry)
    return views


def _model_discovery_parse_renamed_from_attribute(self, dict_node: ast.Dict) -> dict:
    """Parse ``__renamed_from__ = {"new": "old"}`` from AST.

    Both sides must be string literals: the map is read by a planner that
    writes ``RENAME COLUMN`` against a production table, so a computed key
    is refused rather than resolved. A non-literal entry is skipped and
    the rename simply is not declared — which makes the planner fall back
    to reporting a drop and an add for a human to judge, the safe default.
    """
    renames: dict[str, str] = {}
    for key, value in zip(dict_node.keys, dict_node.values, strict=False):
        if (
            isinstance(key, ast.Constant)
            and isinstance(key.value, str)
            and isinstance(value, ast.Constant)
            and isinstance(value.value, str)
        ):
            renames[key.value] = value.value
    return renames


def _model_discovery_parse_indexes_attribute(self, list_node: ast.List) -> list:
    """Parse ``__indexes__ = [{"name": .., "up": .., "down": ..}]`` from AST.

    ``up`` is the forward SQL (CREATE [UNIQUE] INDEX …, ALTER TABLE … ADD
    CONSTRAINT/COLUMN …). ``down`` is optional — when omitted it defaults to
    ``DROP INDEX IF EXISTS <name>`` (the common case). Adjacent string
    literals are merged by the parser, so multi-line SQL written as
    implicitly-concatenated strings arrives here as a single constant.
    """
    indexes = []
    for elt in list_node.elts:
        if not isinstance(elt, ast.Dict):
            continue
        entry: dict[str, str] = {}
        for key, value in zip(elt.keys, elt.values, strict=False):
            if not isinstance(key, ast.Constant):
                continue
            resolved = self._literal_sql(value)
            if resolved is not _UNRESOLVED:
                entry[key.value] = resolved
        name = entry.get("name")
        up = entry.get("up")
        if not name and not up:
            continue
        # A named entry whose SQL will not resolve is FATAL, never skipped:
        # a silently dropped __indexes__ entry means the generator emits no
        # DDL, schema:check has nothing to compare, and the index simply
        # never exists while every gate stays green.
        if not up:
            raise RuntimeError(
                f"__indexes__ entry '{name}' has no resolvable 'up' SQL. "
                "Interpolations must be module- or class-level string "
                "constants — the model is parsed, never imported."
            )
        if not name:
            raise RuntimeError(
                f"__indexes__ entry with SQL {up!r} has no resolvable 'name'."
            )
        down = entry.get("down") or f"DROP INDEX IF EXISTS {name}"
        indexes.append({"name": name, "up": up, "down": down})
    return indexes


def _model_discovery_literal_sql(self, node: ast.AST):
    """Resolve a string node to its literal value, or ``_UNRESOLVED``.

    Handles plain constants and f-strings whose interpolations are names of
    module- or class-level string constants. Implicitly-concatenated string
    literals are already merged by the parser; a concatenation that mixes in
    an f-string arrives here as a single ``JoinedStr``.
    """
    if isinstance(node, ast.Constant):
        return node.value if isinstance(node.value, str) else _UNRESOLVED

    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
                continue
            if not isinstance(value, ast.FormattedValue):
                return _UNRESOLVED
            resolved = self._resolve_name(value.value)
            if not isinstance(resolved, str):
                return _UNRESOLVED
            parts.append(resolved)
        return "".join(parts)

    return _UNRESOLVED


def _model_discovery_resolve_name(self, node: ast.AST):
    """Look a bare name or ``self.NAME`` up in the class/module constants."""
    if isinstance(node, ast.Name):
        constants = getattr(self, "_module_constants", {})
        if node.id in constants:
            return constants[node.id]
        return getattr(self, "_class_constants", {}).get(node.id, _UNRESOLVED)
    if isinstance(node, ast.Attribute):
        return getattr(self, "_class_constants", {}).get(node.attr, _UNRESOLVED)
    return _UNRESOLVED


def _model_discovery_parse_raw_sql_fields(self, dict_node: ast.Dict, model_info: dict):
    """Parse fields() method that returns {'up': function, 'down': function}."""
    has_up_function = False
    has_down_function = False

    for key, value in zip(dict_node.keys, dict_node.values, strict=False):
        if (
            isinstance(key, ast.Constant)
            and key.value in ["up", "down"]
            and isinstance(value, ast.Name)
        ):
            if key.value == "up":
                has_up_function = True
            elif key.value == "down":
                has_down_function = True

    if has_up_function:
        model_info["has_raw_sql"] = True
        model_info["has_up_function"] = True
        model_info["has_down_function"] = has_down_function

        # Extract SQL dependencies from the up() function (will be called later with all_known_tables)
        model_info["needs_sql_dependency_resolution"] = True
