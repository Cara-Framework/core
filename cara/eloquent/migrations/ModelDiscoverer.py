"""
ModelDiscoverer: Discover and parse model files.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from cara.eloquent.schema.Schema import FIELD_TYPES_WITH_NAMES as _FIELD_TYPES_WITH_NAMES
from cara.eloquent.schema.Schema import (
    FIELD_TYPES_WITHOUT_NAMES as _FIELD_TYPES_WITHOUT_NAMES,
)
from cara.facades import Log
from cara.support import paths

from . import (
    _ModelDependencyDiscovery,
    _ModelFieldDiscovery,
    _ModelStructureDiscovery,
    _SeparateForeignKeyDiscovery,
)

_logger = logging.getLogger("cara.migrations.discoverer")

# Sentinel: a ``self.CONSTANT`` default that has no resolvable class-level
# literal (distinct from a legitimate ``None`` default value).

_RAW_REFERENCES_RE = re.compile(
    r'\bREFERENCES\s+(?:"?[A-Za-z_][A-Za-z0-9_]*"?\.)?'
    r'"?(?P<table>[A-Za-z_][A-Za-z0-9_]*)"?',
    re.IGNORECASE,
)


class ModelDiscoverer:
    """Discover model files and extract Field.* definitions."""

    # The legal field vocabulary is owned by ``FieldBuilder`` (the object a
    # model's ``fields`` property is handed) and is READ here, never restated.
    # The list used to be hand-copied, and every omission erased columns in
    # silence: no ``jsonb`` entry deleted the ``metadata`` column from ~10
    # tables, and ``char``/``binary`` were still missing — a
    # ``field.char("currency_code", 3)`` produced no field definition at all,
    # so ``make:migration`` wrote the table without the column,
    # ``migrations:check`` compared the same blind view and stayed green, and
    # ``schema:check`` reported the live column as undeclared drift.
    FIELD_TYPES_WITH_NAMES = _FIELD_TYPES_WITH_NAMES
    FIELD_TYPES_WITHOUT_NAMES = _FIELD_TYPES_WITHOUT_NAMES

    # Migration history may intentionally pin a tiny ``Model`` subclass so a
    # data transition can keep using the old row shape. It is executable
    # history, never a canonical model source, and must not enter discovery.
    DISCOVERY_EXCLUDED_DIRECTORIES = {
        "migrations",
        "tests",
        "test",
        "venv",
        "__pycache__",
        "node_modules",
        "build",
        "dist",
        ".git",
    }

    # Implicit ``*_id`` inference is only safe for ID-shaped columns. External
    # string identifiers such as ``notification_id`` may share a table prefix
    # without being relational keys.
    IMPLICIT_FOREIGN_KEY_TYPES = {
        "tiny_integer",
        "small_integer",
        "medium_integer",
        "integer",
        "big_integer",
        "unsigned_integer",
        "unsigned_big_integer",
        "increments",
        "big_increments",
        "id",
    }

    def __init__(self):
        # Don't resolve path at init time - do it at runtime when needed
        self.models_dir = None

    def discover_models(self) -> list[dict]:
        """Discover all model files by scanning for classes that inherit from Model"""
        models = []

        # Get project root
        project_root = Path(paths("")).parent if paths("") else Path.cwd()

        # Scan project root with max 5 levels deep
        models.extend(self._scan_path_for_models(project_root, max_depth=5))

        # Fail closed on duplicate identities. Keeping whichever file happened
        # to be scanned first lets one schema silently overwrite another in the
        # table-keyed dependency graph.
        seen_names: dict[str, str] = {}
        seen_tables: dict[str, str] = {}
        seen_files: set[str] = set()
        unique_models = []
        for model in models:
            # Exclude models from within Cara framework
            model_file = model.get("file", "")
            if "/cara/" in model_file and (
                "eloquent" in model_file or "queues" in model_file
            ):
                continue
            # Local workspaces expose commons through a symlink while the
            # monorepo root also contains its real directory. They are the
            # same source file, not two model declarations.
            resolved_file = str(Path(model_file).resolve())
            if resolved_file in seen_files:
                continue
            seen_files.add(resolved_file)
            name = model["name"]
            table = model["table"]
            if name in seen_names:
                raise RuntimeError(
                    f"Duplicate model class {name!r}: {seen_names[name]} and {model_file}"
                )
            if table in seen_tables:
                raise RuntimeError(
                    f"Duplicate model table {table!r}: {seen_tables[table]} and {model_file}"
                )
            seen_names[name] = model_file
            seen_tables[table] = model_file
            unique_models.append(model)

        return unique_models

    def _scan_path_for_models(
        self, path: Path, max_depth: int = 5, current_depth: int = 0
    ) -> list[dict]:
        """Recursively scan a path for model files with depth limit"""
        models = []

        # Stop if we've reached max depth
        if current_depth >= max_depth:
            return models

        try:
            # Sort by name so discovery order is deterministic run-to-run.
            # ``iterdir()`` yields filesystem order, which varies across
            # machines/runs and leaked into the FK-respecting topological
            # sort below — producing unstable migration file sequence
            # numbers for the same set of models.
            for item in sorted(path.iterdir(), key=lambda p: p.name):
                # Skip hidden directories, venv, __pycache__, .git, etc.
                if (
                    item.name.startswith(".")
                    or item.name in self.DISCOVERY_EXCLUDED_DIRECTORIES
                ):
                    continue

                if item.is_dir():
                    # Recursively scan subdirectories
                    models.extend(
                        self._scan_path_for_models(item, max_depth, current_depth + 1)
                    )
                elif item.is_file() and item.suffix == ".py":
                    # Skip __init__.py and test files
                    if (
                        item.name.startswith("__")
                        or item.name.startswith("test_")
                        or item.name.endswith("_test.py")
                    ):
                        continue

                    try:
                        model_info = self._parse_model_file(item)
                        if model_info:
                            # Add file path to model info
                            model_info["file"] = str(item)
                            models.append(model_info)
                    except Exception as e:
                        # Skip files that can't be parsed, but make it
                        # visible: a syntax-broken model silently excluded
                        # here would just vanish from migration generation.
                        self._warn_unparseable_model(item, e)
                        continue
        except PermissionError:
            # Skip directories we can't read
            pass

        return models

    def _warn_unparseable_model(self, file_path: Path, error: Exception) -> None:
        """Log (don't swallow) a model file that failed to parse.

        Keeps the ``continue`` so one broken file doesn't abort the whole
        discovery run, but names the file + exception so a syntax-broken
        model excluded from migration generation is visible.
        """
        try:
            Log.warning(
                "Skipping unparseable model file %s: %s",
                file_path,
                error,
                category="cara.eloquent.migrations",
            )
        except Exception:
            _logger.warning("Skipping unparseable model file %s: %s", file_path, error)

    def resolve_dependency_order(self, models: list[dict]) -> list[dict]:
        """Resolve dependency order for models (FK dependencies first)."""

        # First pass: Resolve SQL dependencies for raw SQL models
        all_table_names = [model["table"] for model in models]

        for model in models:
            if model.get("needs_sql_dependency_resolution"):
                self._extract_raw_sql_dependencies(model, all_table_names)

        # Second pass: Build dependency graph
        dependency_graph = {}

        for model in models:
            table_name = model["table"]
            dependencies = []
            foreign_keys = []  # Track foreign keys for this model

            # Check for foreign key fields
            for field_name, field_info in model["fields"].items():
                # Check for explicit foreign key info from fluent API
                foreign_key_info = field_info.get("foreign_key")
                if foreign_key_info and foreign_key_info.get("on"):
                    referenced_table = foreign_key_info["on"]
                    dependencies.append(referenced_table)
                    foreign_keys.append(
                        {
                            "field": field_name,
                            "references_table": referenced_table,
                            "references_field": foreign_key_info.get("references", "id"),
                            "on_delete": foreign_key_info.get("on_delete", "RESTRICT"),
                        }
                    )
                # Fallback to old detection method. ``all_table_names`` is
                # threaded in so a ``*_id`` column is only treated as a FK
                # when its resolved target is an ACTUAL known table —
                # otherwise ``public_id`` would invent a phantom FK to a
                # non-existent ``public`` table (and merged_into_brand_id
                # would point at the wrong table).
                elif self._is_foreign_key_field(field_name, field_info, all_table_names):
                    referenced_table = self._extract_referenced_table(
                        field_name, field_info, all_table_names
                    )
                    if referenced_table:
                        dependencies.append(referenced_table)
                        foreign_keys.append(
                            {
                                "field": field_name,
                                "references_table": referenced_table,
                                "references_field": "id",
                                "on_delete": "SET NULL"
                                if field_info.get("params", {}).get("nullable", False)
                                else "RESTRICT",
                            }
                        )

            # Composite FKs declared via ``field.foreign([...]).on("t")`` live
            # in their own collection (no single ``fields`` entry), so register
            # their referenced table here too — otherwise the CREATE TABLE that
            # adds the composite constraint could be ordered before its target
            # table exists.
            for composite_fk in model.get("composite_foreign_keys", []):
                referenced_table = composite_fk.get("on")
                if referenced_table:
                    dependencies.append(referenced_table)
                    foreign_keys.append(
                        {
                            "field": composite_fk["columns"],
                            "references_table": referenced_table,
                            "references_field": composite_fk.get("references"),
                            "on_delete": composite_fk.get("on_delete") or "RESTRICT",
                        }
                    )

            # Raw constraints in ``__indexes__`` are emitted after CREATE
            # TABLE, but their referenced table must still precede this model.
            # This matters when the raw composite FK is the only relationship.
            for index in model.get("indexes", []):
                for match in _RAW_REFERENCES_RE.finditer(index.get("up", "")):
                    referenced_table = match.group("table")
                    if referenced_table in all_table_names:
                        dependencies.append(referenced_table)

            dependency_graph[table_name] = dependencies
            model["foreign_keys"] = foreign_keys

        # Perform topological sort
        sorted_models = self._topological_sort(models, dependency_graph)

        return sorted_models

    _extract_model_structure = (
        _ModelStructureDiscovery._model_discovery_extract_model_structure
    )
    _is_model_class = _ModelStructureDiscovery._model_discovery_is_model_class
    _literal_sql = _ModelStructureDiscovery._model_discovery_literal_sql
    _parse_fields_dict = _ModelStructureDiscovery._model_discovery_parse_fields_dict
    _parse_fields_method = _ModelStructureDiscovery._model_discovery_parse_fields_method
    _parse_indexes_attribute = (
        _ModelStructureDiscovery._model_discovery_parse_indexes_attribute
    )
    _parse_model_attribute = (
        _ModelStructureDiscovery._model_discovery_parse_model_attribute
    )
    _parse_model_file = _ModelStructureDiscovery._model_discovery_parse_model_file
    _parse_raw_sql_fields = _ModelStructureDiscovery._model_discovery_parse_raw_sql_fields
    _parse_renamed_from_attribute = (
        _ModelStructureDiscovery._model_discovery_parse_renamed_from_attribute
    )
    _parse_views_attribute = (
        _ModelStructureDiscovery._model_discovery_parse_views_attribute
    )
    _resolve_name = _ModelStructureDiscovery._model_discovery_resolve_name
    _snake_case = _ModelStructureDiscovery._model_discovery_snake_case

    _column_name_literal = _ModelFieldDiscovery._model_discovery_column_name_literal
    _extract_composite_call = _ModelFieldDiscovery._model_discovery_extract_composite_call
    _extract_field_definition = (
        _ModelFieldDiscovery._model_discovery_extract_field_definition
    )
    _extract_field_definition_new_syntax = (
        _ModelFieldDiscovery._model_discovery_extract_field_definition_new_syntax
    )
    _extract_field_name_from_call = (
        _ModelFieldDiscovery._model_discovery_extract_field_name_from_call
    )
    _extract_referenced_table = (
        _ModelFieldDiscovery._model_discovery_extract_referenced_table
    )
    _extract_separate_foreign_key_definition = (
        _SeparateForeignKeyDiscovery._extract_separate_foreign_key_definition
    )
    _foreign_key_arg = staticmethod(_ModelFieldDiscovery._model_discovery_foreign_key_arg)
    _is_foreign_key_field = _ModelFieldDiscovery._model_discovery_is_foreign_key_field
    _is_separate_foreign_key_call = (
        _ModelFieldDiscovery._model_discovery_is_separate_foreign_key_call
    )
    _literal_argument = _ModelFieldDiscovery._model_discovery_literal_argument
    _parse_lambda_fields = _ModelFieldDiscovery._model_discovery_parse_lambda_fields
    _record_composite = staticmethod(
        _ModelFieldDiscovery._model_discovery_record_composite
    )
    _resolve_id_column_to_table = (
        _ModelFieldDiscovery._model_discovery_resolve_id_column_to_table
    )
    _resolve_self_constant = _ModelFieldDiscovery._model_discovery_resolve_self_constant
    _warn_unrecognised_field_call = (
        _ModelFieldDiscovery._model_discovery_warn_unrecognised_field_call
    )

    _extract_raw_sql_dependencies = (
        _ModelDependencyDiscovery._model_discovery_extract_raw_sql_dependencies
    )
    _extract_sql_from_function = (
        _ModelDependencyDiscovery._model_discovery_extract_sql_from_function
    )
    _log_cycle_break = _ModelDependencyDiscovery._model_discovery_log_cycle_break
    _parse_sql_for_dependencies = (
        _ModelDependencyDiscovery._model_discovery_parse_sql_for_dependencies
    )
    _topological_sort = _ModelDependencyDiscovery._model_discovery_topological_sort
