"""Model dependency ordering and raw-SQL dependency discovery."""

from __future__ import annotations

import ast
import logging
import re

from cara.facades import Log

_logger = logging.getLogger("cara.migrations.discoverer")


def _model_discovery_topological_sort(
    self, models: list[dict], dependency_graph: dict[str, list[str]]
) -> list[dict]:
    """Topological sort using a simple iterative approach."""
    model_lookup = {model["table"]: model for model in models}
    result = []
    remaining = list(models)
    processed_tables = set()

    # Keep iterating until all models are processed
    while remaining:
        # Find models with all dependencies satisfied
        ready_models = []
        for model in remaining:
            table_name = model["table"]
            dependencies = dependency_graph.get(table_name, [])
            # Check if all dependencies are already processed
            if all(
                dep in processed_tables or dep == table_name or dep not in model_lookup
                for dep in dependencies
            ):
                ready_models.append(model)

        if not ready_models:
            # No model is ready → a circular FK dependency. Break it
            # deterministically on the lexicographically-lowest table
            # (was ``remaining[0]``, which depended on discovery order
            # and produced unstable migration sequences). Surface the
            # cycle instead of breaking it silently.
            cycle_model = min(remaining, key=lambda m: m["table"])
            ready_models = [cycle_model]
            self._log_cycle_break(cycle_model, remaining, dependency_graph)

        # Sort each ready level by table name so the FK-respecting
        # order is stable run-to-run (discovery order must not leak
        # into the emitted migration sequence numbers).
        ready_models.sort(key=lambda m: m["table"])

        # Add ready models to result and mark as processed
        for model in ready_models:
            result.append(model)
            processed_tables.add(model["table"])
            remaining.remove(model)

    return result


def _model_discovery_log_cycle_break(
    self,
    cycle_model: dict,
    remaining: list[dict],
    dependency_graph: dict[str, list[str]],
) -> None:
    """Warn that a circular FK dependency forced a non-FK-ordered break.

    Names the table chosen to break on, the unresolved tables still in
    the cycle, and the offending dependency edges, so a real FK cycle in
    the model set is visible rather than silently mis-ordered.
    """
    unresolved = sorted(m["table"] for m in remaining)
    edges = sorted(
        f"{m['table']} -> {dep}"
        for m in remaining
        for dep in dependency_graph.get(m["table"], [])
        if dep in unresolved and dep != m["table"]
    )
    try:
        Log.warning(
            "Circular FK dependency in model discovery; breaking on '%s'. "
            "Unresolved tables: %s. Cycle edges: %s",
            cycle_model["table"],
            ", ".join(unresolved),
            ", ".join(edges) or "(none detected)",
            category="cara.eloquent.migrations",
        )
    except Exception:
        _logger.warning(
            "Circular FK dependency in model discovery; breaking on '%s'. "
            "Unresolved tables: %s. Cycle edges: %s",
            cycle_model["table"],
            ", ".join(unresolved),
            ", ".join(edges) or "(none detected)",
        )


def _model_discovery_extract_raw_sql_dependencies(
    self, model_info: dict, all_known_tables: list[str] | None = None
):
    """Extract table dependencies from raw SQL in the up() function."""
    if all_known_tables is None:
        all_known_tables = []
    try:
        # Get the model file path
        model_file = model_info.get("file")
        if not model_file:
            return

        # Read the file content
        with open(model_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Parse the AST to find the up() function
        tree = ast.parse(content)

        # Find the up() function within the fields property
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "up":
                # Extract SQL from DB.statement() calls
                sql_content = self._extract_sql_from_function(node)
                if sql_content:
                    # Parse SQL for table references
                    dependencies = self._parse_sql_for_dependencies(
                        sql_content, model_info, all_known_tables
                    )
                    if dependencies:
                        # Add dependencies to fields for dependency resolution
                        for dep_table in dependencies:
                            field_name = f"_sql_dependency_{dep_table}"
                            model_info["fields"][field_name] = {
                                "type": "sql_dependency",
                                "foreign_key": {"on": dep_table, "references": "id"},
                            }

    except Exception:
        _logger.debug("dependency extraction skipped", exc_info=True)


def _model_discovery_extract_sql_from_function(
    self, function_node: ast.FunctionDef
) -> str:
    """Extract SQL content from DB.statement() calls in function."""
    sql_parts = []

    for node in ast.walk(function_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "statement"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "DB"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            sql_parts.append(node.args[0].value)

    return "\n".join(sql_parts)


def _model_discovery_parse_sql_for_dependencies(
    self, sql: str, model_info: dict, all_known_tables: list[str]
) -> list[str]:
    """Parse SQL content to find table dependencies using known tables from discovery."""
    dependencies = []

    # Extract all potential table references from SQL
    potential_tables = set()

    # Pattern 1: FROM table_name
    from_matches = re.finditer(r"FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.IGNORECASE)
    for match in from_matches:
        potential_tables.add(match.group(1).lower())

    # Pattern 2: JOIN table_name
    join_matches = re.finditer(
        r"(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)",
        sql,
        re.IGNORECASE,
    )
    for match in join_matches:
        potential_tables.add(match.group(1).lower())

    # Pattern 3: REFERENCES table_name (for foreign keys)
    ref_matches = re.finditer(
        r"REFERENCES\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.IGNORECASE
    )
    for match in ref_matches:
        potential_tables.add(match.group(1).lower())

    # Pattern 4: Extract foreign key column patterns (column_id -> column table)
    fk_column_matches = re.finditer(r"([a-zA-Z_][a-zA-Z0-9_]*)_id", sql, re.IGNORECASE)
    for match in fk_column_matches:
        base_name = match.group(1).lower()
        potential_tables.add(base_name)

    # Filter potential tables against known tables from model discovery
    current_table = model_info.get("table", "").lower()
    known_tables_lower = [t.lower() for t in all_known_tables]

    for table_name in potential_tables:
        if (
            table_name != current_table
            and table_name in known_tables_lower
            and table_name not in dependencies
        ):
            dependencies.append(table_name)

    return dependencies
