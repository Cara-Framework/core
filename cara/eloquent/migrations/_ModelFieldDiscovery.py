"""Field-builder call and foreign-key discovery."""

from __future__ import annotations

import ast
import logging

from cara.eloquent.schema.Schema import FIELD_TYPE_PARAMETERS as _FIELD_TYPE_PARAMETERS
from cara.facades import Log

_logger = logging.getLogger("cara.migrations.discoverer")
_UNRESOLVED = object()


def _model_discovery_extract_field_definition(self, call_node: ast.Call) -> dict | None:
    """Extract Field.* definition from AST call node (old syntax)."""
    if (
        isinstance(call_node.func, ast.Attribute)
        and isinstance(call_node.func.value, ast.Name)
        and call_node.func.value.id == "Field"
    ):
        field_type = call_node.func.attr
        params = {}

        # Extract positional arguments
        for i, arg in enumerate(call_node.args):
            if isinstance(arg, ast.Constant):
                if field_type == "decimal":
                    if i == 0:
                        params["precision"] = arg.value
                    elif i == 1:
                        params["scale"] = arg.value
                elif field_type == "string" and i == 0:
                    params["length"] = arg.value

        # Extract keyword arguments
        for keyword in call_node.keywords:
            if isinstance(keyword.value, ast.Constant):
                params[keyword.arg] = keyword.value.value
            elif isinstance(keyword.value, ast.List):
                # Handle list values like options=["value1", "value2"]
                list_values = []
                for element in keyword.value.elts:
                    if isinstance(element, ast.Constant):
                        list_values.append(element.value)
                params[keyword.arg] = list_values

        return {"type": field_type, "params": params}

    return None

def _model_discovery_parse_lambda_fields(self, lambda_node: ast.Lambda, model_info: dict):
    """Parse lambda field: (...) body to extract field definitions."""
    if isinstance(lambda_node.body, ast.Tuple):
        # Handle tuple of field definitions
        for field_call in lambda_node.body.elts:
            if isinstance(field_call, ast.Call):
                # Composite ``field.unique([...])`` — list-of-cols
                # constraint, distinct from the chained
                # ``.unique()`` modifier on a single column.
                cu = self._extract_composite_call(field_call, "unique")
                if cu is not None:
                    self._record_composite(model_info["composite_uniques"], cu[0], cu[1])
                    continue
                # Composite ``field.index([...])`` or
                # ``field.index("col")`` — emit ``table.index(...)``.
                ci = self._extract_composite_call(field_call, "index")
                if ci is not None:
                    self._record_composite(model_info["composite_indexes"], ci[0], ci[1])
                    continue
                # Check if this is a separate foreign key definition
                if self._is_separate_foreign_key_call(field_call):
                    foreign_key_def = self._extract_separate_foreign_key_definition(
                        field_call
                    )
                    if foreign_key_def and foreign_key_def.get("composite"):
                        # Composite FK: no single ``fields`` entry to attach
                        # to (the local side is a list), so collect it as a
                        # top-level entry — the same pattern as
                        # composite_uniques / composite_indexes.
                        entry = {
                            "columns": foreign_key_def["field"],
                            "name": foreign_key_def.get("name"),
                            "references": foreign_key_def["references"],
                            "on": foreign_key_def["on"],
                            "on_delete": foreign_key_def.get("on_delete"),
                            **(
                                {
                                    "on_delete_columns": foreign_key_def[
                                        "on_delete_columns"
                                    ]
                                }
                                if foreign_key_def.get("on_delete_columns")
                                else {}
                            ),
                            "on_update": foreign_key_def.get("on_update"),
                        }
                        if entry not in model_info["composite_foreign_keys"]:
                            model_info["composite_foreign_keys"].append(entry)
                    elif foreign_key_def:
                        field_name = foreign_key_def["field"]
                        # Add foreign key info to existing field
                        if field_name in model_info["fields"]:
                            model_info["fields"][field_name]["foreign_key"] = (
                                foreign_key_def
                            )
                else:
                    # Always try to extract field definition first
                    field_def = self._extract_field_definition_new_syntax(field_call)
                    if field_def:
                        field_name = self._extract_field_name_from_call(field_call)
                        if field_name:
                            model_info["fields"][field_name] = field_def
                            # A chained single-column ``.index()`` modifier
                            # becomes a one-column ``composite_indexes`` entry
                            # so the (shared) emitter renders
                            # ``table.index(["<field_name>"])``. Without this
                            # the index was parsed but never emitted.
                            if field_def.get("params", {}).get("index"):
                                self._record_composite(
                                    model_info["composite_indexes"],
                                    [field_name],
                                    None,
                                )
                        else:
                            # Handle special fields without names (timestamps, soft_deletes)
                            field_type = field_def.get("type")
                            if field_type in ["timestamps", "soft_deletes"]:
                                model_info["fields"][field_type] = field_def


def _model_discovery_resolve_self_constant(self, node: ast.AST):
    """Resolve ``self.CONSTANT`` or ``ModelName.CONSTANT`` to a literal."""
    if (
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id in {"self", getattr(self, "_class_name", None)}
    ):
        return getattr(self, "_class_constants", {}).get(node.attr, _UNRESOLVED)
    return _UNRESOLVED


def _model_discovery_literal_argument(self, node: ast.AST):
    """Resolve a builder argument to its literal value, or ``_UNRESOLVED``.

    ``enum`` takes a list of options and every other builder takes
    scalars, so both shapes are handled here instead of at each call site.
    Anything else (a name, an expression) stays unresolved so the caller
    records no param rather than a garbage one.
    """
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, (ast.List, ast.Tuple)):
        return [
            element.value for element in node.elts if isinstance(element, ast.Constant)
        ]
    return _UNRESOLVED


def _model_discovery_extract_field_definition_new_syntax(
    self, call_node: ast.Call
) -> dict | None:
    """Extract field definition from new syntax: field.string("name").nullable()"""
    field_type = None
    params = {}
    foreign_key_info = {}

    # Traverse the call chain to extract field type and modifiers
    current = call_node
    while current:
        if isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                # This is a method call like .nullable() or .default(value)
                method_name = current.func.attr

                if method_name in (
                    self.FIELD_TYPES_WITH_NAMES | self.FIELD_TYPES_WITHOUT_NAMES
                ):
                    # This is the base field type. Which extra argument
                    # means what is read off ``FieldBuilder``'s own
                    # signature — the parser used to hand-code the
                    # positional indices per type (``decimal`` 1/2 =
                    # precision/scale, ``string``/``char`` 1 = length),
                    # which meant a builder that grew a parameter kept it
                    # invisible here.
                    field_type = method_name
                    names = _FIELD_TYPE_PARAMETERS.get(field_type, ())
                    offset = 1 if field_type in self.FIELD_TYPES_WITH_NAMES else 0
                    for i, arg in enumerate(current.args):
                        position = i - offset
                        if not 0 <= position < len(names):
                            continue
                        value = self._literal_argument(arg)
                        if value is not _UNRESOLVED:
                            params[names[position]] = value
                    # Keyword form was dropped ENTIRELY: only positional
                    # args were read, so a declared
                    # ``field.decimal("price", precision=12, scale=4)``
                    # reached the emitter with no params at all and was
                    # written as NUMERIC(10,2) — four digits of scale lost
                    # from a money column, in silence, with every check
                    # green. The legacy extractor has always read
                    # ``keywords``; this one never did.
                    for keyword in current.keywords:
                        if keyword.arg not in names:
                            continue
                        value = self._literal_argument(keyword.value)
                        if value is not _UNRESOLVED:
                            params[keyword.arg] = value

                elif method_name == "nullable":
                    params["nullable"] = True
                elif method_name == "unique":
                    params["unique"] = True
                elif method_name == "index":
                    # Chained single-column ``.index()`` modifier, e.g.
                    # ``field.string("name", 255).index()``. Distinct from
                    # the standalone ``field.index([...])`` composite call.
                    # Without this branch the chained index was silently
                    # dropped from the generated migration. Captured as a
                    # param here; the caller turns it into a single-column
                    # ``composite_indexes`` entry so the emitter renders
                    # ``table.index(["name"])``.
                    params["index"] = True
                elif method_name == "use_current":
                    params["use_current"] = True
                elif method_name == "backfill_from":
                    # Read by evolve-mode planning only; a generated
                    # migration never renders it, because a fresh install
                    # has no existing rows to fill.
                    if current.args and isinstance(current.args[0], ast.Constant):
                        params["backfill_from"] = current.args[0].value
                elif method_name == "default":
                    if current.args:
                        arg = current.args[0]
                        resolved = self._resolve_self_constant(arg)
                        if isinstance(arg, ast.Constant):
                            params["default"] = arg.value
                        elif resolved is not _UNRESOLVED:
                            # ``self.CONSTANT`` → its class-level literal, so
                            # the migration carries the VALUE (quoted like any
                            # literal) instead of an unresolvable ``self.X``.
                            params["default"] = resolved
                        else:
                            # Expression default (DB.raw("now()"), an enum
                            # member) — keep the source verbatim + flag it so
                            # the generator emits it UNQUOTED instead of
                            # silently dropping it.
                            params["default"] = ast.unparse(arg)
                            params["default_is_raw"] = True

                # Foreign key methods
                elif method_name == "foreign":
                    foreign_key_info["is_foreign"] = True
                elif method_name == "references":
                    if current.args and isinstance(current.args[0], ast.Constant):
                        foreign_key_info["references"] = current.args[0].value
                elif method_name == "on":
                    if current.args and isinstance(current.args[0], ast.Constant):
                        foreign_key_info["on"] = current.args[0].value
                elif method_name == "on_delete":
                    if current.args and isinstance(current.args[0], ast.Constant):
                        foreign_key_info["on_delete"] = current.args[0].value
                    if len(current.args) > 1:
                        foreign_key_info["on_delete_columns"] = self._foreign_key_arg(
                            current.args[1]
                        )
                    for keyword in current.keywords:
                        if keyword.arg == "columns":
                            foreign_key_info["on_delete_columns"] = (
                                self._foreign_key_arg(keyword.value)
                            )
                elif method_name == "on_update":
                    if current.args and isinstance(current.args[0], ast.Constant):
                        foreign_key_info["on_update"] = current.args[0].value

                # Move to the object being called (chaining)
                current = current.func.value
            else:
                break
        else:
            break

    if field_type:
        result = {"type": field_type, "params": params}

        # Add foreign key information if this is a foreign key
        if foreign_key_info.get("is_foreign"):
            # Get field name for foreign key config
            field_name = self._extract_field_name_from_call(call_node)
            if field_name:
                foreign_key_config = {
                    "field": field_name,
                    "references": foreign_key_info.get("references"),
                    "on": foreign_key_info.get("on"),
                    "on_delete": foreign_key_info.get("on_delete"),
                    **(
                        {
                            "on_delete_columns": foreign_key_info[
                                "on_delete_columns"
                            ]
                        }
                        if foreign_key_info.get("on_delete_columns")
                        else {}
                    ),
                    "on_update": foreign_key_info.get("on_update"),
                }
                result["foreign_key"] = foreign_key_config

        return result

    self._warn_unrecognised_field_call(call_node)
    return None


def _model_discovery_warn_unrecognised_field_call(self, call_node: ast.Call) -> None:
    """Announce a field call the parser could not type, instead of dropping it.

    Every column this parser fails to recognise disappears in total
    silence: it never enters ``model_info["fields"]``, so
    ``make:migration --overwrite`` writes the table without it,
    ``migrations:check`` compares the same blind view and stays green, and
    ``schema:check`` then reports the live column as undeclared drift —
    three guards agreeing that a correct model is wrong. The vocabulary is
    now derived from ``FieldBuilder`` so this should be unreachable for a
    legal declaration; if it fires, the model wrote something
    ``Schema.build`` cannot execute either, and ops should see it.

    WARNING rather than a raise: discovery also runs under the read-only
    ``migrations:check`` / ``schema:check`` paths, where refusing to start
    would replace one silent wrong answer with no answer at all.
    """
    base_method = None
    current = call_node
    while isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
        base_method = current.func.attr
        current = current.func.value

    source = ast.unparse(call_node)
    model = getattr(self, "_class_name", None) or "<unknown model>"
    try:
        Log.warning(
            "Dropping unrecognised field declaration in %s: %s "
            "(base method %r is not a Schema.build field builder)",
            model,
            source,
            base_method,
            category="cara.eloquent.migrations",
        )
    except Exception:
        _logger.warning(
            "Dropping unrecognised field declaration in %s: %s "
            "(base method %r is not a Schema.build field builder)",
            model,
            source,
            base_method,
        )


def _model_discovery_extract_field_name_from_call(
    self, call_node: ast.Call
) -> str | None:
    """Extract field name from the first string argument in the call chain."""
    # We need to find the base field type call (like field.string("name"))
    # and extract the field name from there, not from modifier calls like .default(True)

    current = call_node
    while current:
        if isinstance(current, ast.Call):
            # Check if this is a base field type call
            if (
                isinstance(current.func, ast.Attribute)
                and isinstance(current.func.value, ast.Name)
                and current.func.value.id == "field"
            ):
                field_method = current.func.attr
                if field_method in self.FIELD_TYPES_WITH_NAMES:
                    # This is the base field call, extract first string argument
                    if (
                        current.args
                        and isinstance(current.args[0], ast.Constant)
                        and isinstance(current.args[0].value, str)
                    ):
                        return current.args[0].value
                    # …or the model's own class constant:
                    # ``field.unsigned_big_integer(self.ORIGIN_CHANNEL_ID_COLUMN)``
                    # keeps ONE spelling of the column shared with the
                    # repositories that query it. Resolved exactly like a
                    # schema default already is (``_resolve_self_constant``).
                    # Without this the column is INVISIBLE to migration
                    # generation and to ``schema:check``: the table is
                    # generated without it, a fresh install comes up missing
                    # a load-bearing FK column, and the drift report accuses
                    # the database of holding a column "not declared in the
                    # model" — while the model plainly declares it.
                    if current.args:
                        resolved = self._resolve_self_constant(current.args[0])
                        if isinstance(resolved, str):
                            return resolved
                elif field_method in self.FIELD_TYPES_WITHOUT_NAMES:
                    # Special fields that don't take field names
                    return None

            # Move to chained call
            if isinstance(current.func, ast.Attribute):
                current = current.func.value
            else:
                break
        else:
            break
    return None


def _model_discovery_is_foreign_key_field(
    self,
    field_name: str,
    field_info: dict,
    all_table_names: list[str],
) -> bool:
    """Check if field is a foreign key.

    A ``*_id`` column is only a foreign key when its ``_id``-stripped
    target resolves to an ACTUAL known table (so ``public_id`` /
    ``external_id`` / ``correlation_id`` stay plain columns instead of
    inventing phantom FKs). The explicit ``params['foreign_key']`` flag
    is always honoured.
    """
    if field_info.get("params", {}).get("foreign_key", False):
        return True
    if not field_name.endswith("_id"):
        return False
    if field_info.get("type") not in self.IMPLICIT_FOREIGN_KEY_TYPES:
        return False
    return self._resolve_id_column_to_table(field_name, all_table_names) is not None


def _model_discovery_extract_referenced_table(
    self,
    field_name: str,
    field_info: dict,
    all_table_names: list[str],
) -> str | None:
    """Extract referenced table name from foreign key field."""
    # For ID-shaped fields ending with _id, resolve the prefix to a table.
    if field_name.endswith("_id") and (
        field_info.get("params", {}).get("foreign_key", False)
        or field_info.get("type") in self.IMPLICIT_FOREIGN_KEY_TYPES
    ):
        return self._resolve_id_column_to_table(field_name, all_table_names)

    # Check for explicit references parameter
    return field_info.get("params", {}).get("references")


def _model_discovery_resolve_id_column_to_table(
    self, field_name: str, all_table_names: list[str]
) -> str | None:
    """Resolve a ``*_id`` column to a known table name, or ``None``.

    Strips the ``_id`` suffix and matches against the known table set,
    tolerating the singular/plural alias (``user_id`` → ``users``,
    ``category_id`` → ``category``). Returns the ACTUAL table name as it
    appears in ``all_table_names`` so the dependency graph and emitted FK
    reference the real table. Returns ``None`` when no known table matches
    (e.g. ``public_id``, ``external_id``, ``merged_into_brand_id`` whose
    ``merged_into_brand`` prefix is not a table — its real FK comes from
    an explicit ``field.foreign(...)`` instead).
    """
    base = field_name[:-3]  # strip "_id"
    if not base:
        return None
    known = set(all_table_names)
    # Exact match (singular convention: brand_id -> brand).
    if base in known:
        return base
    # Singular/plural aliases for the few pluralized tables (users).
    for candidate in (base + "s", base + "es"):
        if candidate in known:
            return candidate
    if base.endswith("s") and base[:-1] in known:
        return base[:-1]
    return None


def _model_discovery_record_composite(
    entries: list, columns: list, name: str | None
) -> None:
    """Append a ``{"columns": [...], "name": ...}`` constraint declaration.

    Deduped on the column tuple — the same columns must never yield two
    objects. When the columns are already recorded, a declared ``name``
    still wins over a previously recorded ``None`` (the chained
    ``.index()`` modifier carries no name, the standalone
    ``field.index([...], name=...)`` call does).
    """
    if not columns:
        return
    for entry in entries:
        if entry["columns"] == columns:
            if name and not entry["name"]:
                entry["name"] = name
            return
    entries.append({"columns": columns, "name": name})


def _model_discovery_extract_composite_call(self, call_node: ast.Call, method_name: str):
    """Match top-level ``field.<method_name>(...)`` and pull columns + name.

    Returns:
        * ``None`` when the call is not ``field.<method_name>(...)``
          (so the caller knows to keep trying other handlers).
        * ``([], None)`` when it is the right call but no string columns
          were extractable (still consumed; no constraint added).
        * ``([col, col, ...], name_or_None)`` on success, where ``name`` is
          the ``name=`` keyword the model declared. The model owns the
          object name — dropping it here made the generated migration fall
          back to an auto-derived name, so a later hand-written migration
          had to create/rename the declared one and the table ended up with
          two indexes on the same columns.

    Accepts both ``field.unique(["a", "b"])`` (list arg) and
    ``field.index("a")`` (single string), since the legacy schema
    builder allowed both.
    """
    if not isinstance(call_node, ast.Call):
        return None
    if not isinstance(call_node.func, ast.Attribute):
        return None
    if call_node.func.attr != method_name:
        return None
    if not isinstance(call_node.func.value, ast.Name):
        return None
    if call_node.func.value.id != "field":
        return None
    name = next(
        (
            kw.value.value
            for kw in call_node.keywords
            if kw.arg == "name"
            and isinstance(kw.value, ast.Constant)
            and isinstance(kw.value.value, str)
        ),
        None,
    )
    if not call_node.args:
        return [], None
    first = call_node.args[0]
    if isinstance(first, ast.List):
        cols = []
        for elt in first.elts:
            col = self._column_name_literal(elt)
            if col is not None:
                cols.append(col)
        return cols, name
    col = self._column_name_literal(first)
    if col is not None:
        return [col], name
    return [], None


def _model_discovery_column_name_literal(self, node: ast.AST) -> str | None:
    """A column-name argument as a string: a literal, or ``self.CONSTANT``.

    Models may name a column through their own class constant so the column
    has ONE spelling shared with the repositories that query it
    (``field.index(self.ORIGIN_CHANNEL_ID_COLUMN)``). Resolving it here
    keeps such an index visible to migration generation and to
    ``schema:check`` — unresolved, the index is silently never generated and
    the drift report accuses the database of holding an index the model
    "does not declare", while the model plainly declares it.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    resolved = self._resolve_self_constant(node)
    return resolved if isinstance(resolved, str) else None


def _model_discovery_is_separate_foreign_key_call(self, call_node: ast.Call) -> bool:
    """Check if this is a separate foreign key call: field.foreign("field_name").references("id").on("table")"""
    # Traverse the call chain to look for 'foreign' method
    current = call_node
    while current:
        if isinstance(current, ast.Call):
            if isinstance(current.func, ast.Attribute):
                if (
                    isinstance(current.func.value, ast.Name)
                    and current.func.value.id == "field"
                    and current.func.attr == "foreign"
                ):
                    return True
                # Move to the object being called (chaining)
                current = current.func.value
            else:
                break
        else:
            break
    return False


def _model_discovery_foreign_key_arg(arg: ast.expr):
    """Resolve a ``foreign(...)`` / ``references(...)`` argument.

    Returns a ``str`` for the scalar form (``foreign("a")``), a list of
    ``str`` for the composite form (``foreign(["a", "b"])``), or ``None``
    when the argument is neither a string constant nor a list of string
    constants. Mirrors how ``_extract_composite_call`` reads list args so
    the composite FK declaration matches the composite unique/index form.
    """
    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
        return arg.value
    if isinstance(arg, ast.List):
        cols = [
            elt.value
            for elt in arg.elts
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
        ]
        return cols if cols else None
    return None
