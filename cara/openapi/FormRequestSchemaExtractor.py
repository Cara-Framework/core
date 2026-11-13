"""Extract request contracts from :class:`FormRequest` rule dictionaries.

The extractor reads source with :mod:`ast`; it never boots an application's
providers or runs its functions. The one import it makes is the module a request
names a constant from (``from app.models import Product`` for
``in:{','.join(Product.CONDITIONS)}``), read for plain data only, so an ``in:``
rule built from a kernel vocabulary publishes its enum like a literal one.
Literal Cara validation rules become JSON Schema and the
original rule is retained as an extension, so a construct that OpenAPI cannot
express is visible instead of silently invented.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from cara.filtering import (
    LAST_DAYS_MAX,
    MAX_CONDITIONS,
    MAX_GROUP_CHILDREN,
    MAX_ROOT_NODES,
    RAW_LENGTH_CAP,
    VALUES_HARD_CAP,
)

from ._RequestConstants import (
    _resolve_module_values,
    _rule_expression,
    _static_value,
)

_CURSOR_TOKEN_MAX = 4096
_MODIFIERS = frozenset({"bail", "confirmed", "distinct", "sometimes"})
_FORBIDDEN = frozenset({"missing", "prohibited"})

#: The wire grammar of a ``filter_tree:<schema>`` parameter, said once for
#: every operation that takes one. The grammar is the framework's
#: (``cara.filtering.FilterTree``); the vocabulary — fields, operators, options
#: — is the app's schema, published beside the paths as ``x-filter-schemas``
#: and named on the parameter by ``x-filter-schema``. What goes in ``v`` (each
#: operator's arity, each kind's value form) and the tree's ceilings are the
#: parser's too, so they are said here from its own constants: a field's kind
#: and operators alone left a customer learning them from 422s (api-consumer
#: review, 2026-11-12). The ceilings are ``TreeSchema``'s defaults.
_FILTER_TREE_GRAMMAR = (
    "A filter expression as compact JSON (URL-encoded in the query string). "
    'A condition is {"f": "<field>", "o": "<operator>", "v": ["<value>", ...]} '
    "and every value is a string: `in` / `not_in` take one or more values, "
    "`between` takes two (low, high; both ends included), `empty` / `not_empty` "
    "take none, and every other operator takes one. A value follows the field's "
    "kind: select, one of its option values; entity, a public id starting with "
    'its `prefix`; boolean, "true" or "false"; number, a decimal such as '
    '"12.50"; text, the text itself; date, a calendar day YYYY-MM-DD in the '
    "time zone the API reports in (`before` is earlier than that day, `after` "
    "later than it), and for `last_days` a day count from 1 to "
    f"{LAST_DAYS_MAX} that includes today. The root is an array of conditions "
    'and groups joined with AND, or {"any": [...]} to join them with OR. A '
    'group is {"any": [conditions]} (OR) or {"all": [conditions]} (AND) and '
    f"holds conditions only. At most {MAX_ROOT_NODES} top-level nodes, "
    f"{MAX_GROUP_CHILDREN} conditions in a group, {MAX_CONDITIONS} conditions "
    f"in all, {VALUES_HARD_CAP} values in a condition and {RAW_LENGTH_CAP} "
    'characters. Example: [{"f": "status", "o": "in", "v": ["active"]}, '
    '{"any": [{"f": "tag", "o": "contains", "v": ["red"]}, {"f": "brand", '
    '"o": "empty"}]}]. The fields this operation accepts, each with its '
    "operators and options, are listed under the document's `x-filter-schemas` "
    "by the name in this parameter's `x-filter-schema`; an expression the "
    "schema refuses answers 422 naming the first problem."
)


@dataclass(slots=True)
class _RuleNode:
    rule: str | None = None
    children: dict[str, _RuleNode] = field(default_factory=dict)


def _number(value: str) -> int | float | None:
    try:
        number = float(value)
    except ValueError:
        return None
    return int(number) if number.is_integer() else number


def _bound(schema: dict[str, Any], kind: str, value: int | float) -> None:
    schema_type = schema.get("type")
    if schema_type == "string":
        schema["minLength" if kind == "min" else "maxLength"] = int(value)
    elif schema_type == "array":
        schema["minItems" if kind == "min" else "maxItems"] = int(value)
    elif schema_type in {"integer", "number"}:
        schema["minimum" if kind == "min" else "maximum"] = value


def _rule_schema(rule: str | None) -> dict[str, Any]:
    if not rule:
        return {}
    tokens = [token.strip() for token in rule.split("|") if token.strip()]
    names = {token.partition(":")[0] for token in tokens}
    schema: dict[str, Any] = {"x-cara-rule": rule}

    if "decimal_text" in names:
        schema["type"] = "string"
    elif "integer" in names:
        schema["type"] = "integer"
    elif "numeric" in names:
        schema["type"] = "number"
    elif "boolean" in names:
        schema["type"] = "boolean"
    elif "array" in names:
        schema.update({"type": "array", "items": {}})
    elif "dict" in names:
        schema.update({"type": "object", "additionalProperties": True})
    elif {"string", "email", "url", "date", "dateformat"} & names:
        schema["type"] = "string"

    if "nullable" in names:
        schema["nullable"] = True
    if "email" in names:
        schema["format"] = "email"
    if "url" in names:
        schema["format"] = "uri"
    if "date" in names:
        schema["format"] = "date"
    if "alpha" in names:
        schema["pattern"] = "^[A-Za-z]+$"
    if "required" in names and schema.get("type") == "string":
        schema["minLength"] = 1

    for token in tokens:
        name, separator, argument = token.partition(":")
        if not separator:
            continue
        if name == "in" and "{" not in argument:
            schema["enum"] = argument.split(",")
        elif name == "in_csv":
            schema["x-cara-in-csv"] = argument.split(",")
        elif name == "public_id_csv":
            schema["x-cara-public-id-csv"] = argument or True
        elif name in {"min", "max"} and (number := _number(argument)) is not None:
            _bound(schema, name, number)
        elif name == "between":
            low, _, high = argument.partition(",")
            if (low_number := _number(low)) is not None:
                _bound(schema, "min", low_number)
            if (high_number := _number(high)) is not None:
                _bound(schema, "max", high_number)
        elif name == "size" and (number := _number(argument)) is not None:
            _bound(schema, "min", number)
            _bound(schema, "max", number)
        elif name == "regex":
            schema["pattern"] = argument
        elif name == "starts_with":
            schema["pattern"] = (
                "^(?:" + "|".join(re.escape(value) for value in argument.split(",")) + ")"
            )
        elif name == "dateformat":
            schema["x-cara-date-format"] = argument
        elif name == "decimal_text":
            parts = argument.split(",")
            try:
                precision, scale = (int(part) for part in parts)
            except TypeError, ValueError:
                continue
            if precision < 1 or scale < 0 or scale > precision:
                continue
            integer_digits = precision - scale
            if integer_digits == 0:
                whole = "0"
            elif integer_digits == 1:
                whole = "(?:0|[1-9])"
            else:
                whole = f"(?:0|[1-9][0-9]{{0,{integer_digits - 1}}})"
            fraction = f"(?:\\.[0-9]{{1,{scale}}})?" if scale else ""
            schema.update(
                {
                    "pattern": f"^{whole}{fraction}$",
                    "maxLength": (
                        scale + 2
                        if integer_digits == 0
                        else precision + (1 if scale else 0)
                    ),
                    "x-cara-decimal-precision": precision,
                    "x-cara-decimal-scale": scale,
                }
            )
        elif name in {"gt", "lt"}:
            schema[f"x-cara-{name}"] = argument
        elif name == "filter_tree" and argument:
            # The parser refuses anything longer, whatever the rule says.
            schema["x-cara-filter-tree"] = argument
            schema["maxLength"] = min(
                schema.get("maxLength", RAW_LENGTH_CAP), RAW_LENGTH_CAP
            )

    unsupported = sorted(
        name
        for name in names
        if name
        not in {
            *_MODIFIERS,
            *_FORBIDDEN,
            "alpha",
            "array",
            "between",
            "boolean",
            "date",
            "dateformat",
            "decimal_text",
            "dict",
            "email",
            "filter_tree",
            "gt",
            "in",
            "in_csv",
            "integer",
            "lt",
            "max",
            "min",
            "nullable",
            "numeric",
            "present",
            "prohibited",
            "public_id_csv",
            "regex",
            "required",
            "size",
            "starts_with",
            "string",
            "url",
        }
    )
    if unsupported:
        schema["x-cara-unsupported-rules"] = unsupported
    return schema


def _is_required(rule: str | None) -> bool:
    tokens = set((rule or "").split("|"))
    return "sometimes" not in tokens and bool(tokens & {"present", "required"})


def _is_forbidden(rule: str | None) -> bool:
    names = {token.partition(":")[0] for token in (rule or "").split("|")}
    return bool(names & _FORBIDDEN)


def _node_schema(node: _RuleNode) -> dict[str, Any]:
    schema = _rule_schema(node.rule)
    wildcard = node.children.get("*")
    named = {name: child for name, child in node.children.items() if name != "*"}
    if named:
        schema["type"] = "object"
        schema["additionalProperties"] = (
            _node_schema(wildcard) if wildcard is not None else False
        )
        properties: dict[str, Any] = {}
        required: list[str] = []
        forbidden: list[str] = []
        for name, child in sorted(named.items()):
            if _is_forbidden(child.rule):
                forbidden.append(name)
                continue
            properties[name] = _node_schema(child)
            if _is_required(child.rule):
                required.append(name)
        schema["properties"] = properties
        if required:
            schema["required"] = required
        if forbidden:
            schema["x-cara-forbidden-fields"] = forbidden
    elif wildcard is not None:
        wildcard_schema = _node_schema(wildcard)
        if schema.get("type") == "object":
            schema["additionalProperties"] = wildcard_schema
        else:
            schema["type"] = "array"
            schema["items"] = wildcard_schema
    return schema


def _schema_rules(schema: dict[str, Any]) -> dict[str, str]:
    """Recover every nested rule from an inherited request schema."""

    rules: dict[str, str] = {}

    def visit(current: dict[str, Any], prefix: str) -> None:
        for forbidden in current.get("x-cara-forbidden-fields", []):
            name = f"{prefix}.{forbidden}" if prefix else forbidden
            rules[name] = "prohibited"
        additional = current.get("additionalProperties")
        if isinstance(additional, dict):
            wildcard_name = f"{prefix}.*" if prefix else "*"
            wildcard_rule = additional.get("x-cara-rule")
            if isinstance(wildcard_rule, str):
                rules[wildcard_name] = wildcard_rule
            visit(additional, wildcard_name)
        for name, child in current.get("properties", {}).items():
            field_name = f"{prefix}.{name}" if prefix else name
            raw = child.get("x-cara-rule")
            if isinstance(raw, str):
                rules[field_name] = raw
            if child.get("type") == "array" and isinstance(child.get("items"), dict):
                item_name = f"{field_name}.*"
                item = child["items"]
                item_rule = item.get("x-cara-rule")
                if isinstance(item_rule, str):
                    rules[item_name] = item_rule
                visit(item, item_name)
            else:
                visit(child, field_name)

    visit(schema, "")
    return rules


def _cursor_rules(call: ast.Call) -> dict[str, str] | None:
    if not isinstance(call.func, ast.Name) or call.func.id != "cursor_rules":
        return None
    minimum = 1
    maximum = 100
    for keyword in call.keywords:
        value = _static_value(keyword.value, {})
        if keyword.arg == "min_limit" and isinstance(value, int):
            minimum = value
        elif keyword.arg == "max_limit" and isinstance(value, int):
            maximum = value
    return {
        "limit": f"nullable|integer|between:{minimum},{maximum}",
        "cursor": f"bail|sometimes|required|string|max:{_CURSOR_TOKEN_MAX}",
        "page": "missing",
        "per_page": "missing",
        "offset": "missing",
    }


class FormRequestSchemaExtractor:
    """Build ``RequestClass -> JSON Schema`` without importing the app."""

    def __init__(self, requests_dir: Path):
        self._dir = requests_dir

    def extract(self) -> dict[str, dict[str, Any]]:
        modules: dict[Path, ast.Module] = {}
        for path in sorted(self._dir.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            modules[path] = tree

        module_values = _resolve_module_values(self._dir, modules)
        classes: dict[str, tuple[ast.ClassDef, dict[str, Any]]] = {}
        for path, tree in modules.items():
            values = module_values[path]
            for node in tree.body:
                if isinstance(node, ast.ClassDef) and node.name.endswith("Request"):
                    classes[node.name] = (node, values)

        schemas: dict[str, dict[str, Any]] = {}
        resolving: set[str] = set()

        def resolve(name: str) -> dict[str, Any]:
            if name in schemas:
                return schemas[name]
            if name in resolving:
                raise RuntimeError(f"Cyclic FormRequest inheritance: {name}")
            resolving.add(name)
            cls, values = classes[name]
            rules: dict[str, str] = {}
            partial = False
            additional_schema: dict[str, Any] | bool = True

            for base in cls.bases:
                if isinstance(base, ast.Name) and base.id in classes:
                    inherited = resolve(base.id)
                    rules.update(_schema_rules(inherited))
                    partial = partial or bool(inherited.get("x-cara-rules-partial"))

            rules_method = next(
                (
                    node
                    for node in cls.body
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name == "rules"
                ),
                None,
            )
            if rules_method is not None:
                extracted, method_partial, dynamic_additional = self._rules(
                    rules_method, values
                )
                rules.update(extracted)
                partial = partial or method_partial
                if dynamic_additional is not None:
                    additional_schema = dynamic_additional

            root = _RuleNode()
            for field_name, rule in rules.items():
                node = root
                for part in field_name.split("."):
                    node = node.children.setdefault(part, _RuleNode())
                node.rule = rule
            schema = _node_schema(root)
            schema.setdefault("type", "object")
            schema.setdefault("properties", {})
            schema["additionalProperties"] = additional_schema
            doc = (ast.get_docstring(cls) or "").lower()
            if "query" in doc and "body" not in doc:
                schema["x-cara-location"] = "query"
            elif "body" in doc and "query" not in doc:
                schema["x-cara-location"] = "body"
            if "form/json" in doc or "form or json" in doc:
                schema["x-cara-content-types"] = [
                    "application/json",
                    "application/x-www-form-urlencoded",
                ]
            if partial:
                schema["x-cara-rules-partial"] = True
            schemas[name] = schema
            resolving.remove(name)
            return schema

        for name in sorted(classes):
            resolve(name)
        return dict(sorted(schemas.items()))

    @staticmethod
    def _rules(
        method: ast.FunctionDef | ast.AsyncFunctionDef, values: dict[str, Any]
    ) -> tuple[dict[str, str], bool, dict[str, Any] | None]:
        rules: dict[str, str] = {}
        partial = False
        dynamic_additional: dict[str, Any] | None = None
        local_values = dict(values)
        for statement in method.body:
            if isinstance(statement, ast.Assign) and len(statement.targets) == 1:
                target = statement.targets[0]
                if isinstance(target, ast.Name):
                    value = _static_value(statement.value, local_values)
                    if value is not None:
                        local_values[target.id] = value
                    if isinstance(statement.value, ast.DictComp):
                        rule, _ = _rule_expression(statement.value.value, local_values)
                        dynamic_additional = _rule_schema(rule)
                        partial = True

        returns = [node for node in ast.walk(method) if isinstance(node, ast.Return)]
        for returned in returns:
            value = returned.value
            if isinstance(value, ast.Call) and (cursor := _cursor_rules(value)):
                rules.update(cursor)
                continue
            if not isinstance(value, ast.Dict):
                partial = True
                continue
            for key_node, rule_node in zip(value.keys, value.values, strict=True):
                if key_node is None:
                    if isinstance(rule_node, ast.Call) and (
                        cursor := _cursor_rules(rule_node)
                    ):
                        rules.update(cursor)
                    elif isinstance(
                        spread := _static_value(rule_node, local_values), dict
                    ):
                        for key, spread_rule in spread.items():
                            if not isinstance(key, str) or not isinstance(
                                spread_rule, str
                            ):
                                partial = True
                                continue
                            rules[key] = spread_rule
                    else:
                        partial = True
                    continue
                key = _static_value(key_node, local_values)
                if not isinstance(key, str):
                    partial = True
                    continue
                rule, unresolved = _rule_expression(rule_node, local_values)
                rules[key] = rule
                partial = partial or unresolved
        return rules, partial, dynamic_additional


def request_query_parameters(schema: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert a FormRequest component into top-level query parameters."""
    required = set(schema.get("required", []))
    parameters: list[dict[str, Any]] = []
    for name, property_schema in schema.get("properties", {}).items():
        query_schema = {
            key: value
            for key, value in property_schema.items()
            if not key.startswith("x-cara-") and key != "nullable"
        }
        parameter: dict[str, Any] = {
            "name": name,
            "in": "query",
            "required": name in required,
            "schema": query_schema,
        }
        if tree := property_schema.get("x-cara-filter-tree"):
            parameter["description"] = _FILTER_TREE_GRAMMAR
            parameter["x-filter-schema"] = tree
        if property_schema.get("type") == "object":
            parameter.update({"style": "deepObject", "explode": True})
        elif property_schema.get("type") == "array":
            parameter.update({"style": "form", "explode": False})
        parameters.append(parameter)
    return parameters
