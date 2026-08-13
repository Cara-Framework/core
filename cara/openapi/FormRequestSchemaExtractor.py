"""Extract request contracts from :class:`FormRequest` rule dictionaries.

The extractor reads source with :mod:`ast`; it never imports an application or
boots its providers.  Literal Cara validation rules become JSON Schema and the
original rule is retained as an extension, so a construct that OpenAPI cannot
express is visible instead of silently invented.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_CURSOR_TOKEN_MAX = 4096
_MODIFIERS = frozenset({"bail", "confirmed", "distinct", "sometimes"})
_FORBIDDEN = frozenset({"missing", "prohibited"})


@dataclass(slots=True)
class _RuleNode:
    rule: str | None = None
    children: dict[str, _RuleNode] = field(default_factory=dict)


def _static_value(node: ast.AST, values: dict[str, Any]) -> Any:
    """Evaluate only inert literal expressions used by request constants."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return values.get(node.id)
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        resolved = [_static_value(item, values) for item in node.elts]
        if any(value is None for value in resolved):
            return None
        return resolved
    if isinstance(node, ast.Dict):
        resolved_dict: dict[Any, Any] = {}
        for key_node, value_node in zip(node.keys, node.values, strict=True):
            value = _static_value(value_node, values)
            if key_node is None:
                if not isinstance(value, dict):
                    return None
                resolved_dict.update(value)
                continue
            key = _static_value(key_node, values)
            if key is None or value is None:
                return None
            try:
                resolved_dict[key] = value
            except TypeError:
                return None
        return resolved_dict
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_value(node.left, values)
        right = _static_value(node.right, values)
        if left is not None and right is not None:
            try:
                return left + right
            except TypeError:
                return None
    return None


def _module_values(
    tree: ast.Module, imported_values: dict[str, Any] | None = None
) -> dict[str, Any]:
    values = dict(imported_values or {})
    for node in tree.body:
        target: ast.Name | None = None
        value: ast.AST | None = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0] if isinstance(node.targets[0], ast.Name) else None
            value = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target = node.target
            value = node.value
        if target is None or value is None:
            continue
        resolved = _static_value(value, values)
        if resolved is not None:
            values[target.id] = resolved
    return values


def _module_name(root: Path, path: Path) -> str:
    relative = path.relative_to(root).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _imported_module_name(
    root: Path,
    path: Path,
    node: ast.ImportFrom,
    known_modules: frozenset[str],
) -> str | None:
    if node.level:
        package = list(path.relative_to(root).parent.parts)
        ascend = node.level - 1
        if ascend > len(package):
            return None
        base = package[: len(package) - ascend]
        if node.module:
            base.extend(node.module.split("."))
        candidate = ".".join(base)
        return candidate if candidate in known_modules else None
    if not node.module:
        return None
    matches = [
        module
        for module in known_modules
        if node.module == module or node.module.endswith(f".{module}")
    ]
    return max(matches, key=len) if matches else None


def _resolve_module_values(
    root: Path, modules: dict[Path, ast.Module]
) -> dict[Path, dict[str, Any]]:
    """Resolve inert constants, including imports between request modules."""

    module_paths = {_module_name(root, path): path for path in modules}
    known_modules = frozenset(module_paths)
    resolved = {path: _module_values(tree) for path, tree in modules.items()}
    for _ in range(len(modules) + 1):
        changed = False
        for path, tree in modules.items():
            imported: dict[str, Any] = {}
            for node in tree.body:
                if not isinstance(node, ast.ImportFrom):
                    continue
                target_name = _imported_module_name(root, path, node, known_modules)
                if target_name is None:
                    continue
                target_values = resolved[module_paths[target_name]]
                for alias in node.names:
                    if alias.name in target_values:
                        imported[alias.asname or alias.name] = target_values[alias.name]
            current = _module_values(tree, imported)
            if current != resolved[path]:
                resolved[path] = current
                changed = True
        if not changed:
            break
    return resolved


def _rule_expression(node: ast.AST, values: dict[str, Any]) -> tuple[str, bool]:
    """Render one rule string, marking unresolved f-string fragments partial."""
    resolved = _static_value(node, values)
    if isinstance(resolved, str):
        return resolved, False
    if not isinstance(node, ast.JoinedStr):
        return ast.unparse(node), True

    chunks: list[str] = []
    partial = False
    for part in node.values:
        if isinstance(part, ast.Constant) and isinstance(part.value, str):
            chunks.append(part.value)
            continue
        if isinstance(part, ast.FormattedValue):
            value = _static_value(part.value, values)
            if value is not None:
                chunks.append(str(value))
            else:
                chunks.append("{" + ast.unparse(part.value) + "}")
                partial = True
    return "".join(chunks), partial


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
        if property_schema.get("type") == "object":
            parameter.update({"style": "deepObject", "explode": True})
        elif property_schema.get("type") == "array":
            parameter.update({"style": "form", "explode": False})
        parameters.append(parameter)
    return parameters
