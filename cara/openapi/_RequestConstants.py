"""Resolve the constants a FormRequest builds its rules from, without running it.

``FormRequestSchemaExtractor`` reads request source with :mod:`ast`. A rule is
often built, not written: ``f"in:{','.join(LANE_KEYS)}"`` with ``LANE_KEYS``
imported from a kernel module, or ``Product.CONDITIONS`` off a model. This
module evaluates exactly those inert shapes — literals, names, constant
attributes of a class, ``"<sep>".join``/``sorted``/``list``/``tuple`` over a
resolved constant, ``+`` — and nothing else: an app function is never called
to document a rule, and a value that is not plain data resolves to ``None``
(the rule stays partial, which the published schema says).
"""

from __future__ import annotations

import ast
import importlib
import inspect
from decimal import Decimal
from pathlib import Path
from typing import Any


def _inert(value: Any) -> bool:
    """A plain data constant: scalars, and containers holding only those."""
    if isinstance(value, (str, int, float, bool, Decimal)):
        return True
    if isinstance(value, (list, tuple, set, frozenset)):
        return all(_inert(item) for item in value)
    if isinstance(value, dict):
        return all(_inert(key) and _inert(item) for key, item in value.items())
    return False


def _sequence(value: Any) -> list[Any] | None:
    """``value`` iterated as the runtime would, in a deterministic order: an
    unordered set renders sorted, so the published enum never reshuffles."""
    if isinstance(value, (set, frozenset)):
        try:
            return sorted(value)
        except TypeError:
            return None
    if isinstance(value, (list, tuple, dict)):
        return list(value)
    return None


def _static_call(node: ast.Call, values: dict[str, Any]) -> Any:
    """``"<sep>".join(x)``, ``sorted(x)``, ``list(x)`` and ``tuple(x)`` over a
    resolved constant — the shapes ``in:`` rules are built with."""
    if node.keywords or len(node.args) != 1:
        return None
    items = _sequence(_static_value(node.args[0], values))
    if items is None:
        return None
    func = node.func
    if (
        isinstance(func, ast.Attribute)
        and func.attr == "join"
        and isinstance(func.value, ast.Constant)
        and isinstance(func.value.value, str)
    ):
        if all(isinstance(item, str) for item in items):
            return func.value.value.join(items)
        return None
    if isinstance(func, ast.Name) and func.id not in values:
        if func.id == "sorted":
            try:
                return sorted(items)
            except TypeError:
                return None
        if func.id in {"list", "tuple"}:
            return items
    return None


def _static_value(node: ast.AST, values: dict[str, Any]) -> Any:
    """Evaluate only inert literal expressions used by request constants."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        return values.get(node.id)
    if isinstance(node, ast.Attribute):
        base = _static_value(node.value, values)
        if not isinstance(base, type):
            return None
        try:
            # Static lookup: a model metaclass ``__getattr__`` must not run.
            attribute = inspect.getattr_static(base, node.attr)
        except AttributeError:
            return None
        return attribute if _inert(attribute) else None
    if isinstance(node, ast.Call):
        return _static_call(node, values)
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


def _external_values(
    tree: ast.Module, path: Path, root: Path, known: frozenset[str]
) -> dict[str, Any]:
    """Constants and classes a request module imports from OUTSIDE the
    requests tree (``from app.models import Product``,
    ``from app.shared.runtime.OpenIssueLanes import LANE_KEYS``).

    The module is imported to read them — the boot-free kernel barrels a
    generator already imports — and only plain data constants and classes are
    kept (a class only as the base of a ``Product.CONDITIONS`` read). A module
    that does not import here leaves its names unresolved: the rule stays
    partial, as before.
    """
    found: dict[str, Any] = {}
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.level or not node.module:
            continue
        if _imported_module_name(root, path, node, known) is not None:
            continue
        try:
            module = importlib.import_module(node.module)
        except Exception:  # noqa: BLE001 — an unimportable module documents nothing
            continue
        for alias in node.names:
            try:
                value = inspect.getattr_static(module, alias.name)
            except AttributeError:
                # A barrel's lazy export (module ``__getattr__``) imports
                # the module that owns the name; nothing else runs.
                try:
                    value = getattr(module, alias.name)
                except Exception:  # noqa: BLE001 — unresolvable stays partial
                    continue
            if isinstance(value, type) or _inert(value):
                found[alias.asname or alias.name] = value
    return found


def _resolve_module_values(
    root: Path, modules: dict[Path, ast.Module]
) -> dict[Path, dict[str, Any]]:
    """Resolve inert constants, including imports between request modules and
    constants imported from outside the requests tree."""

    module_paths = {_module_name(root, path): path for path in modules}
    known_modules = frozenset(module_paths)
    external = {
        path: _external_values(tree, path, root, known_modules)
        for path, tree in modules.items()
    }
    resolved = {
        path: _module_values(tree, external[path]) for path, tree in modules.items()
    }
    for _ in range(len(modules) + 1):
        changed = False
        for path, tree in modules.items():
            imported: dict[str, Any] = dict(external[path])
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
