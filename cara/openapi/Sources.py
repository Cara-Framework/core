"""Read module structure from source: imports, lazy exports, package roots.

The parsing half of ``SourceIndex``: what a module binds at its top level,
how a relative import or a generated barrel's ``_LAZY_EXPORTS`` entry names
an absolute module, and the C3 order a class's bases resolve in. Nothing here
imports the code it reads.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

_FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)


def _import_root(directory: Path) -> Path:
    """The directory a package tree is imported from: the first ancestor of
    ``directory`` that is not itself a package (Python's own rule)."""
    root = directory
    while (root / "__init__.py").is_file():
        root = root.parent
    return root


def _module_name(root: Path, path: Path) -> str:
    """The dotted name ``path`` is imported as from ``root``."""
    parts = list(path.relative_to(root).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _first_parameter(function: Any) -> str | None:
    """The receiver name of a method (``self`` / ``cls``); ``None`` for a
    static method or a function with no positional parameter."""
    if any(_simple_name(d) == "staticmethod" for d in function.decorator_list):
        return None
    positional = [*function.args.posonlyargs, *function.args.args]
    return positional[0].arg if positional else None


def _parameter_annotation(function: Any, name: str) -> ast.AST | None:
    arguments = function.args
    for parameter in (
        *arguments.posonlyargs,
        *arguments.args,
        *arguments.kwonlyargs,
    ):
        if parameter.arg == name:
            return parameter.annotation
    return None


def _simple_name(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _subscript_base(node: ast.AST) -> ast.AST:
    """``Generic[T]`` / ``Base[Row]`` as a base class is its subscripted class."""
    return node.value if isinstance(node, ast.Subscript) else node


def _top_level(statements: list[ast.stmt]):
    """Module-level statements, through ``if`` / ``try`` blocks (an import
    under ``if TYPE_CHECKING`` still binds the name an annotation uses)."""
    for statement in statements:
        yield statement
        if isinstance(statement, ast.If):
            yield from _top_level(statement.body)
            yield from _top_level(statement.orelse)
        elif isinstance(statement, (ast.Try, ast.TryStar)):
            for block in (statement.body, statement.orelse, statement.finalbody):
                yield from _top_level(block)
            for handler in statement.handlers:
                yield from _top_level(handler.body)


def _absolute(package: str, module: str | None, level: int) -> str:
    """The absolute module a ``from <level dots><module> import`` names."""
    if level == 0:
        return module or ""
    parts = package.split(".") if package else []
    base = parts[: len(parts) - (level - 1)] if level > 1 else parts
    return ".".join([*base, *([module] if module else [])])


def _imports(tree: ast.Module, package: str) -> dict[str, tuple[str, str | None]]:
    """Every import binding; a module-level one wins over a function-local
    import of the same name."""
    bindings: dict[str, tuple[str, str | None]] = {}
    local: dict[str, tuple[str, str | None]] = {}
    top = set(map(id, _top_level(tree.body)))
    for node in ast.walk(tree):
        target = bindings if id(node) in top else local
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    target[alias.asname] = (alias.name, None)
                else:
                    head = alias.name.split(".")[0]
                    target[head] = (head, None)
        elif isinstance(node, ast.ImportFrom):
            source = _absolute(package, node.module, node.level)
            for alias in node.names:
                if alias.name != "*":
                    target[alias.asname or alias.name] = (source, alias.name)
    for name, binding in local.items():
        bindings.setdefault(name, binding)
    return bindings


def _lazy_exports(tree: ast.Module, package: str) -> dict[str, tuple[str, str]]:
    """A generated barrel's ``_LAZY_EXPORTS = {name: (".module", "attr")}``."""
    exports: dict[str, tuple[str, str]] = {}
    for statement in tree.body:
        target = (
            statement.targets[0]
            if isinstance(statement, ast.Assign) and len(statement.targets) == 1
            else statement.target
            if isinstance(statement, ast.AnnAssign)
            else None
        )
        value = getattr(statement, "value", None)
        if not (
            isinstance(target, ast.Name)
            and target.id == "_LAZY_EXPORTS"
            and isinstance(value, ast.Dict)
        ):
            continue
        for key, entry in zip(value.keys, value.values, strict=False):
            if (
                isinstance(key, ast.Constant)
                and isinstance(entry, ast.Tuple)
                and len(entry.elts) == 2
                and all(isinstance(e, ast.Constant) for e in entry.elts)
            ):
                relative, attribute = (e.value for e in entry.elts)
                level = len(relative) - len(relative.lstrip("."))
                source = _absolute(package, relative.lstrip(".") or None, level)
                exports[key.value] = (source, attribute)
    return exports


def _c3(sequences: list[list[Any]]) -> list[Any]:
    """C3 linearization; an inconsistent hierarchy falls back to
    left-to-right first occurrence (Python would refuse to define it)."""
    pending = [list(sequence) for sequence in sequences if sequence]
    merged: list[Any] = []
    while pending:
        for sequence in pending:
            head = sequence[0]
            if not any(head in other[1:] for other in pending):
                break
        else:
            flat = [klass for sequence in pending for klass in sequence]
            return merged + [k for i, k in enumerate(flat) if k not in flat[:i]]
        merged.append(head)
        pending = [[k for k in sequence if k != head] for sequence in pending]
        pending = [sequence for sequence in pending if sequence]
    return merged
