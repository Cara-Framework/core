"""Read the generated route table without importing it.

``routes:generate`` writes a thin aggregator (``routes/<kind>.py``) that imports
bounded shards under ``routes/generated/<kind>/group_NNN.py``. Both the spec
generator and route-level guards need those definitions as SOURCE — importing
them would boot providers — so the aggregator's own import list is the source
of truth for which shards are live. A shard that exists on disk but is no
longer imported must not contribute routes to anything.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

_HTTP_VERBS = ("get", "post", "put", "patch", "delete")


def route_shard_paths(deployable_root: Path, kind: str = "api") -> tuple[Path, ...]:
    """Return the exact generated shard files imported by ``routes/<kind>.py``."""
    root = deployable_root.resolve()
    aggregator = root / "routes" / f"{kind}.py"
    tree = ast.parse(aggregator.read_text(encoding="utf-8"), filename=str(aggregator))
    prefix = f"generated.{kind}.group_"
    paths: list[Path] = []
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom) or node.level != 1 or not node.module:
            continue
        if not node.module.startswith(prefix):
            continue
        path = aggregator.parent.joinpath(*node.module.split(".")).with_suffix(".py")
        if not path.is_file():
            raise FileNotFoundError(f"Generated route shard is missing: {path}")
        paths.append(path)
    if not paths:
        raise RuntimeError(
            f"routes/{kind}.py does not reference any generated route shards"
        )
    if len(paths) != len(set(paths)):
        raise RuntimeError(
            f"routes/{kind}.py references a generated route shard more than once"
        )
    return tuple(paths)


def route_shard_source(deployable_root: Path, kind: str = "api") -> str:
    """Concatenate only executable route definitions, in aggregator order."""
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in route_shard_paths(deployable_root, kind)
    )


def _literal_str(node: ast.AST) -> str | None:
    return (
        node.value
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
        else None
    )


def _group_prefix(call: ast.Call) -> str:
    """For a ``Route.prefix("/x")...`` chain, pull the prefix argument."""
    cur: ast.AST | None = call
    while isinstance(cur, ast.Call):
        func = cur.func
        if isinstance(func, ast.Attribute) and func.attr == "prefix" and cur.args:
            return _literal_str(cur.args[0]) or ""
        cur = func.value if isinstance(func, ast.Attribute) else None
        if cur is None:
            break
    return ""


def _literal_strings(node: ast.AST) -> list[str]:
    """Return a literal middleware declaration without evaluating source."""
    if value := _literal_str(node):
        return [value]
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        return [value for item in node.elts if (value := _literal_str(item))]
    return []


def _group_middleware(call: ast.Call) -> list[str]:
    """Collect every ``.middleware(...)`` in one fluent group chain."""
    middleware: list[str] = []
    cur: ast.AST | None = call
    while isinstance(cur, ast.Call):
        func = cur.func
        if isinstance(func, ast.Attribute) and func.attr == "middleware":
            for argument in cur.args:
                middleware.extend(_literal_strings(argument))
        cur = func.value if isinstance(func, ast.Attribute) else None
        if cur is None:
            break
    return middleware


def _route_middleware(call: ast.Call) -> list[str]:
    """Read middleware declared directly on a route leaf."""
    middleware: list[str] = []
    for keyword in call.keywords:
        if keyword.arg == "middleware":
            middleware.extend(_literal_strings(keyword.value))
    return middleware


def _verb(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Attribute) and func.attr in _HTTP_VERBS:
        return func.attr
    return None


def parse_routes(
    shard_paths: tuple[Path, ...] | list[Path], base_prefix: str = ""
) -> list[dict[str, Any]]:
    """Flatten the generated route tree into ``{method, path, controller,
    action, name}`` entries.

    Prefixes accumulate down the nested ``Route.prefix(...).routes(...)`` tree
    so the emitted path is the full URL a client actually calls.
    """
    routes: list[dict[str, Any]] = []

    def emit(
        call: ast.Call, prefix: str, verb: str, inherited_middleware: list[str]
    ) -> None:
        if len(call.args) < 2:
            return
        path = _literal_str(call.args[0]) or ""
        target = _literal_str(call.args[1]) or ""
        name = ""
        for keyword in call.keywords:
            if keyword.arg == "name":
                name = _literal_str(keyword.value) or ""
        controller, _, action = target.partition("@")
        routes.append(
            {
                "method": verb.upper(),
                "path": prefix + path,
                "controller": controller,
                "action": action,
                "name": name,
                "middleware": list(
                    dict.fromkeys(inherited_middleware + _route_middleware(call))
                ),
            }
        )

    def walk_group(call: ast.Call, prefix: str, inherited_middleware: list[str]) -> None:
        """Walk a ``Route.prefix(...).middleware(...).routes(<children>)`` call."""
        this_prefix = prefix + _group_prefix(call)
        this_middleware = list(
            dict.fromkeys(inherited_middleware + _group_middleware(call))
        )
        cur: ast.AST | None = call
        while isinstance(cur, ast.Call):
            func = cur.func
            if isinstance(func, ast.Attribute) and func.attr == "routes":
                for child in cur.args:
                    if not isinstance(child, ast.Call):
                        continue
                    verb = _verb(child)
                    if verb:
                        emit(child, this_prefix, verb, this_middleware)
                    else:
                        walk_group(child, this_prefix, this_middleware)
                return
            cur = func.value if isinstance(func, ast.Attribute) else None
            if cur is None:
                return

    for shard in shard_paths:
        tree = ast.parse(shard.read_text(encoding="utf-8"), filename=str(shard))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Return) or not isinstance(node.value, ast.Tuple):
                continue
            for group in node.value.elts:
                if isinstance(group, ast.Call):
                    walk_group(group, base_prefix, [])
    return routes


def openapi_path(path: str) -> str:
    """Routing uses ``@param`` holes; OpenAPI uses ``{param}``."""
    parts = []
    for segment in path.split("/"):
        if segment.startswith("@"):
            parts.append("{" + segment[1:].split(":")[0] + "}")  # strip constraint
        elif segment.startswith("{") and ":" in segment:
            parts.append("{" + segment[1:].split(":")[0] + "}")
        else:
            parts.append(segment)
    return "/".join(parts)


def path_params(path: str) -> list[dict[str, Any]]:
    """Declare every ``{param}`` hole of an OpenAPI path as a path parameter."""
    out = []
    for segment in path.split("/"):
        if segment.startswith("{") and segment.endswith("}"):
            out.append(
                {
                    "name": segment[1:-1],
                    "in": "path",
                    "required": True,
                    "schema": {"type": "string"},
                }
            )
    return out
